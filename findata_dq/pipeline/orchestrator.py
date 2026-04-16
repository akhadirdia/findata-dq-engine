"""
Pipeline orchestrateur — Étape 8.

Flow Buzzelli
-------------
  Raw  →  Staged  →  Mastered
  CSV       DQ checks      Certifié (aucun IV-H)

Responsabilités
---------------
1. Charger les enregistrements (CSV ou list[dict])
2. Appliquer les 12 dimensions à chaque enregistrement
3. Appliquer la dimension Collection au niveau batch
4. Lancer le détecteur ML (optionnel)
5. Lancer la remédiation LLM sur les IV (optionnel)
6. Construire la Scorecard avec tous les agrégats
7. Séparer les enregistrements Mastered-éligibles

Utilisation rapide
------------------
    from findata_dq.pipeline.orchestrator import DQOrchestrator, OrchestratorConfig

    config = OrchestratorConfig(pipeline_env="staging", ml_enabled=True)
    orch = DQOrchestrator(config)
    scorecard = orch.run_from_csv("tests/fixtures/policies_invalid.csv", "policies")
    print(f"Score global : {scorecard.global_dq_score:.1f}/100")
    print(f"Mastered éligibles : {scorecard.nb_records_mastered_eligible}/{scorecard.total_records}")
"""

from __future__ import annotations

import csv
import time
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from findata_dq.dimensions import (
    Accuracy, BusinessRules, Cohesion, Collection,
    Completeness, Congruence, Conformity, Fairness,
    ModelDrift, Privacy, Precision, Timeliness,
)
from findata_dq.models.dq_result import DQResult, DQStatus, ImpactLevel, RecordSummary
from findata_dq.models.scorecard import DimensionSummary, FinancialImpact, Scorecard


# ── Configuration ─────────────────────────────────────────────────────────────

@dataclass
class OrchestratorConfig:
    """
    Configuration complète du pipeline.
    Toutes les options sont optionnelles avec des valeurs par défaut sensées.
    """

    pipeline_env: str = "development"

    # ── ML (Isolation Forest) ────────────────────────────────────────────────
    ml_enabled: bool = True
    ml_contamination: float = 0.05
    ml_n_estimators: int = 200

    # ── LLM Remédiation ──────────────────────────────────────────────────────
    llm_enabled: bool = False        # désactivé par défaut (coût API)
    llm_max_calls: int = 5
    llm_model: str = "claude-haiku-4-5-20251001"
    anthropic_api_key: str | None = None

    # ── BusinessRules ────────────────────────────────────────────────────────
    reference_dt: date | None = None          # None → date.today()

    # ── Privacy ──────────────────────────────────────────────────────────────
    pii_fields: list[str] = field(default_factory=lambda: [
        "cause_sinistre", "description", "commentaire", "notes"
    ])
    retention_days: int = 3650                # ~10 ans
    refused_client_ids: set[str] = field(default_factory=set)
    check_pseudonymization: bool = False

    # ── Congruence (Z-score) ─────────────────────────────────────────────────
    # Fourni au moment du run si stats disponibles
    congruence_stats: dict[str, dict] | None = None

    # ── Collection (batch) ───────────────────────────────────────────────────
    expected_record_count: int | None = None
    expected_total: float | None = None
    sum_field: str | None = None

    # ── Cohesion (FK) ────────────────────────────────────────────────────────
    fk_checks: list[tuple] | None = None     # ex: [("id_client", valid_client_ids)]

    # ── Conformity ───────────────────────────────────────────────────────────
    # Patterns custom — None → utilise les patterns par défaut
    custom_patterns: dict[str, Any] | None = None


# ── Dimensions instanciées une seule fois (singleton léger) ──────────────────

_DIMENSIONS = [
    Completeness(),
    Timeliness(),
    Accuracy(),
    Precision(),
    Conformity(),
    Congruence(),
    BusinessRules(),
    Privacy(),
    Fairness(),
    ModelDrift(),
]
# Collection est batch-level — instanciée séparément
_COLLECTION_DIM = Collection()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_csv(path: Path | str) -> list[dict]:
    """Charge un CSV en liste de dicts. Toutes les valeurs restent des strings."""
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _infer_dataset(path: Path | str) -> str:
    """Déduit le nom du dataset depuis le nom de fichier si non fourni."""
    stem = Path(path).stem.lower()
    if "polic" in stem:
        return "policies"
    if "claim" in stem or "sinistr" in stem:
        return "claims"
    if "log" in stem:
        return "logs"
    if "model" in stem:
        return "model_metadata"
    return "unknown"



# Champ ID canonique par dataset
_ID_FIELD: dict[str, str] = {
    "policies": "num_police",
    "claims": "id_sinistre",
    "logs": "log_id",
    "model_metadata": "model_id",
}


def _build_record_config(cfg: OrchestratorConfig) -> dict:
    """Construit le dict config passé à chaque dimension par enregistrement."""
    # Timeliness attend un datetime avec timezone, pas une date
    ref_date = cfg.reference_dt or date.today()
    ref_dt = datetime.combine(ref_date, dtime.min).replace(tzinfo=timezone.utc)

    config: dict[str, Any] = {
        "pipeline_env": cfg.pipeline_env,
        "reference_dt": ref_dt,
        "pii_fields": cfg.pii_fields,
        "retention_days": cfg.retention_days,
        "refused_client_ids": cfg.refused_client_ids,
        "check_pseudonymization": cfg.check_pseudonymization,
    }
    if cfg.congruence_stats:
        config["stats"] = cfg.congruence_stats
    if cfg.fk_checks:
        config["fk_checks"] = cfg.fk_checks
    if cfg.custom_patterns:
        config["patterns"] = cfg.custom_patterns
    return config


def _build_batch_config(cfg: OrchestratorConfig, records: list[dict]) -> dict:
    """Config pour la dimension Collection (niveau batch)."""
    return {
        "expected_count": cfg.expected_record_count or len(records),
        "actual_count": len(records),
        "expected_total": cfg.expected_total,
        "sum_field": cfg.sum_field,
        "records": records,
    }


# ── Agrégation ────────────────────────────────────────────────────────────────

def _aggregate(
    records: list[dict],
    all_results: list[DQResult],
    dataset: str,
    start_time: float,
    cfg: OrchestratorConfig,
    ml_anomaly_ids: list[str],
    nb_llm_calls: int,
) -> Scorecard:
    """Construit la Scorecard à partir de tous les DQResult."""

    # ── Agrégats par dimension ────────────────────────────────────────────────
    by_dim: dict[str, DimensionSummary] = {}
    for r in all_results:
        ds = by_dim.setdefault(r.dimension, DimensionSummary(dimension=r.dimension))
        ds.nb_tested += 1
        if r.status == DQStatus.VALID:
            ds.nb_v += 1
        elif r.status == DQStatus.SUSPECT:
            ds.nb_s += 1
        else:
            ds.nb_iv += 1
            if r.impact == ImpactLevel.HIGH:
                ds.nb_iv_high += 1

    for ds in by_dim.values():
        if ds.nb_tested > 0:
            ds.dimension_score = round(
                (ds.nb_v + 0.5 * ds.nb_s) / ds.nb_tested, 4
            )

    # ── Agrégats par enregistrement ───────────────────────────────────────────
    by_rec: dict[str, RecordSummary] = {}
    for r in all_results:
        rid = r.record_id
        if rid not in by_rec:
            by_rec[rid] = RecordSummary(
                record_id=rid,
                dataset=dataset,
                worst_status=DQStatus.VALID,
                global_score=1.0,
                is_mastered_eligible=True,
                financial_impact_total_usd=0.0,
            )
        rec = by_rec[rid]

        # Mise à jour worst_status
        if r.status == DQStatus.INVALID:
            rec.worst_status = DQStatus.INVALID
            if r.impact == ImpactLevel.HIGH:
                rec.is_mastered_eligible = False
                if r.dimension not in rec.dimensions_iv:
                    rec.dimensions_iv.append(r.dimension)
        elif r.status == DQStatus.SUSPECT and rec.worst_status == DQStatus.VALID:
            rec.worst_status = DQStatus.SUSPECT
            if r.dimension not in rec.dimensions_s:
                rec.dimensions_s.append(r.dimension)

        if r.financial_impact_usd:
            rec.financial_impact_total_usd += r.financial_impact_usd

    # Recalcul global_score par enregistrement
    for rid, rec in by_rec.items():
        status_map = {DQStatus.VALID: 1.0, DQStatus.SUSPECT: 0.5, DQStatus.INVALID: 0.0}
        rec.global_score = round(status_map.get(rec.worst_status, 0.0), 4)

    # ── Compteurs globaux ─────────────────────────────────────────────────────
    nb_iv_total = sum(1 for r in all_results if r.status == DQStatus.INVALID)
    nb_iv_high = sum(
        1 for r in all_results
        if r.status == DQStatus.INVALID and r.impact == ImpactLevel.HIGH
    )
    nb_s_total = sum(1 for r in all_results if r.status == DQStatus.SUSPECT)
    nb_mastered = sum(1 for rec in by_rec.values() if rec.is_mastered_eligible)

    # ── Score global (moyenne des scores dimensions) ──────────────────────────
    dim_scores = [ds.dimension_score for ds in by_dim.values() if ds.nb_tested > 0]
    global_score = round(
        (sum(dim_scores) / len(dim_scores) * 100) if dim_scores else 100.0, 2
    )

    # ── Impact financier ──────────────────────────────────────────────────────
    fi_total = sum(r.financial_impact_usd or 0.0 for r in all_results)
    fi_by_dim: dict[str, float] = {}
    for r in all_results:
        if r.financial_impact_usd:
            fi_by_dim[r.dimension] = fi_by_dim.get(r.dimension, 0.0) + r.financial_impact_usd

    financial_impact = FinancialImpact(
        total_usd=fi_total,
        by_dimension=fi_by_dim,
    )

    return Scorecard(
        scorecard_id=uuid4().hex,
        dataset=dataset,
        pipeline_env=cfg.pipeline_env,
        evaluated_at=datetime.now(tz=timezone.utc),
        total_records=len(records),
        total_fields_tested=len(all_results),
        results=all_results,
        by_dimension=by_dim,
        by_record=by_rec,
        global_dq_score=global_score,
        nb_iv_total=nb_iv_total,
        nb_iv_high_impact=nb_iv_high,
        nb_s_total=nb_s_total,
        nb_records_mastered_eligible=nb_mastered,
        financial_impact=financial_impact,
        nb_ml_anomalies=len(ml_anomaly_ids),
        ml_anomaly_record_ids=ml_anomaly_ids,
        llm_cost_usd=0.0,                     # estimation future
        nb_llm_remediations=nb_llm_calls,
        pipeline_duration_seconds=round(time.time() - start_time, 3),
    )


# ── Classe principale ─────────────────────────────────────────────────────────

class DQOrchestrator:
    """
    Orchestrateur principal du pipeline DQ.

    Exemple minimal
    ---------------
    >>> orch = DQOrchestrator()
    >>> sc = orch.run_from_csv("tests/fixtures/policies_invalid.csv", "policies")
    >>> print(sc.global_dq_score)

    Avec toutes les options
    -----------------------
    >>> cfg = OrchestratorConfig(pipeline_env="production", ml_enabled=True, llm_enabled=False)
    >>> orch = DQOrchestrator(cfg)
    >>> sc = orch.run(records, dataset="claims")
    """

    def __init__(self, config: OrchestratorConfig | None = None) -> None:
        self.config = config or OrchestratorConfig()

    # ── Entrée principale ────────────────────────────────────────────────────

    def run(self, records: list[dict], dataset: str) -> Scorecard:
        """
        Exécute le pipeline complet sur une liste de records déjà chargée.

        Parameters
        ----------
        records : list[dict] — enregistrements (chaque dict = 1 ligne CSV)
        dataset : str        — nom logique du dataset (policies, claims, logs, model_metadata)

        Returns
        -------
        Scorecard avec tous les agrégats DQ.
        """
        start = time.time()
        cfg = self.config

        # Injecter dataset + record_id canonique (requis par toutes les dimensions)
        id_field = _ID_FIELD.get(dataset, "record_id")
        tagged = []
        for r in records:
            enriched = {**r, "dataset": dataset}
            if "record_id" not in enriched and id_field in enriched:
                enriched["record_id"] = enriched[id_field]
            tagged.append(enriched)

        # ── Étape 1 : Dimensions par enregistrement ──────────────────────────
        record_config = _build_record_config(cfg)
        all_results: list[DQResult] = []

        for record in tagged:
            for dim in _DIMENSIONS:
                try:
                    results = dim.validate(record, record_config)
                    all_results.extend(results)
                except Exception:  # noqa: BLE001
                    # Une dimension qui plante ne doit pas tuer le pipeline
                    pass

        # ── Étape 2 : Collection (batch-level) ──────────────────────────────
        batch_config = _build_batch_config(cfg, tagged)
        # On appelle Collection sur un record factice représentant le batch entier
        batch_record = {
            "record_id": f"BATCH_{dataset}",
            "dataset": dataset,
            **batch_config,
        }
        try:
            coll_results = _COLLECTION_DIM.validate(batch_record, batch_config)
            all_results.extend(coll_results)
        except Exception:  # noqa: BLE001
            pass

        # ── Étape 3 : ML Anomaly Detection (optionnel) ───────────────────────
        ml_anomaly_ids: list[str] = []
        if cfg.ml_enabled and len(tagged) >= 10:
            try:
                from findata_dq.ai.anomaly_detector import MLAnomalyDetector  # noqa: PLC0415
                detector = MLAnomalyDetector(
                    contamination=cfg.ml_contamination,
                    n_estimators=cfg.ml_n_estimators,
                )
                ml_results = detector.fit_predict(tagged)
                all_results.extend(ml_results)
                ml_anomaly_ids = [
                    r.record_id for r in ml_results
                    if r.status == DQStatus.INVALID
                ]
            except Exception:  # noqa: BLE001
                pass

        # ── Étape 4 : LLM Remédiation (optionnel) ────────────────────────────
        nb_llm_calls = 0
        if cfg.llm_enabled:
            try:
                from findata_dq.ai.remediation import LLMRemediator  # noqa: PLC0415
                remediator = LLMRemediator(
                    model=cfg.llm_model,
                    max_calls=cfg.llm_max_calls,
                    api_key=cfg.anthropic_api_key,
                )
                all_results = remediator.remediate(all_results)
                nb_llm_calls = sum(
                    1 for r in all_results
                    if r.remediation and r.remediation.generated_by == "LLM"
                )
            except Exception:  # noqa: BLE001
                pass

        # ── Étape 5 : Aggregation → Scorecard ────────────────────────────────
        return _aggregate(
            records=tagged,
            all_results=all_results,
            dataset=dataset,
            start_time=start,
            cfg=cfg,
            ml_anomaly_ids=ml_anomaly_ids,
            nb_llm_calls=nb_llm_calls,
        )

    def run_from_csv(
        self,
        path: Path | str,
        dataset: str | None = None,
    ) -> Scorecard:
        """
        Charge un CSV et exécute le pipeline.

        Parameters
        ----------
        path    : chemin vers le fichier CSV
        dataset : nom logique (si None, déduit depuis le nom de fichier)
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Fichier introuvable : {path}")

        ds = dataset or _infer_dataset(path)
        records = _load_csv(path)
        return self.run(records, ds)

    # ── Accès aux enregistrements Mastered ───────────────────────────────────

    def get_mastered_records(
        self,
        records: list[dict],
        scorecard: Scorecard,
    ) -> list[dict]:
        """
        Retourne les enregistrements éligibles à l'état Mastered.
        Un enregistrement est Mastered si aucune règle IV-High ne le bloque.
        """
        eligible_ids = {
            rid for rid, rec in scorecard.by_record.items()
            if rec.is_mastered_eligible
        }
        # Fallback : si aucun record_id standard, utilise num_police / id_sinistre
        id_field = next(
            (k for k in ("record_id", "num_police", "id_sinistre", "id") if k in (records[0] if records else {})),
            None,
        )
        if id_field is None:
            return records  # impossible de filtrer

        return [r for r in records if str(r.get(id_field, "")) in eligible_ids]
