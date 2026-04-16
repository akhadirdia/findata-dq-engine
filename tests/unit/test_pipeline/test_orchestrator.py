"""
Tests Étape 8 — DQOrchestrator.

Stratégie
---------
- Tests unitaires sur des micro-datasets in-memory (rapides, déterministes)
- Un test d'intégration sur policies_invalid.csv (500 lignes)
- ML activé, LLM désactivé (pas de coût API)

Blocs
-----
  Bloc 1 : Configuration et initialisation
  Bloc 2 : Comportement sur micro-datasets (V / S / IV)
  Bloc 3 : Structure de la Scorecard
  Bloc 4 : Intégration policies_invalid.csv
  Bloc 5 : get_mastered_records
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from findata_dq.models.dq_result import DQStatus
from findata_dq.pipeline.orchestrator import (
    DQOrchestrator,
    OrchestratorConfig,
    _infer_dataset,
)

FIXTURES_DIR = Path(__file__).parents[3] / "tests" / "fixtures"
TODAY = date.today()


# ── Micro-datasets ────────────────────────────────────────────────────────────

def _make_policy(
    num: str = "AU-123456",
    client: str = "CLI-0001",
    effet: str | None = None,
    expiration: str | None = None,
    prime: str = "1500.00",
    montant: str = "50000.00",
    statut: str = "active",
    date_creation_offset_days: int = 10,
) -> dict:
    return {
        "num_police": num,
        "id_client": client,
        "date_effet": effet or (TODAY - timedelta(days=100)).isoformat(),
        "date_expiration": expiration or (TODAY + timedelta(days=265)).isoformat(),
        "type_couverture": "auto",
        "prime_annuelle": prime,
        "montant_assure": montant,
        "statut_police": statut,
        "franchise": "500",
        "date_creation": (TODAY - timedelta(days=date_creation_offset_days)).isoformat(),
    }


# ── Bloc 1 : Configuration ────────────────────────────────────────────────────

class TestOrchestratorConfig:

    def test_config_defaut(self):
        cfg = OrchestratorConfig()
        assert cfg.pipeline_env == "development"
        assert cfg.ml_enabled is True
        assert cfg.llm_enabled is False
        assert cfg.llm_max_calls == 5

    def test_config_custom(self):
        cfg = OrchestratorConfig(
            pipeline_env="production",
            ml_enabled=False,
            retention_days=730,
        )
        assert cfg.pipeline_env == "production"
        assert cfg.ml_enabled is False
        assert cfg.retention_days == 730

    def test_orchestrator_init_sans_config(self):
        orch = DQOrchestrator()
        assert orch.config.pipeline_env == "development"

    def test_orchestrator_init_avec_config(self):
        cfg = OrchestratorConfig(pipeline_env="staging")
        orch = DQOrchestrator(cfg)
        assert orch.config.pipeline_env == "staging"


# ── Bloc 2 : Comportement sur micro-datasets ──────────────────────────────────

class TestOrchestratorBehavior:

    @pytest.fixture(scope="class")
    def orch_no_ml(self):
        return DQOrchestrator(OrchestratorConfig(ml_enabled=False, llm_enabled=False))

    def test_run_liste_vide_retourne_scorecard(self, orch_no_ml):
        sc = orch_no_ml.run([], "policies")
        assert sc.total_records == 0
        assert sc.global_dq_score >= 0.0

    def test_run_un_enregistrement_valide(self, orch_no_ml):
        records = [_make_policy()]
        sc = orch_no_ml.run(records, "policies")
        assert sc.total_records == 1
        assert sc.total_fields_tested > 0

    def test_run_detecte_champ_obligatoire_manquant(self, orch_no_ml):
        """Un enregistrement sans num_police → au moins 1 IV Completeness."""
        record = _make_policy()
        record.pop("num_police")
        sc = orch_no_ml.run([record], "policies")
        comp_ivs = [
            r for r in sc.results
            if r.dimension == "Completeness" and r.status == DQStatus.INVALID
        ]
        assert len(comp_ivs) >= 1

    def test_run_detecte_date_hors_periode(self, orch_no_ml):
        """Enregistrement avec date_creation vieille de 100 jours → Timeliness IV (>90j)."""
        record = _make_policy(date_creation_offset_days=100)
        sc = orch_no_ml.run([record], "policies")
        timeliness = [
            r for r in sc.results
            if r.dimension == "Timeliness"
            and r.status in (DQStatus.SUSPECT, DQStatus.INVALID)
        ]
        assert len(timeliness) >= 1

    def test_run_tous_valides_score_eleve(self, orch_no_ml):
        """5 polices propres → score global > 70."""
        records = [_make_policy(num=f"AU-{i:06d}", client=f"CLI-{i:04d}") for i in range(5)]
        sc = orch_no_ml.run(records, "policies")
        assert sc.global_dq_score > 70.0

    def test_run_avec_ml_enabled(self):
        """ML activé sur 50 records — vérifie que le pipeline ne plante pas."""
        cfg = OrchestratorConfig(ml_enabled=True, llm_enabled=False)
        orch = DQOrchestrator(cfg)
        records = [
            _make_policy(num=f"AU-{i:06d}", client=f"CLI-{i:04d}", prime=str(1000 + i * 10))
            for i in range(50)
        ]
        sc = orch.run(records, "policies")
        assert sc.total_records == 50
        # ML résultats ajoutés
        ml_results = [r for r in sc.results if r.dimension == "AnomalyDetection"]
        assert len(ml_results) == 50


# ── Bloc 3 : Structure de la Scorecard ───────────────────────────────────────

class TestScorecardStructure:

    @pytest.fixture(scope="class")
    def scorecard_small(self):
        cfg = OrchestratorConfig(ml_enabled=False, llm_enabled=False)
        orch = DQOrchestrator(cfg)
        records = [
            _make_policy(num=f"AU-{i:06d}", client=f"CLI-{i:04d}")
            for i in range(10)
        ]
        return orch.run(records, "policies")

    def test_scorecard_id_non_vide(self, scorecard_small):
        assert len(scorecard_small.scorecard_id) > 0

    def test_scorecard_by_dimension_non_vide(self, scorecard_small):
        assert len(scorecard_small.by_dimension) > 0

    def test_scorecard_by_record_10_entrees(self, scorecard_small):
        # Collection crée un record BATCH_, les 10 polices ont leur propre entrée
        policy_recs = {
            rid for rid in scorecard_small.by_record
            if not rid.startswith("BATCH_")
        }
        assert len(policy_recs) == 10

    def test_scorecard_global_score_entre_0_et_100(self, scorecard_small):
        assert 0.0 <= scorecard_small.global_dq_score <= 100.0

    def test_scorecard_duration_positive(self, scorecard_small):
        assert scorecard_small.pipeline_duration_seconds is not None
        assert scorecard_small.pipeline_duration_seconds >= 0.0

    def test_scorecard_pass_rate_coherent(self, scorecard_small):
        pr = scorecard_small.pass_rate
        assert 0.0 <= pr <= 1.0

    def test_scorecard_nb_mastered_coherent(self, scorecard_small):
        assert 0 <= scorecard_small.nb_records_mastered_eligible <= scorecard_small.total_records

    def test_scorecard_get_iv_results(self, scorecard_small):
        ivs = scorecard_small.get_iv_results()
        assert isinstance(ivs, list)

    def test_scorecard_heatmap_data(self, scorecard_small):
        data = scorecard_small.to_heatmap_data()
        assert isinstance(data, list)
        if data:
            assert "dimension" in data[0]
            assert "status" in data[0]

    def test_infer_dataset_policies(self):
        assert _infer_dataset("policies_invalid.csv") == "policies"

    def test_infer_dataset_claims(self):
        assert _infer_dataset("claims_fraud.csv") == "claims"

    def test_infer_dataset_logs(self):
        assert _infer_dataset("access_logs.csv") == "logs"

    def test_infer_dataset_model(self):
        assert _infer_dataset("model_metadata.csv") == "model_metadata"


# ── Bloc 4 : Intégration policies_invalid.csv ─────────────────────────────────

class TestIntegrationPoliciesInvalid:
    """
    Test end-to-end sur les 500 polices de policies_invalid.csv.
    Defects intentionnels : COMPLETENESS_IV, TIMELINESS_IV,
                            CONFORMITY_IV, CONGRUENCE_IV, BUSINESS_RULES_IV
    """

    @pytest.fixture(scope="class")
    def scorecard(self):
        path = FIXTURES_DIR / "policies_invalid.csv"
        if not path.exists():
            pytest.skip(f"Fixture manquante : {path}")

        cfg = OrchestratorConfig(ml_enabled=False, llm_enabled=False)
        orch = DQOrchestrator(cfg)
        return orch.run_from_csv(path, "policies")

    def test_charge_500_records(self, scorecard):
        assert scorecard.total_records == 500

    def test_nb_iv_positif(self, scorecard):
        """Il y a des defects intentionnels → nb_iv_total > 0."""
        assert scorecard.nb_iv_total > 0

    def test_nb_mastered_inferieur_total(self, scorecard):
        """Les polices avec defects ne sont pas toutes Mastered-éligibles."""
        assert scorecard.nb_records_mastered_eligible < scorecard.total_records

    def test_score_global_raisonnable(self, scorecard):
        """80 % des polices sont NONE → score > 50."""
        assert scorecard.global_dq_score > 50.0

    def test_completeness_presente_dans_by_dimension(self, scorecard):
        assert "Completeness" in scorecard.by_dimension

    def test_timeliness_presente_dans_by_dimension(self, scorecard):
        assert "Timeliness" in scorecard.by_dimension

    def test_scorecard_id_unique(self, scorecard):
        assert len(scorecard.scorecard_id) == 32  # UUID hex sans tirets

    def test_run_from_csv_infer_dataset(self):
        """run_from_csv sans dataset explicite doit inférer 'policies'."""
        path = FIXTURES_DIR / "policies_invalid.csv"
        if not path.exists():
            pytest.skip(f"Fixture manquante : {path}")
        cfg = OrchestratorConfig(ml_enabled=False, llm_enabled=False)
        orch = DQOrchestrator(cfg)
        sc = orch.run_from_csv(path)  # pas de dataset explicite
        assert sc.dataset == "policies"


# ── Bloc 5 : get_mastered_records ─────────────────────────────────────────────

class TestGetMasteredRecords:

    def test_retourne_liste(self):
        cfg = OrchestratorConfig(ml_enabled=False)
        orch = DQOrchestrator(cfg)
        records = [_make_policy(num=f"AU-{i:06d}", client=f"CLI-{i:04d}") for i in range(5)]
        sc = orch.run(records, "policies")
        mastered = orch.get_mastered_records(records, sc)
        assert isinstance(mastered, list)

    def test_mastered_subset_du_total(self):
        cfg = OrchestratorConfig(ml_enabled=False)
        orch = DQOrchestrator(cfg)
        records = [_make_policy(num=f"AU-{i:06d}", client=f"CLI-{i:04d}") for i in range(10)]
        sc = orch.run(records, "policies")
        mastered = orch.get_mastered_records(records, sc)
        assert len(mastered) <= len(records)

    def test_liste_vide_retourne_vide(self):
        cfg = OrchestratorConfig(ml_enabled=False)
        orch = DQOrchestrator(cfg)
        sc = orch.run([], "policies")
        mastered = orch.get_mastered_records([], sc)
        assert mastered == []
