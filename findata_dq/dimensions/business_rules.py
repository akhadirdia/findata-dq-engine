"""
Dimension 9 — BusinessRules (Règles Métier Inter-Tables)

Logique : Contraintes métier qui requièrent une jointure ou une relation
entre plusieurs champs/tables. Ne peuvent pas être vérifiées champ par champ.

Règles assurance (sinistres) :
  R1 : date_sinistre DOIT être dans [date_effet, date_expiration]
  R2 : montant_reclame NE PEUT PAS dépasser montant_assure
  R3 : une police EXPIREE ne peut pas avoir de sinistre OUVERT

Règles sécurité (logs) :
  R4 : une action 'delete' requiert session_id actif
  R5 : montant_transaction > 50 000 requiert status_code == 200

Règles IA (modèles) :
  R6 : un modèle en production NE PEUT PAS avoir drift_score IV
"""

from __future__ import annotations

from datetime import date
from typing import Any

from findata_dq.dimensions.base import BaseDimension
from findata_dq.models.dq_result import DQResult, DQStatus, ImpactLevel

_HIGH_VALUE_TRANSACTION = 50_000.0


def _to_date(value: Any) -> date | None:
    """Parse une valeur en date."""
    if value is None or str(value).strip() in ("", "null", "none"):
        return None
    s = str(value).strip()
    try:
        return date.fromisoformat(s[:10])  # prend uniquement YYYY-MM-DD
    except ValueError:
        return None


class BusinessRules(BaseDimension):
    """
    Dimension 9 — BusinessRules.
    Applique les règles métier inter-champs propres au domaine assurantiel et IA.
    """

    name = "BusinessRules"
    description = "Valide les contraintes métier inter-champs (sinistres, logs, modèles IA)."
    default_impact = ImpactLevel.HIGH

    def validate(
        self,
        record: dict[str, Any],
        config: dict[str, Any] | None = None,
    ) -> list[DQResult]:
        config = config or {}
        """
        Les règles activées dépendent du dataset.
        Toutes les règles applicables sont évaluées automatiquement.

        Paramètres config :
          dataset      : str   — 'claims' | 'logs' | 'model_metadata'
          reference_dt : date  — date de référence (défaut : today, utile pour tests)
        """
        dataset = record.get("dataset", config.get("dataset", "unknown"))
        today = config.get("reference_dt", date.today())
        results: list[DQResult] = []

        if dataset == "claims":
            results += self._check_claims(record, today)
        elif dataset == "logs":
            results += self._check_logs(record)
        elif dataset == "model_metadata":
            results += self._check_model(record)
        else:
            # Tente toutes les règles si dataset inconnu
            results += self._check_claims(record, today)
            results += self._check_logs(record)
            results += self._check_model(record)

        return results

    # ── R1, R2, R3 — Sinistres ────────────────────────────────────────────────

    def _check_claims(self, record: dict, today: date) -> list[DQResult]:
        results = []

        date_sinistre = _to_date(record.get("date_sinistre"))
        date_effet = _to_date(record.get("date_effet_police") or record.get("date_effet"))
        date_expiration = _to_date(record.get("date_expiration_police") or record.get("date_expiration"))
        montant_reclame = record.get("montant_reclame")
        montant_assure = record.get("montant_assure_police") or record.get("montant_assure")
        statut_sinistre = str(record.get("statut_sinistre", "")).strip().lower()

        # R1 — date_sinistre dans la période de couverture
        if date_sinistre and date_effet and date_expiration:
            if date_sinistre < date_effet or date_sinistre > date_expiration:
                results.append(self._make_result(
                    record=record,
                    field_name="date_sinistre",
                    field_value=str(date_sinistre),
                    status=DQStatus.INVALID,
                    impact=ImpactLevel.HIGH,
                    rule_applied=(
                        f"R1 : date_sinistre ({date_sinistre}) hors période de couverture "
                        f"[{date_effet}, {date_expiration}]."
                    ),
                    details={
                        "rule": "R1",
                        "date_sinistre": str(date_sinistre),
                        "date_effet": str(date_effet),
                        "date_expiration": str(date_expiration),
                    },
                ))
            else:
                results.append(self._make_result(
                    record=record,
                    field_name="date_sinistre",
                    field_value=str(date_sinistre),
                    status=DQStatus.VALID,
                    impact=ImpactLevel.LOW,
                    rule_applied="R1 : date_sinistre dans la période de couverture.",
                    details={"rule": "R1"},
                ))

        # R2 — montant_reclame <= montant_assure
        if montant_reclame is not None and montant_assure is not None:
            try:
                reclame = float(montant_reclame)
                assure = float(montant_assure)
                if reclame > assure:
                    results.append(self._make_result(
                        record=record,
                        field_name="montant_reclame",
                        field_value=str(reclame),
                        status=DQStatus.INVALID,
                        impact=ImpactLevel.HIGH,
                        rule_applied=(
                            f"R2 : montant_reclame ({reclame:,.2f}) > montant_assure ({assure:,.2f}). "
                            f"Dépassement de {reclame - assure:,.2f}."
                        ),
                        details={
                            "rule": "R2",
                            "montant_reclame": reclame,
                            "montant_assure": assure,
                            "depassement": round(reclame - assure, 2),
                        },
                    ))
                else:
                    results.append(self._make_result(
                        record=record,
                        field_name="montant_reclame",
                        field_value=str(reclame),
                        status=DQStatus.VALID,
                        impact=ImpactLevel.LOW,
                        rule_applied="R2 : montant_reclame <= montant_assure.",
                        details={"rule": "R2"},
                    ))
            except (ValueError, TypeError):
                pass

        # R3 — police expirée ne peut pas avoir sinistre ouvert
        if statut_sinistre == "ouvert" and date_expiration:
            if date_expiration < today:
                results.append(self._make_result(
                    record=record,
                    field_name="statut_sinistre",
                    field_value=statut_sinistre,
                    status=DQStatus.INVALID,
                    impact=ImpactLevel.HIGH,
                    rule_applied=(
                        f"R3 : sinistre 'ouvert' sur police expirée le {date_expiration} "
                        f"(aujourd'hui : {today})."
                    ),
                    details={
                        "rule": "R3",
                        "statut_sinistre": statut_sinistre,
                        "date_expiration": str(date_expiration),
                        "today": str(today),
                    },
                ))
            else:
                results.append(self._make_result(
                    record=record,
                    field_name="statut_sinistre",
                    field_value=statut_sinistre,
                    status=DQStatus.VALID,
                    impact=ImpactLevel.LOW,
                    rule_applied="R3 : sinistre ouvert sur police active.",
                    details={"rule": "R3"},
                ))

        return results

    # ── R4, R5 — Logs ─────────────────────────────────────────────────────────

    def _check_logs(self, record: dict) -> list[DQResult]:
        results = []

        action_type = str(record.get("action_type", "")).strip().lower()
        session_id = record.get("session_id")
        status_code = record.get("status_code")
        montant = record.get("montant_transaction") or record.get("montant")

        # R4 — delete requiert session_id
        if action_type == "delete":
            session_absent = session_id is None or str(session_id).strip() in ("", "null", "none")
            if session_absent:
                results.append(self._make_result(
                    record=record,
                    field_name="session_id",
                    field_value=None,
                    status=DQStatus.INVALID,
                    impact=ImpactLevel.HIGH,
                    rule_applied="R4 : action 'delete' sans session_id actif — violation sécurité.",
                    details={"rule": "R4", "action_type": action_type},
                ))
            else:
                results.append(self._make_result(
                    record=record,
                    field_name="session_id",
                    field_value=str(session_id),
                    status=DQStatus.VALID,
                    impact=ImpactLevel.LOW,
                    rule_applied="R4 : action 'delete' avec session_id valide.",
                    details={"rule": "R4"},
                ))

        # R5 — montant > 50k requiert status_code 200
        if montant is not None and status_code is not None:
            try:
                m = float(montant)
                sc = int(status_code)
                if abs(m) > _HIGH_VALUE_TRANSACTION and sc != 200:
                    results.append(self._make_result(
                        record=record,
                        field_name="status_code",
                        field_value=str(sc),
                        status=DQStatus.SUSPECT,
                        impact=ImpactLevel.MEDIUM,
                        rule_applied=(
                            f"R5 : transaction {m:,.0f} > 50 000 avec status_code {sc} != 200."
                        ),
                        details={
                            "rule": "R5",
                            "montant": m,
                            "status_code": sc,
                            "threshold": _HIGH_VALUE_TRANSACTION,
                        },
                    ))
            except (ValueError, TypeError):
                pass

        return results

    # ── R6 — Modèles IA ───────────────────────────────────────────────────────

    def _check_model(self, record: dict) -> list[DQResult]:
        results = []

        statut_prod = str(record.get("statut_production", "")).strip().lower()
        drift_status = str(record.get("drift_status", "")).strip().upper()
        drift_score = record.get("drift_score")

        if statut_prod == "production" and drift_status == "IV":
            details: dict = {"rule": "R6", "statut_production": statut_prod, "drift_status": drift_status}
            if drift_score is not None:
                details["drift_score"] = float(drift_score)

            results.append(self._make_result(
                record=record,
                field_name="drift_status",
                field_value=drift_status,
                status=DQStatus.INVALID,
                impact=ImpactLevel.HIGH,
                rule_applied=(
                    f"R6 : modèle en production avec drift IV "
                    f"(PSI={drift_score}) — retraining requis immédiatement."
                ),
                details=details,
            ))
        elif statut_prod == "production":
            results.append(self._make_result(
                record=record,
                field_name="drift_status",
                field_value=drift_status or "N/A",
                status=DQStatus.VALID,
                impact=ImpactLevel.LOW,
                rule_applied="R6 : modèle en production avec drift acceptable.",
                details={"rule": "R6"},
            ))

        return results
