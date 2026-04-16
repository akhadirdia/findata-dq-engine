"""
Dimension 2 — Timeliness (Actualité)

Logique : Vérifier la fraîcheur des données par rapport à la date du jour.

  Polices / Sinistres (données assurantielles) :
    V  si delta_jours < 30
    S  si 30 <= delta_jours < 90
    IV si delta_jours >= 90

  Logs de sécurité (données temps-réel) :
    V  si delta_minutes < 60
    S  si 60 <= delta_minutes < 240
    IV si delta_minutes >= 240

  Métadonnées modèles IA :
    V  si delta_jours < 7
    S  si 7 <= delta_jours < 30
    IV si delta_jours >= 30
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

from findata_dq.dimensions.base import BaseDimension
from findata_dq.models.dq_result import DQResult, DQStatus, ImpactLevel

# Seuils en jours (polices/sinistres) — surchargeables via .env
_V_DAYS  = int(os.getenv("TIMELINESS_V_DAYS", "30"))
_S_DAYS  = int(os.getenv("TIMELINESS_S_DAYS", "90"))

# Seuils en minutes (logs temps-réel)
_V_MINUTES = 60
_S_MINUTES = 240

# Seuils en jours (modèles IA)
_V_MODEL_DAYS = 7
_S_MODEL_DAYS = 30

# Champs de date à vérifier par dataset
_DATE_FIELDS: dict[str, dict[str, str]] = {
    "policies": {
        "date_creation": "insurance",
    },
    "claims": {
        "date_creation": "insurance",
        "date_sinistre": "insurance",
    },
    "logs": {
        "timestamp": "realtime",
        "date_creation": "realtime",
    },
    "model_metadata": {
        "last_drift_check": "model",
        "date_creation": "model",
        "training_date": "model",
    },
}


def _parse_date_field(value: Any) -> datetime | None:
    """Tente de parser une valeur en datetime UTC."""
    if value is None or str(value).strip() in ("", "null", "none", "nan"):
        return None
    s = str(value).strip()
    # Python 3.7+ fromisoformat gère tous les formats ISO 8601 modernes (avec timezone)
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    except ValueError:
        pass
    # Fallback : formats strptime classiques
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=UTC)
        except ValueError:
            continue
    return None


def _classify_insurance(delta_days: float) -> str:
    if delta_days < _V_DAYS:
        return DQStatus.VALID
    if delta_days < _S_DAYS:
        return DQStatus.SUSPECT
    return DQStatus.INVALID


def _classify_realtime(delta_minutes: float) -> str:
    if delta_minutes < _V_MINUTES:
        return DQStatus.VALID
    if delta_minutes < _S_MINUTES:
        return DQStatus.SUSPECT
    return DQStatus.INVALID


def _classify_model(delta_days: float) -> str:
    if delta_days < _V_MODEL_DAYS:
        return DQStatus.VALID
    if delta_days < _S_MODEL_DAYS:
        return DQStatus.SUSPECT
    return DQStatus.INVALID


class Timeliness(BaseDimension):
    """
    Dimension 2 — Timeliness.
    Vérifie la fraîcheur des données selon leur type (assurance, temps-réel, modèles IA).
    """

    name = "Timeliness"
    description = "Vérifie la fraîcheur des données par rapport à la date du jour."
    default_impact = ImpactLevel.HIGH

    def validate(
        self,
        record: dict[str, Any],
        config: dict[str, Any] | None = None,
    ) -> list[DQResult]:
        config = config or {}
        """
        Paramètres config :
          date_fields : dict[str, str]  — {field_name: 'insurance'|'realtime'|'model'}
          dataset     : str
          reference_dt: datetime        — date de référence (défaut: now UTC, utile pour tests)
        """
        dataset = record.get("dataset", config.get("dataset", "unknown"))
        now_utc = config.get("reference_dt", datetime.now(UTC))
        results: list[DQResult] = []

        date_fields: dict[str, str] = config.get(
            "date_fields",
            _DATE_FIELDS.get(dataset, {}),
        )

        for field, mode in date_fields.items():
            raw_value = record.get(field)
            dt = _parse_date_field(raw_value)

            if dt is None:
                # Champ absent → Completeness s'en charge, on ne double pas
                continue

            delta = now_utc - dt
            delta_days = delta.total_seconds() / 86_400
            delta_minutes = delta.total_seconds() / 60

            if mode == "realtime":
                status = _classify_realtime(delta_minutes)
                rule = (
                    f"Logs temps-réel : {delta_minutes:.0f} min écoulées "
                    f"(V<{_V_MINUTES}m, S<{_S_MINUTES}m, IV>={_S_MINUTES}m)"
                )
                details = {
                    "mode": "realtime",
                    "delta_minutes": round(delta_minutes, 1),
                    "v_threshold_minutes": _V_MINUTES,
                    "s_threshold_minutes": _S_MINUTES,
                }
            elif mode == "model":
                status = _classify_model(delta_days)
                rule = (
                    f"Modèle IA : {delta_days:.1f} jours depuis le dernier check "
                    f"(V<{_V_MODEL_DAYS}j, S<{_S_MODEL_DAYS}j)"
                )
                details = {
                    "mode": "model",
                    "delta_days": round(delta_days, 1),
                    "v_threshold_days": _V_MODEL_DAYS,
                    "s_threshold_days": _S_MODEL_DAYS,
                }
            else:  # insurance (défaut)
                status = _classify_insurance(delta_days)
                rule = (
                    f"Données assurantielles : {delta_days:.0f} jours "
                    f"(V<{_V_DAYS}j, S<{_S_DAYS}j, IV>={_S_DAYS}j)"
                )
                details = {
                    "mode": "insurance",
                    "delta_days": round(delta_days, 1),
                    "v_threshold_days": _V_DAYS,
                    "s_threshold_days": _S_DAYS,
                }

            impact = ImpactLevel.HIGH if status == DQStatus.INVALID else ImpactLevel.LOW

            results.append(self._make_result(
                record=record,
                field_name=field,
                field_value=str(raw_value),
                status=status,
                impact=impact,
                rule_applied=rule,
                details=details,
            ))

        return results
