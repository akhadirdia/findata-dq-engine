"""
Classe abstraite BaseDimension.
Toutes les 12 dimensions héritent de cette classe et implémentent validate().

Contrat :
  - Input  : record (dict), config (dict optionnel)
  - Output : list[DQResult] — un résultat par champ testé
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional
from uuid import uuid4

from findata_dq.models.dq_result import DQResult, DQStatus, ImpactLevel


class BaseDimension(ABC):
    """
    Classe de base pour toutes les dimensions DQ (Buzzelli + Extended).

    Usage :
        class Completeness(BaseDimension):
            name = "Completeness"
            default_impact = "H"

            def validate(self, record, config={}):
                results = []
                for field, is_mandatory in config.get("mandatory_fields", {}).items():
                    value = record.get(field)
                    status = "IV" if (is_mandatory and value is None) else "V"
                    results.append(self._make_result(
                        record=record, field_name=field, field_value=value,
                        status=status, impact="H" if status == "IV" else "L",
                        rule_applied=f"{'Mandatory' if is_mandatory else 'Optional'} field check",
                    ))
                return results
    """

    # ── À surcharger dans chaque dimension ────────────────────────────────────
    name: str = "BaseDimension"
    description: str = ""
    default_impact: str = ImpactLevel.MEDIUM

    @abstractmethod
    def validate(
        self,
        record: dict[str, Any],
        config: dict[str, Any] = {},
    ) -> list[DQResult]:
        """
        Valide un enregistrement selon cette dimension.

        Args:
            record : dict représentant une ligne du dataset
                     Doit contenir 'record_id' et 'dataset' au minimum.
            config : paramètres de la dimension (seuils, champs obligatoires, etc.)
                     Les valeurs par défaut sont dans .env ou CLAUDE.md section 4.

        Returns:
            list[DQResult] — un DQResult par champ testé.
            Retourner une liste vide si la dimension ne s'applique pas à ce record.
        """
        ...

    # ── Helper principal ──────────────────────────────────────────────────────

    def _make_result(
        self,
        record: dict[str, Any],
        field_name: str,
        status: str,
        impact: str,
        rule_applied: str,
        field_value: Any = None,
        details: Optional[dict[str, Any]] = None,
        financial_impact_usd: Optional[float] = None,
        score: Optional[float] = None,
    ) -> DQResult:
        """
        Construit un DQResult standardisé.
        À utiliser dans toutes les implémentations de validate().

        La valeur du champ est masquée si elle correspond à un champ PII connu,
        sauf en environnement production (où le masquage doit être fait en amont).
        """
        record_id = str(record.get("record_id", record.get("id", "UNKNOWN")))
        dataset = str(record.get("dataset", "unknown"))

        # Masquage PII dans les logs (sécurité)
        safe_value = self._mask_if_pii(field_name, field_value)

        # Score normalisé
        if score is None:
            score = {"V": 1.0, "S": 0.5, "IV": 0.0}.get(status, 0.0)

        return DQResult(
            datum_id=f"{record_id}_{field_name}_{self.name}_{uuid4().hex[:6]}",
            dataset=dataset,
            record_id=record_id,
            field_name=field_name,
            field_value=safe_value,
            dimension=self.name,
            status=status,
            impact=impact,
            score=score,
            rule_applied=rule_applied,
            details=details or {},
            financial_impact_usd=financial_impact_usd,
            evaluated_at=datetime.utcnow(),
            pipeline_env=os.getenv("PIPELINE_ENV", "development"),
        )

    # ── Helpers utilitaires ───────────────────────────────────────────────────

    @staticmethod
    def _mask_if_pii(field_name: str, value: Any) -> Optional[str]:
        """
        Masque la valeur si le champ est identifié comme PII.
        Les champs PII ne sont jamais stockés en clair dans DQResult.
        """
        PII_FIELDS = {
            "revenu_estime", "ip_address", "source_ip", "destination_ip",
            "email", "telephone", "nas", "numero_carte",
        }
        if field_name.lower() in PII_FIELDS:
            return "[MASKED]"
        if value is None:
            return None
        return str(value)

    @staticmethod
    def _classify_zscore(z: float) -> str:
        """
        Classifie un Z-score selon les seuils Buzzelli assurance.
        V : |Z| <= 2.0
        S : 2.0 < |Z| < 3.5
        IV : |Z| >= 3.5
        """
        v_threshold = float(os.getenv("ZSCORE_V_THRESHOLD", "2.0"))
        iv_threshold = float(os.getenv("ZSCORE_IV_THRESHOLD", "3.5"))
        abs_z = abs(z)
        if abs_z <= v_threshold:
            return DQStatus.VALID
        if abs_z < iv_threshold:
            return DQStatus.SUSPECT
        return DQStatus.INVALID

    @staticmethod
    def _classify_pct_deviation(pct: float, v_threshold: float, iv_threshold: float) -> str:
        """
        Classifie un écart en pourcentage.
        Utilisé par Prior Value Comparison et Comparison to Average (Congruence).
        """
        if pct < v_threshold:
            return DQStatus.VALID
        if pct < iv_threshold:
            return DQStatus.SUSPECT
        return DQStatus.INVALID

    def __repr__(self) -> str:
        return f"<Dimension:{self.name}>"


# ─── Registre des dimensions ──────────────────────────────────────────────────

class DimensionRegistry:
    """
    Registre central de toutes les dimensions actives.
    Permet au pipeline orchestrateur d'itérer sur les 12 dimensions
    sans les importer individuellement.

    Usage :
        registry = DimensionRegistry()
        registry.register(Completeness())
        for dim in registry.all():
            results = dim.validate(record, config)
    """

    def __init__(self) -> None:
        self._dimensions: dict[str, BaseDimension] = {}

    def register(self, dimension: BaseDimension) -> None:
        self._dimensions[dimension.name] = dimension

    def get(self, name: str) -> Optional[BaseDimension]:
        return self._dimensions.get(name)

    def all(self) -> list[BaseDimension]:
        return list(self._dimensions.values())

    def names(self) -> list[str]:
        return list(self._dimensions.keys())

    def __len__(self) -> int:
        return len(self._dimensions)

    def __repr__(self) -> str:
        return f"DimensionRegistry({self.names()})"
