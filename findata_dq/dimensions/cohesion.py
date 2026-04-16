"""
Dimension 8 — Cohesion (Intégrité référentielle)

Logique : Les clés étrangères doivent référencer des enregistrements valides.
  clef_etrangere IN table_reference     → V
  clef_etrangere NOT IN table_reference → IV, Impact H

Contrôles assurance :
  sinistre.num_police    → policies.num_police
  sinistre.id_client     → clients.id_client
  log.user_id            → users.user_id
  model.model_id         → model_registry.model_id
"""

from __future__ import annotations

from typing import Any

from findata_dq.dimensions.base import BaseDimension
from findata_dq.models.dq_result import DQResult, DQStatus, ImpactLevel

# Définition des relations FK par dataset
# Format : {dataset: [(fk_field, reference_set_config_key)]}
_FK_DEFINITIONS: dict[str, list[tuple[str, str]]] = {
    "claims": [
        ("num_police", "valid_policy_ids"),
        ("id_client", "valid_client_ids"),
    ],
    "logs": [
        ("user_id", "valid_user_ids"),
    ],
    "vehicles": [
        ("id_client", "valid_client_ids"),
        ("num_police", "valid_policy_ids"),
    ],
    "model_metadata": [
        ("model_id", "valid_model_registry_ids"),
    ],
    "fairness_metrics": [
        ("model_id", "valid_model_registry_ids"),
    ],
}


class Cohesion(BaseDimension):
    """
    Dimension 8 — Cohesion.
    Vérifie que les clés étrangères référencent des enregistrements existants.
    """

    name = "Cohesion"
    description = "Vérifie l'intégrité référentielle : les FK doivent exister dans les tables de référence."
    default_impact = ImpactLevel.HIGH

    def validate(
        self,
        record: dict[str, Any],
        config: dict[str, Any] = {},
    ) -> list[DQResult]:
        """
        Paramètres config (REQUIS pour fonctionner) :
          fk_checks       : list[tuple[str, set]]  — [(field_name, valid_ids_set), ...]
                            Si fourni, écrase la résolution automatique par dataset.
          dataset         : str

        Exemple :
          config = {
            "fk_checks": [
                ("num_police", {"AU-123456", "HA-987654"}),
                ("id_client",  {"CLI-0000001", "CLI-0000002"}),
            ]
          }

        Note : Si ni fk_checks ni les reference sets ne sont fournis dans config,
               la dimension retourne une liste vide (pas de référence disponible).
        """
        dataset = record.get("dataset", config.get("dataset", "unknown"))
        results: list[DQResult] = []

        # Mode explicite : liste de (field, valid_set) fournie directement
        explicit_checks: list[tuple[str, set]] = config.get("fk_checks", [])
        if explicit_checks:
            checks = explicit_checks
        else:
            # Mode automatique : résolution depuis config par dataset
            fk_defs = _FK_DEFINITIONS.get(dataset, [])
            checks = []
            for field, config_key in fk_defs:
                ref_set: set | None = config.get(config_key)
                if ref_set is not None:
                    checks.append((field, ref_set))

        if not checks:
            return results  # pas de référence disponible — ne pas échouer silencieusement

        for field, valid_ids in checks:
            value = record.get(field)

            if value is None or str(value).strip() == "":
                # Completeness s'en charge
                continue

            str_value = str(value).strip()

            if str_value in valid_ids:
                results.append(self._make_result(
                    record=record,
                    field_name=field,
                    field_value=str_value,
                    status=DQStatus.VALID,
                    impact=ImpactLevel.LOW,
                    rule_applied=f"Clé '{field}' trouvée dans la table de référence.",
                    details={"ref_size": len(valid_ids)},
                ))
            else:
                results.append(self._make_result(
                    record=record,
                    field_name=field,
                    field_value=str_value,
                    status=DQStatus.INVALID,
                    impact=ImpactLevel.HIGH,
                    rule_applied=(
                        f"Clé étrangère '{field}' = '{str_value}' introuvable "
                        f"dans la table de référence ({len(valid_ids)} entrées valides)."
                    ),
                    details={"ref_size": len(valid_ids), "value": str_value},
                ))

        return results
