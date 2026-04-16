"""
Remédiation LLM — Étape 7.

Principe
--------
Pour chaque DQResult de statut IV, on demande à Claude de proposer :
  - Une valeur corrigée (suggested_value)
  - Une action (auto_fix / human_review / reject)
  - Une explication lisible
  - L'impact si la donnée n'est pas corrigée
  - Un score de confiance

Design
------
- Appel JSON structuré via le SDK anthropic (claude-haiku-4-5 par défaut : rapide et peu coûteux)
- Parsing strict du JSON retourné (pas de markdown, pas de prose)
- Fallback rule-based si l'API est indisponible ou si le budget est épuisé
- Limite : MAX_CALLS_PER_RUN = 5 (paramétrable via env LLM_MAX_CALLS)

Utilisation
-----------
    from findata_dq.ai.remediation import LLMRemediator

    remediator = LLMRemediator()                   # clé via ANTHROPIC_API_KEY
    results = remediator.remediate(dq_results)      # list[DQResult] avec .remediation rempli
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any

from findata_dq.models.dq_result import DQResult, DQStatus, RemediationResult

logger = logging.getLogger(__name__)

# ── Paramètres ────────────────────────────────────────────────────────────────

_DEFAULT_MODEL = os.environ.get("LLM_MODEL", "claude-haiku-4-5-20251001")
_MAX_CALLS = int(os.environ.get("LLM_MAX_CALLS", "5"))
_MAX_TOKENS = int(os.environ.get("LLM_MAX_TOKENS", "512"))

# ── Prompt système ────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """\
Tu es un expert en qualité des données pour un assureur canadien.
On te donne un enregistrement de données marqué INVALIDE (IV) avec le détail de la règle enfreinte.
Tu dois proposer une remédiation structurée en JSON pur (sans markdown, sans commentaire).

Format JSON attendu (EXACTEMENT ces clés) :
{
  "suggested_value": "<valeur corrigée ou null si impossible>",
  "confidence": <float entre 0.0 et 1.0>,
  "action": "<auto_fix | human_review | reject>",
  "explanation": "<explication courte en français (max 2 phrases)>",
  "impact_si_non_corrige": "<risque financier ou réglementaire si non corrigé>"
}

Règles de décision :
- action = "auto_fix"      si confidence >= 0.85 et la correction est déterministe
- action = "human_review"  si confidence entre 0.50 et 0.84, ou si la donnée est ambiguë
- action = "reject"        si la donnée est irrécupérable (fraude avérée, incohérence totale)

Réponds UNIQUEMENT avec le JSON, rien d'autre.
"""

# ── Prompt utilisateur ────────────────────────────────────────────────────────

def _build_user_prompt(result: DQResult) -> str:
    return (
        f"Dataset : {result.dataset}\n"
        f"Enregistrement : {result.record_id}\n"
        f"Champ : {result.field_name}\n"
        f"Valeur actuelle : {result.field_value or '(non disponible)'}\n"
        f"Dimension DQ : {result.dimension}\n"
        f"Règle enfreinte : {result.rule_applied}\n"
        f"Détails : {json.dumps(result.details, ensure_ascii=False, default=str)}\n"
        "\nPropose la remédiation JSON."
    )


# ── Fallback rule-based ────────────────────────────────────────────────────────

def _fallback_remediation(result: DQResult) -> RemediationResult:
    """
    Remédiation déterministe lorsque l'API est indisponible.
    Heuristiques simples par dimension et type de champ.
    """
    dim = result.dimension.lower()
    field = result.field_name.lower()

    # Champs nuls obligatoires → human_review systématique
    if dim == "completeness":
        return RemediationResult(
            suggested_value=None,
            confidence=0.45,
            action="human_review",
            explanation=(
                f"Le champ obligatoire '{result.field_name}' est absent. "
                "Vérification manuelle dans le système source requise."
            ),
            impact_si_non_corrige=(
                "Enregistrement bloqué en étape Staged — non éligible à Mastered."
            ),
            generated_by="rule_based",
        )

    # Montant réclamé supérieur à l'assuré → reject (fraude potentielle)
    if dim == "businessrules" and "montant" in field:
        return RemediationResult(
            suggested_value=None,
            confidence=0.90,
            action="reject",
            explanation=(
                "Le montant réclamé dépasse le montant assuré. "
                "Dossier transmis à l'unité anti-fraude."
            ),
            impact_si_non_corrige=(
                "Paiement indu potentiel — risque financier élevé."
            ),
            generated_by="rule_based",
        )

    # Dérive modèle → human_review
    if dim in ("modeldrift", "anomalydetection"):
        return RemediationResult(
            suggested_value=None,
            confidence=0.60,
            action="human_review",
            explanation=(
                "Dérive détectée sur le modèle. "
                "Réentraînement recommandé avant nouvelle mise en production."
            ),
            impact_si_non_corrige=(
                "Prédictions biaisées — risque réglementaire AI Act / Loi 25."
            ),
            generated_by="rule_based",
        )

    # Date hors couverture → human_review
    if dim == "businessrules" and "date" in field:
        return RemediationResult(
            suggested_value=None,
            confidence=0.55,
            action="human_review",
            explanation=(
                "La date du sinistre est hors de la période de couverture. "
                "Vérifier avec le courtier les dates exactes de la police."
            ),
            impact_si_non_corrige=(
                "Sinistre potentiellement non couvert — litiges client."
            ),
            generated_by="rule_based",
        )

    # Défaut générique
    return RemediationResult(
        suggested_value=None,
        confidence=0.40,
        action="human_review",
        explanation=(
            f"Anomalie détectée sur '{result.field_name}' (dimension {result.dimension}). "
            "Révision manuelle nécessaire."
        ),
        impact_si_non_corrige="Qualité des données dégradée — impact sur la fiabilité des rapports.",
        generated_by="rule_based",
    )


# ── Classe principale ─────────────────────────────────────────────────────────

class LLMRemediator:
    """
    Remédiation intelligente des données IV via Claude.

    Exemple d'utilisation
    ---------------------
    >>> remediator = LLMRemediator()
    >>> enriched = remediator.remediate(iv_results)   # list[DQResult]
    >>> for r in enriched:
    ...     print(r.record_id, r.remediation.action, r.remediation.confidence)
    """

    def __init__(
        self,
        model: str = _DEFAULT_MODEL,
        max_calls: int = _MAX_CALLS,
        api_key: str | None = None,
    ) -> None:
        self.model = model
        self.max_calls = max_calls
        self._client = None
        self._api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")

    def _get_client(self):
        """Lazy init du client Anthropic (évite l'import au niveau module)."""
        if self._client is None:
            try:
                import anthropic  # noqa: PLC0415
                self._client = anthropic.Anthropic(api_key=self._api_key)
            except ImportError as exc:
                raise ImportError(
                    "anthropic requis : pip install anthropic"
                ) from exc
        return self._client

    def _call_llm(self, result: DQResult) -> RemediationResult:
        """
        Appelle Claude et parse la réponse JSON.
        Lève ValueError si le JSON est invalide.
        """
        client = self._get_client()
        response = client.messages.create(
            model=self.model,
            max_tokens=_MAX_TOKENS,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": _build_user_prompt(result)}],
        )
        raw = response.content[0].text.strip()

        # Nettoyage défensif : retirer éventuels blocs markdown
        if raw.startswith("```"):
            raw = "\n".join(
                line for line in raw.splitlines()
                if not line.startswith("```")
            ).strip()

        data: dict[str, Any] = json.loads(raw)

        # Validation minimale
        required_keys = {"confidence", "action", "explanation", "impact_si_non_corrige"}
        missing = required_keys - data.keys()
        if missing:
            raise ValueError(f"Clés manquantes dans la réponse LLM : {missing}")

        if data["action"] not in ("auto_fix", "human_review", "reject"):
            raise ValueError(f"Action invalide : {data['action']!r}")

        conf = float(data["confidence"])
        if not (0.0 <= conf <= 1.0):
            raise ValueError(f"Confidence hors bornes : {conf}")

        return RemediationResult(
            suggested_value=data.get("suggested_value"),
            confidence=conf,
            action=data["action"],
            explanation=data["explanation"],
            impact_si_non_corrige=data["impact_si_non_corrige"],
            generated_by="LLM",
            generated_at=datetime.utcnow(),
        )

    def remediate_one(self, result: DQResult) -> DQResult:
        """
        Enrichit un seul DQResult IV avec une suggestion de remédiation.
        Utilise le fallback si l'API échoue.
        """
        if result.status != DQStatus.INVALID:
            return result  # rien à faire

        try:
            rem = self._call_llm(result)
            logger.debug(
                "LLM remediation OK: %s %s action=%s conf=%.2f",
                result.record_id, result.field_name,
                rem.action, rem.confidence,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "LLM remediation failed (%s) — fallback rule-based for %s/%s",
                exc, result.record_id, result.field_name,
            )
            rem = _fallback_remediation(result)

        # Pydantic v2 : on crée une copie avec le champ rempli
        return result.model_copy(update={"remediation": rem})

    def remediate(
        self,
        results: list[DQResult],
        limit: int | None = None,
    ) -> list[DQResult]:
        """
        Enrichit une liste de DQResult.

        Seuls les IV sont traités. Le nombre d'appels LLM est plafonné à
        min(limit, self.max_calls) pour maîtriser les coûts.

        Parameters
        ----------
        results : liste complète (V, S, IV mélangés)
        limit   : surcharge ponctuelle de max_calls (None = utilise self.max_calls)

        Returns
        -------
        Nouvelle liste où les IV ont .remediation rempli (LLM ou rule_based).
        """
        cap = min(limit, self.max_calls) if limit is not None else self.max_calls
        llm_calls = 0
        enriched: list[DQResult] = []

        for r in results:
            if r.status != DQStatus.INVALID:
                enriched.append(r)
                continue

            if llm_calls < cap:
                enriched.append(self.remediate_one(r))
                llm_calls += 1
            else:
                # Budget épuisé → fallback direct sans appel LLM
                rem = _fallback_remediation(r)
                enriched.append(r.model_copy(update={"remediation": rem}))

        logger.info(
            "Remédiation : %d IV traités (%d appels LLM, %d fallback rule-based)",
            sum(1 for r in results if r.status == DQStatus.INVALID),
            llm_calls,
            sum(1 for r in results if r.status == DQStatus.INVALID) - llm_calls,
        )
        return enriched

    @property
    def model_info(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "max_calls": self.max_calls,
            "api_key_set": bool(self._api_key),
        }
