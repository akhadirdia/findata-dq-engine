"""
Tests Étape 7 — LLMRemediator.

Stratégie
---------
- Aucun appel réel à l'API (coût + flakiness).
- On mocke `anthropic.Anthropic` pour tester le parsing et la logique.
- Les tests de fallback rule-based ne nécessitent pas de mock.

Blocs
-----
  Bloc 1 : Tests du fallback rule-based (sans API)
  Bloc 2 : Tests du parsing LLM (réponse mockée)
  Bloc 3 : Tests de la logique de budget et de plafonnement
  Bloc 4 : Tests de robustesse (JSON mal formé, API down)
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from findata_dq.ai.remediation import LLMRemediator, _build_user_prompt, _fallback_remediation
from findata_dq.models.dq_result import DQResult, DQStatus, ImpactLevel

# ── Factory de DQResult IV ────────────────────────────────────────────────────

def _make_iv(
    record_id: str = "SIN-001",
    field_name: str = "montant_reclame",
    dimension: str = "BusinessRules",
    dataset: str = "claims",
    rule: str = "R2: montant_reclame <= montant_assure",
    details: dict | None = None,
    field_value: str | None = "80000.0",
) -> DQResult:
    uid = uuid4().hex[:6]
    return DQResult(
        datum_id=f"{record_id}_{field_name}_{dimension}_{uid}",
        record_id=record_id,
        dataset=dataset,
        dimension=dimension,
        field_name=field_name,
        field_value=field_value,
        status=DQStatus.INVALID,
        impact=ImpactLevel.HIGH,
        score=0.0,
        rule_applied=rule,
        details=details or {"depassement": 30000.0},
    )


def _make_valid(record_id: str = "POL-001") -> DQResult:
    uid = uuid4().hex[:6]
    return DQResult(
        datum_id=f"{record_id}_date_sinistre_BusinessRules_{uid}",
        record_id=record_id,
        dataset="policies",
        dimension="BusinessRules",
        field_name="date_sinistre",
        status=DQStatus.VALID,
        impact=ImpactLevel.LOW,
        score=1.0,
        rule_applied="R1: date dans couverture",
    )


# ── Helpers mock ──────────────────────────────────────────────────────────────

def _mock_llm_response(payload: dict) -> MagicMock:
    """Simule anthropic.Anthropic().messages.create() retournant payload."""
    text_block = SimpleNamespace(text=json.dumps(payload))
    response = SimpleNamespace(content=[text_block])
    client = MagicMock()
    client.messages.create.return_value = response
    return client


# ── Bloc 1 : Fallback rule-based ──────────────────────────────────────────────

class TestFallbackRuleBased:

    def test_completeness_produit_human_review(self):
        r = _make_iv(dimension="Completeness", field_name="date_effet_police")
        rem = _fallback_remediation(r)
        assert rem.action == "human_review"
        assert rem.generated_by == "rule_based"
        assert 0.0 <= rem.confidence <= 1.0

    def test_business_rules_montant_produit_reject(self):
        r = _make_iv(dimension="BusinessRules", field_name="montant_reclame")
        rem = _fallback_remediation(r)
        assert rem.action == "reject"
        assert rem.confidence >= 0.85

    def test_model_drift_produit_human_review(self):
        r = _make_iv(dimension="ModelDrift", field_name="drift_score")
        rem = _fallback_remediation(r)
        assert rem.action == "human_review"

    def test_anomaly_detection_produit_human_review(self):
        r = _make_iv(dimension="AnomalyDetection", field_name="multivariate_profile")
        rem = _fallback_remediation(r)
        assert rem.action == "human_review"

    def test_business_rules_date_produit_human_review(self):
        r = _make_iv(dimension="BusinessRules", field_name="date_sinistre")
        rem = _fallback_remediation(r)
        assert rem.action == "human_review"

    def test_fallback_generique_confidence_basse(self):
        r = _make_iv(dimension="Precision", field_name="prime_annuelle")
        rem = _fallback_remediation(r)
        assert rem.confidence < 0.60
        assert rem.action == "human_review"

    def test_fallback_impact_non_vide(self):
        r = _make_iv(dimension="Completeness", field_name="id_client")
        rem = _fallback_remediation(r)
        assert len(rem.impact_si_non_corrige) > 0

    def test_fallback_explanation_non_vide(self):
        r = _make_iv(dimension="Conformity", field_name="ip_address")
        rem = _fallback_remediation(r)
        assert len(rem.explanation) > 0


# ── Bloc 2 : Parsing LLM (mock) ───────────────────────────────────────────────

class TestLLMParsing:

    def _remediator_with_mock(self, payload: dict) -> LLMRemediator:
        rem = LLMRemediator(api_key="sk-test")
        rem._client = _mock_llm_response(payload)
        return rem

    def test_auto_fix_confidence_haute(self):
        payload = {
            "suggested_value": "2026-01-15",
            "confidence": 0.92,
            "action": "auto_fix",
            "explanation": "Date corrigée à partir du dossier sinistre.",
            "impact_si_non_corrige": "Sinistre rejeté sans correction.",
        }
        remediator = self._remediator_with_mock(payload)
        r = _make_iv(field_name="date_sinistre")
        enriched = remediator.remediate_one(r)
        assert enriched.remediation is not None
        assert enriched.remediation.action == "auto_fix"
        assert enriched.remediation.confidence == pytest.approx(0.92)
        assert enriched.remediation.suggested_value == "2026-01-15"
        assert enriched.remediation.generated_by == "LLM"

    def test_human_review_confidence_moyenne(self):
        payload = {
            "suggested_value": None,
            "confidence": 0.65,
            "action": "human_review",
            "explanation": "Montant ambigu — vérification nécessaire.",
            "impact_si_non_corrige": "Risque de paiement indu.",
        }
        remediator = self._remediator_with_mock(payload)
        r = _make_iv()
        enriched = remediator.remediate_one(r)
        assert enriched.remediation.action == "human_review"
        assert enriched.remediation.suggested_value is None

    def test_reject_fraude_avérée(self):
        payload = {
            "suggested_value": None,
            "confidence": 0.95,
            "action": "reject",
            "explanation": "Fraude avérée — montant 16× supérieur à l'assuré.",
            "impact_si_non_corrige": "Paiement frauduleux de 30 000 CAD.",
        }
        remediator = self._remediator_with_mock(payload)
        r = _make_iv()
        enriched = remediator.remediate_one(r)
        assert enriched.remediation.action == "reject"
        assert enriched.remediation.confidence >= 0.90

    def test_valid_non_modifie(self):
        """Les résultats V ne sont pas touchés par remediate_one."""
        remediator = LLMRemediator(api_key="sk-test")
        r = _make_valid()
        enriched = remediator.remediate_one(r)
        assert enriched.remediation is None

    def test_json_dans_markdown_nettoyé(self):
        """Réponse avec blocs ``` — doit être parsée quand même."""
        payload = {
            "suggested_value": None,
            "confidence": 0.70,
            "action": "human_review",
            "explanation": "Test.",
            "impact_si_non_corrige": "Impact.",
        }
        raw = "```json\n" + json.dumps(payload) + "\n```"
        text_block = SimpleNamespace(text=raw)
        response = SimpleNamespace(content=[text_block])
        client = MagicMock()
        client.messages.create.return_value = response

        remediator = LLMRemediator(api_key="sk-test")
        remediator._client = client
        enriched = remediator.remediate_one(_make_iv())
        assert enriched.remediation.action == "human_review"


# ── Bloc 3 : Budget et plafonnement ───────────────────────────────────────────

class TestBudgetControl:

    def test_max_calls_respecte(self):
        """Avec max_calls=2 et 4 IV, seulement 2 appels LLM."""
        payload = {
            "suggested_value": None,
            "confidence": 0.70,
            "action": "human_review",
            "explanation": "Test.",
            "impact_si_non_corrige": "Impact.",
        }
        mock_client = _mock_llm_response(payload)
        remediator = LLMRemediator(api_key="sk-test", max_calls=2)
        remediator._client = mock_client

        iv_results = [_make_iv(record_id=f"SIN-{i:03d}") for i in range(4)]
        enriched = remediator.remediate(iv_results)

        assert len(enriched) == 4
        assert mock_client.messages.create.call_count == 2

        # Les 2 premiers ont generated_by=LLM, les 2 derniers rule_based
        assert enriched[0].remediation.generated_by == "LLM"
        assert enriched[1].remediation.generated_by == "LLM"
        assert enriched[2].remediation.generated_by == "rule_based"
        assert enriched[3].remediation.generated_by == "rule_based"

    def test_limit_surcharge_max_calls(self):
        """Paramètre limit écrase temporairement max_calls."""
        payload = {
            "suggested_value": None,
            "confidence": 0.60,
            "action": "human_review",
            "explanation": "X",
            "impact_si_non_corrige": "Y",
        }
        mock_client = _mock_llm_response(payload)
        remediator = LLMRemediator(api_key="sk-test", max_calls=10)
        remediator._client = mock_client

        iv_results = [_make_iv(record_id=f"SIN-{i:03d}") for i in range(5)]
        remediator.remediate(iv_results, limit=1)

        assert mock_client.messages.create.call_count == 1

    def test_v_et_s_non_touches(self):
        """Les V et S sont passés tels quels."""
        remediator = LLMRemediator(api_key="sk-test", max_calls=5)
        uid1, uid2 = uuid4().hex[:6], uuid4().hex[:6]
        results = [
            DQResult(
                datum_id=f"POL-001_champ_Dim_{uid1}", record_id="POL-001",
                dataset="policies", dimension="Completeness", field_name="champ",
                status=DQStatus.VALID, impact=ImpactLevel.LOW, score=1.0,
                rule_applied="R_test",
            ),
            DQResult(
                datum_id=f"POL-002_champ_Dim_{uid2}", record_id="POL-002",
                dataset="policies", dimension="Completeness", field_name="champ",
                status=DQStatus.SUSPECT, impact=ImpactLevel.MEDIUM, score=0.5,
                rule_applied="R_test",
            ),
        ]
        enriched = remediator.remediate(results)
        assert all(r.remediation is None for r in enriched)

    def test_liste_vide(self):
        remediator = LLMRemediator(api_key="sk-test")
        assert remediator.remediate([]) == []


# ── Bloc 4 : Robustesse ───────────────────────────────────────────────────────

class TestRobustness:

    def test_api_down_fallback_automatique(self):
        """Si l'API lève une exception, fallback rule-based silencieux."""
        client = MagicMock()
        client.messages.create.side_effect = ConnectionError("API unreachable")

        remediator = LLMRemediator(api_key="sk-test")
        remediator._client = client
        enriched = remediator.remediate_one(_make_iv())

        assert enriched.remediation is not None
        assert enriched.remediation.generated_by == "rule_based"

    def test_json_invalide_fallback(self):
        """Réponse non-JSON → fallback."""
        text_block = SimpleNamespace(text="Désolé, je ne peux pas répondre.")
        response = SimpleNamespace(content=[text_block])
        client = MagicMock()
        client.messages.create.return_value = response

        remediator = LLMRemediator(api_key="sk-test")
        remediator._client = client
        enriched = remediator.remediate_one(_make_iv())

        assert enriched.remediation.generated_by == "rule_based"

    def test_action_invalide_fallback(self):
        """Action inconnue dans le JSON → fallback."""
        payload = {
            "suggested_value": None,
            "confidence": 0.80,
            "action": "magic_fix",          # invalide
            "explanation": "Test.",
            "impact_si_non_corrige": "Impact.",
        }
        text_block = SimpleNamespace(text=json.dumps(payload))
        response = SimpleNamespace(content=[text_block])
        client = MagicMock()
        client.messages.create.return_value = response

        remediator = LLMRemediator(api_key="sk-test")
        remediator._client = client
        enriched = remediator.remediate_one(_make_iv())

        assert enriched.remediation.generated_by == "rule_based"

    def test_build_user_prompt_contient_champs(self):
        r = _make_iv(record_id="SIN-TEST", field_name="montant_reclame")
        prompt = _build_user_prompt(r)
        assert "SIN-TEST" in prompt
        assert "montant_reclame" in prompt
        assert "BusinessRules" in prompt

    def test_model_info(self):
        rem = LLMRemediator(model="claude-haiku-4-5-20251001", max_calls=3, api_key="sk-x")
        info = rem.model_info
        assert info["model"] == "claude-haiku-4-5-20251001"
        assert info["max_calls"] == 3
        assert info["api_key_set"] is True
