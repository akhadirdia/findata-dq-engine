"""Tests Dimension 12 — Privacy. Règle des 3 cas : V, S, IV."""

from datetime import date, timedelta

import pytest
from findata_dq.dimensions.privacy import Privacy
from findata_dq.models.dq_result import DQStatus, ImpactLevel

dim = Privacy()
TODAY = date.today()


# ── 1. Détection PII ─────────────────────────────────────────────────────────

def test_privacy_IV_email_en_clair():
    record = {
        "record_id": "CLI-001", "dataset": "clients",
        "cause_sinistre": "Accident — contact : jean.dupont@gmail.com",
        "date_creation": (TODAY - timedelta(days=10)).isoformat(),
    }
    config = {
        "pipeline_env": "development",
        "pii_fields": ["cause_sinistre"],
    }
    results = dim.validate(record, config)
    iv = [r for r in results if r.status == DQStatus.INVALID and r.details.get("check") == "pii_detection"]
    assert len(iv) >= 1
    assert iv[0].impact == ImpactLevel.HIGH


def test_privacy_IV_nas_canadien():
    record = {
        "record_id": "CLI-002", "dataset": "clients",
        "cause_sinistre": "NAS assuré : 123-456-789",
        "date_creation": (TODAY - timedelta(days=5)).isoformat(),
    }
    config = {"pipeline_env": "staging", "pii_fields": ["cause_sinistre"]}
    results = dim.validate(record, config)
    iv = [r for r in results if r.details.get("check") == "pii_detection"]
    assert iv[0].details["pii_type"] == "nas_canadien"


def test_privacy_V_aucune_pii(claim_valid):
    """Enregistrement sans PII en dev → pas d'IV pii_detection."""
    config = {
        "pipeline_env": "development",
        "pii_fields": ["cause_sinistre"],
    }
    results = dim.validate(claim_valid, config)
    pii_iv = [r for r in results if r.details.get("check") == "pii_detection"]
    assert len(pii_iv) == 0


def test_privacy_aucune_detection_en_production(claim_valid):
    """En production, la détection PII ne s'exécute pas (masquage fait en amont)."""
    config = {"pipeline_env": "production", "pii_fields": ["cause_sinistre"]}
    results = dim.validate(claim_valid, config)
    pii = [r for r in results if r.details.get("check") == "pii_detection"]
    assert len(pii) == 0


# ── 2. Consentement ───────────────────────────────────────────────────────────

def test_privacy_IV_consentement_retire(claim_valid):
    config = {
        "refused_client_ids": {"CLI-0000001"},
        "pipeline_env": "production",
    }
    results = dim.validate(claim_valid, config)
    consent_iv = [r for r in results if r.details.get("check") == "consent"]
    assert len(consent_iv) == 1
    assert consent_iv[0].status == DQStatus.INVALID


def test_privacy_V_consentement_actif(claim_valid):
    config = {
        "refused_client_ids": {"CLI-9999999"},  # autre client
        "pipeline_env": "production",
    }
    results = dim.validate(claim_valid, config)
    consent_iv = [r for r in results if r.details.get("check") == "consent"]
    assert len(consent_iv) == 0


# ── 3. Rétention ─────────────────────────────────────────────────────────────

def test_privacy_V_retention_dans_limites(claim_valid):
    config = {"pipeline_env": "production", "retention_days": 3650}
    results = dim.validate(claim_valid, config)
    ret = [r for r in results if r.details.get("check") == "retention"]
    assert ret[0].status == DQStatus.VALID


def test_privacy_IV_retention_depassee():
    record = {
        "record_id": "CLI-003", "dataset": "logs",
        "id_client": "CLI-0000003",
        "date_creation": (TODAY - timedelta(days=800)).isoformat(),  # > 730 jours
    }
    config = {"pipeline_env": "production", "retention_days": 730}
    results = dim.validate(record, config)
    ret = [r for r in results if r.details.get("check") == "retention"]
    assert ret[0].status == DQStatus.INVALID
    assert ret[0].details["age_jours"] > 730


# ── 4. Pseudonymisation ───────────────────────────────────────────────────────

def test_privacy_S_id_non_hache(claim_valid):
    """id_client lisible (pas un hash) → S."""
    config = {
        "pipeline_env": "production",
        "check_pseudonymization": True,
    }
    results = dim.validate(claim_valid, config)
    pseudo = [r for r in results if r.details.get("check") == "pseudonymization"]
    assert pseudo[0].status == DQStatus.SUSPECT
    assert pseudo[0].impact == ImpactLevel.MEDIUM


def test_privacy_pas_de_suspect_si_hache():
    """id_client sous forme de hash SHA-256 → pas de S pseudonymisation."""
    record = {
        "record_id": "CLI-004", "dataset": "clients",
        "id_client": "a" * 64,  # SHA-256 (64 chars hex)
        "date_creation": (TODAY - timedelta(days=30)).isoformat(),
    }
    config = {"pipeline_env": "production", "check_pseudonymization": True}
    results = dim.validate(record, config)
    pseudo = [r for r in results if r.details.get("check") == "pseudonymization"]
    assert len(pseudo) == 0
