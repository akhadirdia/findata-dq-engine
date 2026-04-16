"""Tests Dimension 9 — BusinessRules. Règle des 3 cas : V, S, IV."""

from datetime import date, timedelta

import pytest
from findata_dq.dimensions.business_rules import BusinessRules
from findata_dq.models.dq_result import DQStatus, ImpactLevel

dim = BusinessRules()
TODAY = date.today()


# ── R1 : date_sinistre dans la période de couverture ─────────────────────────

def test_R1_V_date_dans_couverture(claim_valid):
    results = dim.validate(claim_valid, {"reference_dt": TODAY})
    r1 = [r for r in results if "R1" in r.details.get("rule", "")]
    assert r1[0].status == DQStatus.VALID


def test_R1_IV_date_avant_effet():
    effet = TODAY - timedelta(days=200)
    expiration = TODAY + timedelta(days=165)
    record = {
        "record_id": "SIN-001", "dataset": "claims",
        "date_sinistre": (effet - timedelta(days=10)).isoformat(),  # avant effet
        "date_effet_police": effet.isoformat(),
        "date_expiration_police": expiration.isoformat(),
        "statut_sinistre": "ouvert",
        "montant_reclame": 5000.0,
        "montant_assure_police": 50000.0,
    }
    results = dim.validate(record, {"reference_dt": TODAY})
    r1 = [r for r in results if "R1" in r.details.get("rule", "")]
    assert r1[0].status == DQStatus.INVALID
    assert r1[0].impact == ImpactLevel.HIGH


def test_R1_IV_date_apres_expiration():
    effet = TODAY - timedelta(days=400)
    expiration = TODAY - timedelta(days=35)  # expirée
    record = {
        "record_id": "SIN-002", "dataset": "claims",
        "date_sinistre": (TODAY - timedelta(days=10)).isoformat(),  # après expiration
        "date_effet_police": effet.isoformat(),
        "date_expiration_police": expiration.isoformat(),
        "statut_sinistre": "ouvert",
        "montant_reclame": 5000.0,
        "montant_assure_police": 50000.0,
    }
    results = dim.validate(record, {"reference_dt": TODAY})
    r1 = [r for r in results if "R1" in r.details.get("rule", "")]
    assert r1[0].status == DQStatus.INVALID


# ── R2 : montant_reclame <= montant_assure ────────────────────────────────────

def test_R2_V_montant_dans_limites(claim_valid):
    results = dim.validate(claim_valid, {"reference_dt": TODAY})
    r2 = [r for r in results if "R2" in r.details.get("rule", "")]
    assert r2[0].status == DQStatus.VALID


def test_R2_IV_montant_depasse_assure():
    record = {
        "record_id": "SIN-003", "dataset": "claims",
        "date_sinistre": (TODAY - timedelta(days=10)).isoformat(),
        "date_effet_police": (TODAY - timedelta(days=200)).isoformat(),
        "date_expiration_police": (TODAY + timedelta(days=165)).isoformat(),
        "statut_sinistre": "ouvert",
        "montant_reclame": 80_000.0,   # > montant_assure
        "montant_assure_police": 50_000.0,
    }
    results = dim.validate(record, {"reference_dt": TODAY})
    r2 = [r for r in results if "R2" in r.details.get("rule", "")]
    assert r2[0].status == DQStatus.INVALID
    assert r2[0].details["depassement"] == pytest.approx(30_000.0)


# ── R3 : sinistre ouvert sur police expirée ───────────────────────────────────

def test_R3_IV_sinistre_ouvert_police_expiree():
    record = {
        "record_id": "SIN-004", "dataset": "claims",
        "date_sinistre": (TODAY - timedelta(days=400)).isoformat(),
        "date_effet_police": (TODAY - timedelta(days=730)).isoformat(),
        "date_expiration_police": (TODAY - timedelta(days=365)).isoformat(),  # expirée
        "statut_sinistre": "ouvert",
        "montant_reclame": 5000.0,
        "montant_assure_police": 50000.0,
    }
    results = dim.validate(record, {"reference_dt": TODAY})
    r3 = [r for r in results if "R3" in r.details.get("rule", "")]
    assert r3[0].status == DQStatus.INVALID


def test_R3_V_sinistre_ouvert_police_active(claim_valid):
    """sinistre ouvert sur police encore active → V."""
    record = dict(claim_valid)
    record["statut_sinistre"] = "ouvert"
    record["date_expiration_police"] = (TODAY + timedelta(days=100)).isoformat()
    results = dim.validate(record, {"reference_dt": TODAY})
    r3 = [r for r in results if "R3" in r.details.get("rule", "")]
    assert r3[0].status == DQStatus.VALID


# ── R4 : delete sans session_id ───────────────────────────────────────────────

def test_R4_IV_delete_sans_session():
    record = {
        "record_id": "LOG-001", "dataset": "logs",
        "action_type": "delete", "session_id": None,
        "status_code": 200,
    }
    results = dim.validate(record)
    r4 = [r for r in results if "R4" in r.details.get("rule", "")]
    assert r4[0].status == DQStatus.INVALID
    assert r4[0].impact == ImpactLevel.HIGH


def test_R4_V_delete_avec_session():
    record = {
        "record_id": "LOG-002", "dataset": "logs",
        "action_type": "delete", "session_id": "SES-12345",
        "status_code": 200,
    }
    results = dim.validate(record)
    r4 = [r for r in results if "R4" in r.details.get("rule", "")]
    assert r4[0].status == DQStatus.VALID


# ── R6 : modèle en production avec drift IV ───────────────────────────────────

def test_R6_IV_modele_prod_drift_iv():
    record = {
        "record_id": "MDL-010", "dataset": "model_metadata",
        "statut_production": "production",
        "drift_status": "IV",
        "drift_score": 0.32,
    }
    results = dim.validate(record)
    r6 = [r for r in results if "R6" in r.details.get("rule", "")]
    assert r6[0].status == DQStatus.INVALID


def test_R6_V_modele_prod_drift_stable(model_valid):
    results = dim.validate(model_valid)
    r6 = [r for r in results if "R6" in r.details.get("rule", "")]
    assert r6[0].status == DQStatus.VALID
