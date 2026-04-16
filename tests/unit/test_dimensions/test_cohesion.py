"""Tests Dimension 8 — Cohesion. Règle des 3 cas : V, S (N/A), IV."""

from findata_dq.dimensions.cohesion import Cohesion
from findata_dq.models.dq_result import DQStatus, ImpactLevel

dim = Cohesion()

VALID_POLICIES = {"AU-123456", "HA-987654", "VIE-111111"}
VALID_CLIENTS = {"CLI-0000001", "CLI-0000002", "CLI-0000003"}


def test_cohesion_V_fk_presente(claim_valid):
    """FK num_police présente dans la table de référence → V."""
    config = {
        "fk_checks": [
            ("num_police", VALID_POLICIES),
            ("id_client", VALID_CLIENTS),
        ]
    }
    results = dim.validate(claim_valid, config)
    v = [r for r in results if r.status == DQStatus.VALID]
    assert len(v) == 2


def test_cohesion_IV_fk_absente(claim_valid):
    """FK num_police introuvable dans la table de référence → IV."""
    record = dict(claim_valid)
    record["num_police"] = "AU-999999"  # n'existe pas
    config = {
        "fk_checks": [
            ("num_police", VALID_POLICIES),
        ]
    }
    results = dim.validate(record, config)
    assert results[0].status == DQStatus.INVALID
    assert results[0].impact == ImpactLevel.HIGH


def test_cohesion_IV_client_inconnu(claim_valid):
    """FK id_client introuvable → IV."""
    record = dict(claim_valid)
    record["id_client"] = "CLI-9999999"
    config = {
        "fk_checks": [
            ("id_client", VALID_CLIENTS),
        ]
    }
    results = dim.validate(record, config)
    assert results[0].status == DQStatus.INVALID


def test_cohesion_vide_sans_reference():
    """Sans référence fournie → résultat vide (ne peut pas échouer silencieusement)."""
    record = {"record_id": "T-001", "dataset": "claims", "num_police": "AU-123456"}
    results = dim.validate(record, {})
    assert results == []


def test_cohesion_champ_none_ignore(claim_valid):
    """FK None → ignorée (Completeness s'en charge)."""
    record = dict(claim_valid)
    record["num_police"] = None
    config = {
        "fk_checks": [
            ("num_police", VALID_POLICIES),
        ]
    }
    results = dim.validate(record, config)
    assert results == []
