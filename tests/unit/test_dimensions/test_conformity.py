"""Tests Dimension 5 — Conformity. Règle des 3 cas : V, S (N/A), IV."""

from findata_dq.dimensions.conformity import Conformity
from findata_dq.models.dq_result import DQStatus, ImpactLevel

dim = Conformity()


def test_conformity_V_num_police_valide(policy_valid):
    results = dim.validate(policy_valid, {"fields": ["num_police"]})
    assert len(results) == 1
    assert results[0].status == DQStatus.VALID


def test_conformity_V_ip_valide(log_valid):
    results = dim.validate(log_valid, {"fields": ["ip_address"]})
    assert len(results) == 1
    assert results[0].status == DQStatus.VALID


def test_conformity_V_model_version(model_valid):
    results = dim.validate(model_valid, {"fields": ["model_version"]})
    assert len(results) == 1
    assert results[0].status == DQStatus.VALID


def test_conformity_IV_num_police_format_invalide(policy_valid):
    record = dict(policy_valid)
    record["num_police"] = "123456"   # pas de prefix lettre
    results = dim.validate(record, {"fields": ["num_police"]})
    assert results[0].status == DQStatus.INVALID
    assert results[0].impact == ImpactLevel.HIGH


def test_conformity_IV_ip_invalide(log_valid):
    record = dict(log_valid)
    record["ip_address"] = "999.999.999.999"
    results = dim.validate(record, {"fields": ["ip_address"]})
    assert results[0].status == DQStatus.INVALID


def test_conformity_IV_type_couverture_invalide(policy_valid):
    record = dict(policy_valid)
    record["type_couverture"] = "moto"   # valeur hors énumération
    results = dim.validate(record, {"fields": ["type_couverture"]})
    assert results[0].status == DQStatus.INVALID


def test_conformity_champ_absent_ignore(policy_valid):
    """Un champ absent (None) est ignoré — Completeness s'en charge."""
    record = dict(policy_valid)
    record["num_police"] = None
    results = dim.validate(record, {"fields": ["num_police"]})
    assert len(results) == 0


def test_conformity_custom_pattern(policy_valid):
    record = dict(policy_valid)
    record["code_contrat"] = "CTR-2024"
    results = dim.validate(
        record,
        {
            "fields": ["code_contrat"],
            "custom_patterns": {"code_contrat": r"^CTR-\d{4}$"},
        },
    )
    assert results[0].status == DQStatus.VALID
