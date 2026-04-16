"""Tests Dimension 1 — Completeness. Règle des 3 cas : V, S, IV."""

import pytest
from findata_dq.dimensions.completeness import Completeness
from findata_dq.models.dq_result import DQStatus, ImpactLevel

dim = Completeness()
CONFIG = {
    "mandatory_fields": ["num_police", "prime_annuelle", "date_effet"],
    "optional_fields": ["franchise"],
}


def test_completeness_V_tous_champs_presents(policy_valid):
    results = dim.validate(policy_valid, CONFIG)
    iv = [r for r in results if r.status == DQStatus.INVALID]
    s  = [r for r in results if r.status == DQStatus.SUSPECT]
    assert len(iv) == 0
    assert len(s) == 0   # ni IV ni S — tout présent


def test_completeness_S_champ_optionnel_absent(policy_valid):
    record = dict(policy_valid)
    record["franchise"] = None
    results = dim.validate(record, CONFIG)
    s = [r for r in results if r.status == DQStatus.SUSPECT]
    assert len(s) == 1
    assert s[0].field_name == "franchise"
    assert s[0].impact == ImpactLevel.LOW


def test_completeness_IV_champ_obligatoire_null(policy_valid):
    record = dict(policy_valid)
    record["num_police"] = None
    results = dim.validate(record, CONFIG)
    iv = [r for r in results if r.status == DQStatus.INVALID]
    assert len(iv) == 1
    assert iv[0].field_name == "num_police"
    assert iv[0].impact == ImpactLevel.HIGH


def test_completeness_IV_champ_obligatoire_vide(policy_valid):
    record = dict(policy_valid)
    record["prime_annuelle"] = ""
    results = dim.validate(record, CONFIG)
    iv = [r for r in results if r.status == DQStatus.INVALID]
    assert any(r.field_name == "prime_annuelle" for r in iv)


def test_completeness_IV_placeholder(policy_valid):
    record = dict(policy_valid)
    record["date_effet"] = "N/A"
    results = dim.validate(record, CONFIG)
    iv = [r for r in results if r.status == DQStatus.INVALID]
    assert any(r.field_name == "date_effet" for r in iv)


def test_completeness_default_dataset(policy_valid):
    """Sans config explicite, utilise les champs par défaut du dataset 'policies'."""
    results = dim.validate(policy_valid)
    assert len(results) > 0
    assert all(r.dimension == "Completeness" for r in results)
