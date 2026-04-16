"""Tests Dimension 11 — ModelDrift. Règle des 3 cas : V, S, IV."""

import pytest

from findata_dq.dimensions.model_drift import ModelDrift, _compute_kl, _compute_psi
from findata_dq.models.dq_result import DQStatus

dim = ModelDrift()

BASE = {"record_id": "MDL-001", "dataset": "model_metadata"}


# ── PSI pré-calculé ───────────────────────────────────────────────────────────

def test_drift_V_psi_stable(model_valid):
    """drift_score = 0.05 → V."""
    results = dim.validate(model_valid)
    psi = [r for r in results if r.field_name == "drift_score"]
    assert psi[0].status == DQStatus.VALID


def test_drift_S_psi_modere():
    record = {**BASE, "drift_score": 0.18}
    results = dim.validate(record)
    psi = [r for r in results if r.field_name == "drift_score"]
    assert psi[0].status == DQStatus.SUSPECT


def test_drift_IV_psi_eleve():
    record = {**BASE, "drift_score": 0.35}
    results = dim.validate(record)
    psi = [r for r in results if r.field_name == "drift_score"]
    assert psi[0].status == DQStatus.INVALID
    assert "retraining" in psi[0].rule_applied.lower()


# ── Performance Drift ─────────────────────────────────────────────────────────

def test_drift_V_perf_stable():
    record = {**BASE, "accuracy": 0.915}
    config = {"accuracy_baseline": 0.920}
    results = dim.validate(record, config)
    perf = [r for r in results if r.field_name == "accuracy"]
    assert perf[0].status == DQStatus.VALID  # delta = -0.005 < 0.02


def test_drift_S_perf_degradation_moderee():
    record = {**BASE, "accuracy": 0.888}
    config = {"accuracy_baseline": 0.920}
    results = dim.validate(record, config)
    perf = [r for r in results if r.field_name == "accuracy"]
    assert perf[0].status == DQStatus.SUSPECT  # delta = -0.032


def test_drift_IV_perf_degradation_forte():
    record = {**BASE, "accuracy": 0.850}
    config = {"accuracy_baseline": 0.920}
    results = dim.validate(record, config)
    perf = [r for r in results if r.field_name == "accuracy"]
    assert perf[0].status == DQStatus.INVALID  # delta = -0.070 >= 0.05


# ── PSI calculé depuis distributions ─────────────────────────────────────────

def test_drift_V_psi_distributions_stables():
    """Distributions quasi-identiques → PSI ~0 → V."""
    actual   = [0.20, 0.30, 0.25, 0.25]
    expected = [0.20, 0.30, 0.25, 0.25]
    record = {**BASE}
    config = {"psi_distributions": {"age_groupe": {"actual": actual, "expected": expected}}}
    results = dim.validate(record, config)
    psi = [r for r in results if "psi_age_groupe" in r.field_name]
    assert psi[0].status == DQStatus.VALID


def test_drift_IV_psi_distributions_divergentes():
    """Distributions très différentes → PSI élevé → IV."""
    actual   = [0.50, 0.10, 0.10, 0.30]
    expected = [0.10, 0.40, 0.40, 0.10]
    record = {**BASE}
    config = {"psi_distributions": {"age_groupe": {"actual": actual, "expected": expected}}}
    results = dim.validate(record, config)
    psi = [r for r in results if "psi_age_groupe" in r.field_name]
    assert psi[0].status == DQStatus.INVALID


# ── KL Divergence ─────────────────────────────────────────────────────────────

def test_drift_V_kl_faible():
    p = [0.25, 0.25, 0.25, 0.25]
    q = [0.25, 0.25, 0.25, 0.25]
    config = {"kl_distributions": {"actual": p, "expected": q}}
    results = dim.validate({**BASE}, config)
    kl = [r for r in results if r.field_name == "prediction_distribution"]
    assert kl[0].status == DQStatus.VALID


def test_drift_IV_kl_eleve():
    p = [0.90, 0.05, 0.03, 0.02]
    q = [0.10, 0.40, 0.30, 0.20]
    config = {"kl_distributions": {"actual": p, "expected": q}}
    results = dim.validate({**BASE}, config)
    kl = [r for r in results if r.field_name == "prediction_distribution"]
    assert kl[0].status == DQStatus.INVALID


# ── Fonctions utilitaires ─────────────────────────────────────────────────────

def test_compute_psi_distributions_identiques():
    p = [0.25, 0.25, 0.25, 0.25]
    assert _compute_psi(p, p) == pytest.approx(0.0, abs=1e-4)


def test_compute_kl_distributions_identiques():
    p = [0.25, 0.25, 0.25, 0.25]
    assert _compute_kl(p, p) == pytest.approx(0.0, abs=1e-4)
