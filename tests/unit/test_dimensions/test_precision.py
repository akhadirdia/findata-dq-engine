"""Tests Dimension 4 — Precision. Règle des 3 cas : V, S, IV."""

import pytest
from findata_dq.dimensions.precision import Precision
from findata_dq.models.dq_result import DQStatus

dim = Precision()


# ── Montants assurance ────────────────────────────────────────────────────────

def test_precision_V_montant_2dec():
    """prime_annuelle avec 2 décimales explicites (string CSV) → V."""
    # Les valeurs CSV arrivent comme strings — les floats Python perdent les zéros trailing
    record = {"record_id": "T-000", "dataset": "policies", "prime_annuelle": "1800.00"}
    results = dim.validate(record, {"fields": ["prime_annuelle"]})
    assert results[0].status == DQStatus.VALID


def test_precision_IV_montant_0dec():
    """prime_annuelle entier (0 déc) → IV."""
    record = {"record_id": "T-001", "dataset": "policies", "prime_annuelle": 1800}
    results = dim.validate(record, {"fields": ["prime_annuelle"]})
    assert results[0].status == DQStatus.INVALID


def test_precision_IV_montant_1dec():
    """prime_annuelle avec 1 décimale → IV."""
    record = {"record_id": "T-002", "dataset": "policies", "prime_annuelle": "1800.5"}
    results = dim.validate(record, {"fields": ["prime_annuelle"]})
    assert results[0].status == DQStatus.INVALID


# ── Scores risque ─────────────────────────────────────────────────────────────

def test_precision_V_score_4dec():
    """drift_score avec 4 décimales → V."""
    record = {"record_id": "T-003", "dataset": "model_metadata", "drift_score": 0.0512}
    results = dim.validate(record, {"fields": ["drift_score"]})
    assert results[0].status == DQStatus.VALID


def test_precision_IV_score_1dec():
    """score avec 1 décimale → IV (< 2 min)."""
    record = {"record_id": "T-004", "dataset": "model_metadata", "drift_score": "0.5"}
    results = dim.validate(record, {"fields": ["drift_score"]})
    assert results[0].status == DQStatus.INVALID


# ── Détection automatique des champs ─────────────────────────────────────────

def test_precision_auto_detection(policy_valid):
    """Sans config, détecte automatiquement les champs financiers du record."""
    results = dim.validate(policy_valid)
    champs = {r.field_name for r in results}
    assert "prime_annuelle" in champs
    assert "montant_assure" in champs


def test_precision_champ_absent_ignore():
    """Champ absent → ignoré."""
    record = {"record_id": "T-005", "dataset": "policies"}
    results = dim.validate(record, {"fields": ["prime_annuelle"]})
    assert results == []
