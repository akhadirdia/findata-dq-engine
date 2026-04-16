"""Tests Dimension 7 — Collection. Règle des 3 cas : V, S, IV."""

from findata_dq.dimensions.collection import Collection
from findata_dq.models.dq_result import DQStatus, ImpactLevel

dim = Collection()

SUMMARY_RECORD = {"record_id": "dataset_summary", "dataset": "policies"}


# ── Count checks ──────────────────────────────────────────────────────────────

def test_collection_V_count_exact():
    """Nombre reçu = attendu (écart 0%) → V."""
    config = {
        "count_checks": [{"name": "polices_mensuelles", "received": 500, "expected": 500}]
    }
    results = dim.validate(SUMMARY_RECORD, config)
    assert results[0].status == DQStatus.VALID


def test_collection_S_count_ecart_faible():
    """Écart entre 1% et 3% → S."""
    config = {
        "count_checks": [{"name": "polices_mensuelles", "received": 492, "expected": 500}]
    }
    # écart = 8/500 = 1.6%
    results = dim.validate(SUMMARY_RECORD, config)
    assert results[0].status == DQStatus.SUSPECT


def test_collection_IV_count_ecart_fort():
    """Écart >= 3% → IV."""
    config = {
        "count_checks": [{"name": "polices_mensuelles", "received": 480, "expected": 500}]
    }
    # écart = 20/500 = 4%
    results = dim.validate(SUMMARY_RECORD, config)
    assert results[0].status == DQStatus.INVALID
    assert results[0].impact == ImpactLevel.HIGH


# ── Sum checks ────────────────────────────────────────────────────────────────

def test_collection_V_somme_controle():
    """Somme calculée proche de la somme attendue (< 1%) → V."""
    config = {
        "sum_checks": [
            {
                "name": "primes_totales",
                "computed_sum": 900_500.0,
                "expected_sum": 900_000.0,
                "field": "prime_annuelle",
            }
        ]
    }
    # écart ≈ 0.056% < 1%
    results = dim.validate(SUMMARY_RECORD, config)
    assert results[0].status == DQStatus.VALID


def test_collection_IV_somme_controle_ecart():
    """Somme avec écart > 3% → IV."""
    config = {
        "sum_checks": [
            {
                "name": "primes_totales",
                "computed_sum": 865_000.0,
                "expected_sum": 900_000.0,
                "field": "prime_annuelle",
            }
        ]
    }
    # écart ≈ 3.9%
    results = dim.validate(SUMMARY_RECORD, config)
    assert results[0].status == DQStatus.INVALID


def test_collection_vide_sans_config():
    """Sans config → résultat vide."""
    results = dim.validate(SUMMARY_RECORD, {})
    assert results == []
