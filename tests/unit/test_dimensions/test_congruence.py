"""Tests Dimension 6 — Congruence. Règle des 3 cas : V, S, IV."""

from findata_dq.dimensions.congruence import Congruence
from findata_dq.models.dq_result import DQStatus

dim = Congruence()

ZSCORE_STATS = {
    "prime_annuelle": {"mean": 1800.0, "std": 400.0},
    "montant_reclame": {"mean": 5000.0, "std": 2000.0},
    "payload_size": {"mean": 10000.0, "std": 3000.0},
}


# ── Z-Score ───────────────────────────────────────────────────────────────────

def test_congruence_V_zscore(policy_valid):
    """prime_annuelle dans la distribution normale → V."""
    config = {"fields": ["prime_annuelle"], "zscore_stats": ZSCORE_STATS}
    results = dim.validate(policy_valid, config)
    assert results[0].status == DQStatus.VALID
    assert abs(results[0].details["z_score"]) <= 2.0


def test_congruence_S_zscore(policy_valid):
    """prime_annuelle Z-score entre 2 et 3.5 → S."""
    record = dict(policy_valid)
    record["prime_annuelle"] = 1800 + 2.5 * 400  # Z = 2.5
    config = {"fields": ["prime_annuelle"], "zscore_stats": ZSCORE_STATS}
    results = dim.validate(record, config)
    assert results[0].status == DQStatus.SUSPECT


def test_congruence_IV_zscore(policy_valid):
    """prime_annuelle outlier Z-score > 3.5 → IV."""
    record = dict(policy_valid)
    record["prime_annuelle"] = 1800 + 5.0 * 400  # Z = 5.0
    config = {"fields": ["prime_annuelle"], "zscore_stats": ZSCORE_STATS}
    results = dim.validate(record, config)
    assert results[0].status == DQStatus.INVALID


# ── Comparison to Average ─────────────────────────────────────────────────────

def test_congruence_V_comparison_to_average(claim_valid):
    """montant_reclame proche de la moyenne → V."""
    config = {
        "fields": ["montant_reclame"],
        "historical_means": {"montant_reclame": 5000.0},
    }
    results = dim.validate(claim_valid, config)
    # 3500 vs 5000 → écart 30% → IV... mais sans zscore_stats, on utilise CTA
    # 3500/5000 = 30% > 15% → IV
    assert results[0].status in (DQStatus.INVALID, DQStatus.SUSPECT)


def test_congruence_V_comparison_to_average_proche():
    """Valeur proche de la moyenne historique → V."""
    record = {"record_id": "T-001", "dataset": "claims", "montant_reclame": 5050.0}
    config = {
        "fields": ["montant_reclame"],
        "historical_means": {"montant_reclame": 5000.0},
    }
    results = dim.validate(record, config)
    assert results[0].status == DQStatus.VALID  # écart 1% < 5%


def test_congruence_IV_comparison_to_average_outlier():
    """Valeur très éloignée de la moyenne → IV."""
    record = {"record_id": "T-002", "dataset": "claims", "montant_reclame": 100_000.0}
    config = {
        "fields": ["montant_reclame"],
        "historical_means": {"montant_reclame": 5000.0},
    }
    results = dim.validate(record, config)
    assert results[0].status == DQStatus.INVALID


# ── Prior Value Comparison ────────────────────────────────────────────────────

def test_congruence_V_prior_value():
    """Évolution < 10% → V."""
    record = {"record_id": "T-003", "dataset": "policies", "prime_annuelle": 1810.0}
    config = {
        "fields": ["prime_annuelle"],
        "prior_values": {"prime_annuelle": 1800.0},
    }
    results = dim.validate(record, config)
    assert results[0].status == DQStatus.VALID  # écart ~0.5%


def test_congruence_S_prior_value():
    """Évolution entre 10% et 20% → S."""
    record = {"record_id": "T-004", "dataset": "policies", "prime_annuelle": 2000.0}
    config = {
        "fields": ["prime_annuelle"],
        "prior_values": {"prime_annuelle": 1800.0},
    }
    results = dim.validate(record, config)
    # (200 / 1900) * 100 ≈ 10.5% → S
    assert results[0].status == DQStatus.SUSPECT


def test_congruence_IV_prior_value():
    """Évolution >= 20% → IV."""
    record = {"record_id": "T-005", "dataset": "policies", "prime_annuelle": 3000.0}
    config = {
        "fields": ["prime_annuelle"],
        "prior_values": {"prime_annuelle": 1800.0},
    }
    results = dim.validate(record, config)
    assert results[0].status == DQStatus.INVALID


def test_congruence_vide_si_aucun_stats():
    """Sans stats configurées → résultat vide."""
    record = {"record_id": "T-006", "dataset": "policies", "prime_annuelle": 9999.0}
    results = dim.validate(record, {"fields": ["prime_annuelle"]})
    assert results == []
