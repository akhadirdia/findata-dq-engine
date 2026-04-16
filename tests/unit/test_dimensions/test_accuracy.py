"""Tests Dimension 3 — Accuracy. Règle des 3 cas : V, S, IV."""

from findata_dq.dimensions.accuracy import Accuracy
from findata_dq.models.dq_result import DQStatus, ImpactLevel

dim = Accuracy()

VALID_TYPES = {"auto", "habitation", "vie", "sante", "entreprise"}
REFERENCE_BAREME = {"collision", "vol", "incendie", "degats_eau"}


# ── Source d'autorité ─────────────────────────────────────────────────────────

def test_accuracy_V_source_autorite(policy_valid):
    """type_couverture dans la source d'autorité → V."""
    config = {
        "authority_checks": [
            {"field": "type_couverture", "reference": VALID_TYPES, "impact": "H"},
        ]
    }
    results = dim.validate(policy_valid, config)
    assert results[0].status == DQStatus.VALID


def test_accuracy_IV_source_autorite(policy_valid):
    """type_couverture absent de la source d'autorité → IV."""
    record = dict(policy_valid)
    record["type_couverture"] = "moto"
    config = {
        "authority_checks": [
            {"field": "type_couverture", "reference": VALID_TYPES, "impact": "H"},
        ]
    }
    results = dim.validate(record, config)
    assert results[0].status == DQStatus.INVALID
    assert results[0].impact == ImpactLevel.HIGH


def test_accuracy_IV_source_autorite_type_dommage(claim_valid):
    """type_dommage inconnu du barème → IV."""
    record = dict(claim_valid)
    record["type_dommage"] = "meteorite"
    config = {
        "authority_checks": [
            {"field": "type_dommage", "reference": REFERENCE_BAREME, "impact": "M"},
        ]
    }
    results = dim.validate(record, config)
    assert results[0].status == DQStatus.INVALID


# ── Triangulation mathématique ────────────────────────────────────────────────

def test_accuracy_V_triangulation_ratio(claim_valid):
    """ratio sinistre/prime calculé cohérent avec la valeur déclarée → V."""
    montant = float(claim_valid["montant_reclame"])
    prime = 1800.0
    ratio_calc = montant / prime
    config = {
        "triangulations": [
            {
                "name": "ratio_sinistre_prime",
                "computed": ratio_calc,
                "declared": ratio_calc,  # identique
                "tolerance": 0.01,
                "impact": "H",
                "field": "montant_reclame",
            }
        ]
    }
    results = dim.validate(claim_valid, config)
    assert results[0].status == DQStatus.VALID


def test_accuracy_IV_triangulation_incoherence():
    """Triangulation incohérente — écart > 1% → IV."""
    record = {"record_id": "T-001", "dataset": "claims"}
    config = {
        "triangulations": [
            {
                "name": "position_ouverts_fermes",
                "computed": 150,   # nb_ouverts - nb_fermes calculé
                "declared": 200,   # valeur portée dans le registre
                "tolerance": 0.01,
                "impact": "H",
                "field": "position_sinistres",
            }
        ]
    }
    results = dim.validate(record, config)
    assert results[0].status == DQStatus.INVALID
    assert results[0].impact == ImpactLevel.HIGH


def test_accuracy_V_triangulation_tolerance():
    """Triangulation avec écart dans la tolérance → V."""
    record = {"record_id": "T-002", "dataset": "policies"}
    config = {
        "triangulations": [
            {
                "name": "somme_primes",
                "computed": 100_050.0,
                "declared": 100_000.0,  # écart 0.05% < tolérance 1%
                "tolerance": 0.01,
                "impact": "H",
                "field": "prime_annuelle",
            }
        ]
    }
    results = dim.validate(record, config)
    assert results[0].status == DQStatus.VALID
