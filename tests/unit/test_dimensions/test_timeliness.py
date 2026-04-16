"""Tests Dimension 2 — Timeliness. Règle des 3 cas : V, S, IV."""

from datetime import UTC, datetime, timedelta

from findata_dq.dimensions.timeliness import Timeliness
from findata_dq.models.dq_result import DQStatus

dim = Timeliness()
NOW = datetime.now(UTC)


def _record_with_date(field: str, dt: datetime, mode: str = "insurance", dataset: str = "policies"):
    return {
        "record_id": "TEST-001",
        "dataset": dataset,
        field: dt.isoformat(),
    }


# ── Mode insurance ────────────────────────────────────────────────────────────

def test_timeliness_V_insurance(policy_valid):
    """date_creation récente (< 30 jours) → V."""
    config = {
        "date_fields": {"date_creation": "insurance"},
        "reference_dt": NOW,
    }
    results = dim.validate(policy_valid, config)
    assert results[0].status == DQStatus.VALID


def test_timeliness_S_insurance():
    """date_creation entre 30 et 90 jours → S."""
    record = _record_with_date("date_creation", NOW - timedelta(days=45))
    config = {"date_fields": {"date_creation": "insurance"}, "reference_dt": NOW}
    results = dim.validate(record, config)
    assert results[0].status == DQStatus.SUSPECT


def test_timeliness_IV_insurance():
    """date_creation > 90 jours → IV."""
    record = _record_with_date("date_creation", NOW - timedelta(days=120))
    config = {"date_fields": {"date_creation": "insurance"}, "reference_dt": NOW}
    results = dim.validate(record, config)
    assert results[0].status == DQStatus.INVALID


# ── Mode realtime ─────────────────────────────────────────────────────────────

def test_timeliness_V_realtime(log_valid):
    """Timestamp < 60 min → V."""
    config = {
        "date_fields": {"timestamp": "realtime"},
        "reference_dt": NOW,
    }
    results = dim.validate(log_valid, config)
    assert results[0].status == DQStatus.VALID


def test_timeliness_S_realtime():
    """Timestamp entre 60 et 240 min → S."""
    record = _record_with_date("timestamp", NOW - timedelta(minutes=90), dataset="logs")
    config = {"date_fields": {"timestamp": "realtime"}, "reference_dt": NOW}
    results = dim.validate(record, config)
    assert results[0].status == DQStatus.SUSPECT


def test_timeliness_IV_realtime():
    """Timestamp > 240 min → IV."""
    record = _record_with_date("timestamp", NOW - timedelta(minutes=300), dataset="logs")
    config = {"date_fields": {"timestamp": "realtime"}, "reference_dt": NOW}
    results = dim.validate(record, config)
    assert results[0].status == DQStatus.INVALID


# ── Mode model ────────────────────────────────────────────────────────────────

def test_timeliness_V_model(model_valid):
    """last_drift_check < 7 jours → V."""
    config = {
        "date_fields": {"last_drift_check": "model"},
        "reference_dt": NOW,
    }
    results = dim.validate(model_valid, config)
    assert results[0].status == DQStatus.VALID


def test_timeliness_S_model():
    """last_drift_check entre 7 et 30 jours → S."""
    record = _record_with_date("last_drift_check", NOW - timedelta(days=15), dataset="model_metadata")
    config = {"date_fields": {"last_drift_check": "model"}, "reference_dt": NOW}
    results = dim.validate(record, config)
    assert results[0].status == DQStatus.SUSPECT


def test_timeliness_IV_model():
    """last_drift_check > 30 jours → IV."""
    record = _record_with_date("last_drift_check", NOW - timedelta(days=45), dataset="model_metadata")
    config = {"date_fields": {"last_drift_check": "model"}, "reference_dt": NOW}
    results = dim.validate(record, config)
    assert results[0].status == DQStatus.INVALID


def test_timeliness_champ_absent_ignore():
    """Champ absent → résultat vide (Completeness s'en charge)."""
    record = {"record_id": "T-001", "dataset": "policies"}
    config = {"date_fields": {"date_creation": "insurance"}, "reference_dt": NOW}
    results = dim.validate(record, config)
    assert results == []
