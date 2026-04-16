"""
Fixtures pytest partagées entre tous les tests du pipeline findata-dq-engine.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

TODAY = date.today()
NOW_UTC = datetime.now(UTC)


# ─── Records valides ──────────────────────────────────────────────────────────

@pytest.fixture
def policy_valid():
    """Police d'assurance auto valide — toutes dimensions doivent retourner V."""
    return {
        "record_id": "POL-000001",
        "dataset": "policies",
        "num_police": "AU-123456",
        "id_client": "CLI-0000001",
        "date_effet": (TODAY - timedelta(days=20)).isoformat(),
        "date_expiration": (TODAY + timedelta(days=345)).isoformat(),
        "type_couverture": "auto",
        "prime_annuelle": 1800.00,
        "montant_assure": 45000.00,
        "statut_police": "active",
        "franchise": 500.00,
        "date_creation": (TODAY - timedelta(days=20)).isoformat(),
    }


@pytest.fixture
def claim_valid():
    """Sinistre valide."""
    effet = TODAY - timedelta(days=300)
    return {
        "record_id": "SIN-2024-000001",
        "dataset": "claims",
        "id_sinistre": "SIN-2024-000001",
        "num_police": "AU-123456",
        "id_client": "CLI-0000001",
        "date_sinistre": (TODAY - timedelta(days=10)).isoformat(),
        "date_declaration": (TODAY - timedelta(days=8)).isoformat(),
        "montant_reclame": 3500.00,
        "montant_rembourse": 3200.00,
        "type_dommage": "collision",
        "cause_sinistre": "Accident de la route",
        "statut_sinistre": "paye",
        "code_postal_lieu": "H3A 1B1",
        "montant_assure_police": 45000.00,
        "date_effet_police": effet.isoformat(),
        "date_expiration_police": (effet + timedelta(days=365)).isoformat(),
        "date_creation": (TODAY - timedelta(days=10)).isoformat(),
    }


@pytest.fixture
def log_valid():
    """Log applicatif valide."""
    return {
        "record_id": "LOG-0000001",
        "dataset": "logs",
        "log_id": "LOG-0000001",
        "timestamp": (NOW_UTC - timedelta(minutes=30)).isoformat(),
        "user_id": "USR-1234",
        "ip_address": "192.168.1.100",
        "action_type": "read",
        "session_id": "SES-12345",
        "device_type": "web",
        "status_code": 200,
        "payload_size": 5000,
        "date_creation": (NOW_UTC - timedelta(minutes=30)).isoformat(),
    }


@pytest.fixture
def model_valid():
    """Métadonnées modèle valides — drift V."""
    return {
        "record_id": "MDL-001",
        "dataset": "model_metadata",
        "model_id": "MDL-001",
        "model_name": "scoring_risque_auto",
        "model_version": "2.1.3",
        "training_date": (TODAY - timedelta(days=5)).isoformat(),
        "deployment_date": (TODAY - timedelta(days=3)).isoformat(),
        "statut_production": "production",
        "accuracy": 0.9200,
        "drift_score": 0.05,
        "drift_status": "V",
        "last_drift_check": (TODAY - timedelta(days=3)).isoformat(),
        "ai_act_compliance_flag": "compliant",
        "risque_vie_privee": "modere",
        "date_creation": (TODAY - timedelta(days=5)).isoformat(),
    }
