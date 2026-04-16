"""
Tests Étape 10 — API FastAPI.

Stratégie
---------
- Utilise httpx.TestClient (pas de serveur réel nécessaire)
- ML désactivé, LLM désactivé pour vitesse et déterminisme
- Blocs :
    Bloc 1 : Routes utilitaires (/, /health)
    Bloc 2 : POST /validate — cas valides
    Bloc 3 : POST /validate — détection de defects
    Bloc 4 : POST /validate — validation de la requête (erreurs 422)
    Bloc 5 : POST /validate — options avancées
"""

from __future__ import annotations

from datetime import date, timedelta

from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)

TODAY = date.today()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_policy(
    num: str = "AU-000001",
    client_id: str = "CLI-0001",
    prime: str = "1500.00",
    montant: str = "50000.00",
    statut: str = "active",
    effet_offset: int = -100,
    expiration_offset: int = 265,
    creation_offset: int = -10,
) -> dict:
    return {
        "num_police": num,
        "id_client": client_id,
        "date_effet": (TODAY + timedelta(days=effet_offset)).isoformat(),
        "date_expiration": (TODAY + timedelta(days=expiration_offset)).isoformat(),
        "type_couverture": "auto",
        "prime_annuelle": prime,
        "montant_assure": montant,
        "statut_police": statut,
        "franchise": "500",
        "date_creation": (TODAY + timedelta(days=creation_offset)).isoformat(),
    }


def _post_validate(records: list[dict], **kwargs) -> dict:
    body = {
        "records": records,
        "dataset": "policies",
        "pipeline_env": "development",
        "ml_enabled": False,
        "llm_enabled": False,
        **kwargs,
    }
    resp = client.post("/validate", json=body)
    return resp


# ── Bloc 1 : Routes utilitaires ───────────────────────────────────────────────

class TestUtilityRoutes:

    def test_root_retourne_200(self):
        resp = client.get("/")
        assert resp.status_code == 200

    def test_root_contient_service(self):
        data = client.get("/").json()
        assert data["service"] == "findata-dq-engine"

    def test_root_contient_endpoints(self):
        data = client.get("/").json()
        assert "endpoints" in data
        assert "POST /validate" in data["endpoints"]

    def test_health_retourne_200(self):
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_health_status_ok(self):
        data = client.get("/health").json()
        assert data["status"] == "ok"

    def test_docs_accessibles(self):
        # Swagger UI doit retourner 200
        resp = client.get("/docs")
        assert resp.status_code == 200


# ── Bloc 2 : POST /validate — cas valides ─────────────────────────────────────

class TestValidateSuccess:

    def test_un_enregistrement_valide_retourne_200(self):
        resp = _post_validate([_make_policy()])
        assert resp.status_code == 200

    def test_scorecard_id_present(self):
        data = _post_validate([_make_policy()]).json()
        assert "scorecard_id" in data
        assert len(data["scorecard_id"]) == 32

    def test_total_records_correct(self):
        records = [_make_policy(num=f"AU-{i:06d}", client_id=f"CLI-{i:04d}") for i in range(5)]
        data = _post_validate(records).json()
        assert data["total_records"] == 5

    def test_global_dq_score_dans_plage(self):
        data = _post_validate([_make_policy()]).json()
        assert 0.0 <= data["global_dq_score"] <= 100.0

    def test_pass_rate_dans_plage(self):
        data = _post_validate([_make_policy()]).json()
        assert 0.0 <= data["pass_rate"] <= 1.0

    def test_by_dimension_non_vide(self):
        data = _post_validate([_make_policy()]).json()
        assert len(data["by_dimension"]) > 0

    def test_pipeline_duration_positive(self):
        data = _post_validate([_make_policy()]).json()
        assert data["pipeline_duration_seconds"] is not None
        assert data["pipeline_duration_seconds"] >= 0.0

    def test_evaluated_at_format_iso(self):
        data = _post_validate([_make_policy()]).json()
        # doit contenir une date ISO 8601
        assert "T" in data["evaluated_at"]

    def test_dataset_retourne_dans_reponse(self):
        data = _post_validate([_make_policy()]).json()
        assert data["dataset"] == "policies"

    def test_10_enregistrements_valides(self):
        records = [_make_policy(num=f"AU-{i:06d}", client_id=f"CLI-{i:04d}") for i in range(10)]
        data = _post_validate(records).json()
        assert data["total_records"] == 10
        assert data["global_dq_score"] > 50.0


# ── Bloc 3 : POST /validate — détection de defects ───────────────────────────

class TestValidateDefects:

    def test_detecte_num_police_manquant(self):
        record = _make_policy()
        record["num_police"] = ""
        data = _post_validate([record]).json()
        assert data["nb_iv_total"] >= 1

    def test_detecte_prime_negative(self):
        record = _make_policy(prime="-500.00")
        data = _post_validate([record]).json()
        assert data["nb_iv_total"] >= 1

    def test_detecte_date_expiration_avant_effet(self):
        record = _make_policy()
        record["date_effet"] = (TODAY + timedelta(days=100)).isoformat()
        record["date_expiration"] = (TODAY - timedelta(days=100)).isoformat()
        data = _post_validate([record]).json()
        assert data["nb_iv_total"] >= 1

    def test_completeness_presente_dans_by_dimension(self):
        data = _post_validate([_make_policy()]).json()
        assert "Completeness" in data["by_dimension"]

    def test_by_dimension_contient_nb_tested(self):
        data = _post_validate([_make_policy()]).json()
        comp = data["by_dimension"].get("Completeness", {})
        assert comp.get("nb_tested", 0) > 0

    def test_plusieurs_defects_cumulent(self):
        """num_police vide + prime négative → au moins 2 IV."""
        record = _make_policy(prime="-100.00")
        record["num_police"] = ""
        data = _post_validate([record]).json()
        assert data["nb_iv_total"] >= 2

    def test_enregistrements_propres_score_eleve(self):
        records = [_make_policy(num=f"AU-{i:06d}", client_id=f"CLI-{i:04d}") for i in range(5)]
        data = _post_validate(records).json()
        assert data["global_dq_score"] > 60.0


# ── Bloc 4 : POST /validate — validation de la requête ───────────────────────

class TestValidateRequestErrors:

    def test_liste_vide_retourne_422(self):
        resp = client.post("/validate", json={
            "records": [],
            "dataset": "policies",
            "ml_enabled": False,
            "llm_enabled": False,
        })
        assert resp.status_code == 422

    def test_dataset_invalide_retourne_422(self):
        resp = client.post("/validate", json={
            "records": [_make_policy()],
            "dataset": "inexistant",
            "ml_enabled": False,
            "llm_enabled": False,
        })
        assert resp.status_code == 422

    def test_body_manquant_retourne_422(self):
        resp = client.post("/validate")
        assert resp.status_code == 422

    def test_pipeline_env_invalide_retourne_422(self):
        resp = client.post("/validate", json={
            "records": [_make_policy()],
            "dataset": "policies",
            "pipeline_env": "invalid_env",
            "ml_enabled": False,
            "llm_enabled": False,
        })
        assert resp.status_code == 422


# ── Bloc 5 : POST /validate — options avancées ───────────────────────────────

class TestValidateOptions:

    def test_include_raw_results_false_pas_de_raw(self):
        data = _post_validate([_make_policy()], include_raw_results=False).json()
        assert data.get("raw_results") is None

    def test_include_raw_results_true_retourne_liste(self):
        data = _post_validate([_make_policy()], include_raw_results=True).json()
        assert isinstance(data.get("raw_results"), list)
        assert len(data["raw_results"]) > 0

    def test_raw_result_contient_champs_attendus(self):
        data = _post_validate([_make_policy()], include_raw_results=True).json()
        first = data["raw_results"][0]
        assert "datum_id" in first
        assert "dimension" in first
        assert "status" in first
        assert "impact" in first

    def test_pipeline_env_staging_accepte(self):
        resp = _post_validate([_make_policy()], pipeline_env="staging")
        assert resp.status_code == 200
        assert resp.json()["pipeline_env"] == "staging"

    def test_ml_enabled_sur_50_records_ne_plante_pas(self):
        records = [_make_policy(num=f"AU-{i:06d}", client_id=f"CLI-{i:04d}") for i in range(50)]
        resp = _post_validate(records, ml_enabled=True)
        assert resp.status_code == 200
        assert resp.json()["total_records"] == 50
