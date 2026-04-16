# findata-dq-engine

**Financial & Insurance Data Quality Engine — Buzzelli Extended (12 dimensions)**

Moteur de qualité des données pour les secteurs financier et assurantiel. Détecte les anomalies, évalue la conformité et remédie automatiquement via LLM.

---

## Ce que ça fait

- **12 dimensions DQ** : Complétude, Conformité, Cohérence, Timeliness, Congruence, Privacy, Business Rules, Anomaly Detection, Précision, Exactitude, Collection, Fairness
- **Pipeline Raw → Staged → Mastered** : seuls les enregistrements sans IV critique passent en Mastered
- **Isolation Forest** : détection d'anomalies multivariées sur sinistres et polices
- **Remédiation LLM** : suggestions de correction via Claude API avec scoring de confiance
- **API REST** : endpoint `POST /validate` — scorecard JSON en retour
- **Dashboard Streamlit** : heatmap, drill-down IV, scores par dimension

---

## Démarrage rapide

### Prérequis

- Python 3.11+
- (Optionnel) Docker Desktop pour lancer via conteneurs

### Installation

```bash
git clone https://github.com/akhadirdia/findata-dq-engine.git
cd findata-dq-engine
pip install -r requirements.txt
```

### Lancer l'API

```bash
uvicorn api.main:app --reload --port 8001
```

Swagger UI disponible sur **http://localhost:8001/docs**

### Lancer le dashboard

```bash
streamlit run dashboard/app.py
```

Dashboard disponible sur **http://localhost:8501**

### Avec Docker

```bash
docker compose up
```

- API : **http://localhost:8000**
- Dashboard : **http://localhost:8501**

---

## Utilisation de l'API

### Valider des enregistrements

```bash
curl -X POST http://localhost:8001/validate \
  -H "Content-Type: application/json" \
  -d '{
    "records": [
      {
        "num_police": "AU-000001",
        "id_client": "CLI-0001",
        "date_effet": "2024-06-01",
        "date_expiration": "2025-06-01",
        "type_couverture": "auto",
        "prime_annuelle": "1500.00",
        "montant_assure": "50000.00",
        "statut_police": "active",
        "franchise": "500",
        "date_creation": "2024-05-25"
      }
    ],
    "dataset": "policies",
    "ml_enabled": false,
    "llm_enabled": false
  }'
```

### Réponse

```json
{
  "scorecard_id": "8a92da51...",
  "dataset": "policies",
  "global_dq_score": 87.5,
  "nb_iv_total": 2,
  "nb_iv_high_impact": 0,
  "nb_records_mastered_eligible": 9,
  "by_dimension": {
    "Completeness": { "nb_tested": 70, "nb_v": 70, "dimension_score": 1.0 },
    "Timeliness":   { "nb_tested": 10, "nb_v":  8, "dimension_score": 0.8 }
  }
}
```

### Datasets supportés

| Valeur | Description |
|--------|-------------|
| `policies` | Polices d'assurance |
| `claims` | Sinistres |
| `logs` | Logs d'accès système |
| `model_metadata` | Métadonnées de modèles ML |

---

## Lancer les tests

```bash
# Tests unitaires + coverage
pytest tests/unit/ --cov=findata_dq --cov=api --cov-report=term-missing

# Tests API uniquement
pytest tests/unit/test_api/ -v
```

Coverage actuelle : **91%** — 208 tests

---

## Remédiation LLM (optionnel)

Pour activer la remédiation Claude API, créer un fichier `.env` :

```bash
cp .env.example .env
# Ajouter ta clé : ANTHROPIC_API_KEY=sk-ant-...
```

Puis dans la requête :

```json
{ "llm_enabled": true }
```

---

## Stack technique

| Couche | Technologie |
|--------|-------------|
| Moteur DQ | Python 3.11, Pydantic v2 |
| ML | Scikit-learn (Isolation Forest) |
| LLM | Anthropic Claude API (Haiku) |
| API | FastAPI + Uvicorn |
| Dashboard | Streamlit + Plotly |
| Tests | Pytest, coverage 91% |
| CI/CD | GitHub Actions |
| Conteneurs | Docker + Docker Compose |

---

## Structure du projet

```
findata_dq/
├── dimensions/     # 12 dimensions DQ (une classe par dimension)
├── models/         # Pydantic models (DQResult, Scorecard, ...)
├── pipeline/       # Orchestrateur Raw → Staged → Mastered
└── ai/             # Isolation Forest + remédiation LLM

api/
├── main.py         # App FastAPI
├── routers/        # Endpoints
└── schemas/        # Schémas requête/réponse

dashboard/
└── app.py          # Dashboard Streamlit

tests/
├── fixtures/       # CSV de test (policies, claims, logs, model_metadata)
└── unit/           # 208 tests unitaires
```

---

## Auteur

**Abdou Khadir DIA** — [linkedin.com/in/abdou-khadir-dia-284012130](https://linkedin.com/in/abdou-khadir-dia-284012130) · [github.com/akhadirdia](https://github.com/akhadirdia)
