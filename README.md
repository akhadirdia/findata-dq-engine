# findata-dq-engine

> **Les données financières de mauvaise qualité coûtent en moyenne 15 millions USD par an aux entreprises.**
> Ce moteur les détecte, les classe et les remédie — automatiquement.

---

## Le problème

Dans les secteurs financier et assurantiel, une police avec une date d'expiration incohérente, un sinistre avec un montant réclamé supérieur au montant assuré, ou un champ obligatoire manquant peut entraîner :

- Des **décisions de souscription erronées**
- Des **paiements frauduleux non détectés**
- Des **amendes réglementaires** (RGPD, Solvabilité II, AI Act)
- Des **modèles ML entraînés sur des données corrompues**

Les outils génériques de data quality ne comprennent pas ces règles métier. Ce moteur, lui, les connaît.

---

## Ce que fait findata-dq-engine

Un pipeline complet **Raw → Staged → Mastered** qui évalue chaque enregistrement sur **12 dimensions de qualité** issues du framework Buzzelli Extended, enrichi de ML et d'IA générative.

```
Données brutes (CSV / API)
        │
        ▼
┌───────────────────────────────────────────┐
│           12 DIMENSIONS DQ                │
│                                           │
│  Complétude      Conformité   Cohérence   │
│  Timeliness      Congruence   Privacy     │
│  Business Rules  Précision    Exactitude  │
│  Collection      Fairness     Anomaly     │
└───────────────┬───────────────────────────┘
                │
        ┌───────┴────────┐
        ▼                ▼
    STAGED           ISOLATION
  (résultats         FOREST ML
   V / S / IV)     (fraude, outliers)
        │                │
        └───────┬─────────┘
                ▼
        REMÉDIATION LLM
      (Claude API — suggestions
       de correction avec confiance)
                │
                ▼
    ┌─────────────────────┐
    │  SCORECARD + API    │  ← JSON structuré
    │  DASHBOARD STREAMLIT│  ← heatmap interactive
    └─────────────────────┘
                │
                ▼
           MASTERED
    (enregistrements sans IV critique)
```

**Statuts** : `V` (Valide) · `S` (Suspect) · `IV` (Invalide — bloque le passage en Mastered)

---

## Résultats sur données réelles

| Métrique | Valeur |
|----------|--------|
| Couverture de tests | **91%** (208 tests) |
| Dimensions couvertes | **12** (Buzzelli Extended) |
| Datasets supportés | Polices · Sinistres · Logs · Modèles ML |
| Rappel fraude (Isolation Forest) | **≥ 85%** sur données simulées |
| Temps de traitement (500 polices) | **< 1 seconde** (ML désactivé) |

---

## Démarrage en 3 minutes

### 1. Installation

```bash
git clone https://github.com/akhadirdia/findata-dq-engine.git
cd findata-dq-engine
pip install -r requirements.txt
```

### 2. Lancer l'API

```bash
uvicorn api.main:app --reload --port 8001
# → http://localhost:8001/docs  (Swagger interactif)
```

### 3. Valider vos données

```bash
curl -X POST http://localhost:8001/validate \
  -H "Content-Type: application/json" \
  -d '{
    "records": [{
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
    }],
    "dataset": "policies",
    "ml_enabled": false,
    "llm_enabled": false
  }'
```

**Réponse immédiate :**

```json
{
  "scorecard_id": "8a92da51...",
  "global_dq_score": 87.5,
  "nb_iv_total": 2,
  "nb_iv_high_impact": 0,
  "nb_records_mastered_eligible": 9,
  "pipeline_duration_seconds": 0.004,
  "by_dimension": {
    "Completeness":  { "dimension_score": 1.00, "nb_iv": 0 },
    "Timeliness":    { "dimension_score": 0.80, "nb_iv": 2 },
    "BusinessRules": { "dimension_score": 0.90, "nb_iv": 1 }
  }
}
```

### 4. Dashboard visuel (optionnel)

```bash
streamlit run dashboard/app.py
# → http://localhost:8501
```

Heatmap statut × dimension, drill-down des IV, histogramme des scores, anomalies ML.

### 5. Avec Docker

```bash
docker compose up
# API → http://localhost:8000  |  Dashboard → http://localhost:8501
```

---

## Remédiation LLM

Pour activer les suggestions de correction automatique via Claude API :

```bash
cp .env.example .env
# ANTHROPIC_API_KEY=sk-ant-...
```

```json
{ "llm_enabled": true }
```

Le moteur appelle Claude Haiku sur les résultats IV, retourne une action (`auto_fix` / `human_review` / `reject`) avec un score de confiance et une explication métier.

---

## Tests

```bash
# Suite complète (208 tests, coverage 91%)
pytest tests/unit/ --cov=findata_dq --cov=api --cov-report=term-missing

# API uniquement
pytest tests/unit/test_api/ -v
```

---

## Stack

`Python 3.11` · `FastAPI` · `Pydantic v2` · `Scikit-learn` · `Anthropic Claude API` · `Streamlit` · `Plotly` · `Docker` · `GitHub Actions CI`

---

## Auteur

**Abdou Khadir DIA** — Data Scientist Senior · ML · IA générative

[LinkedIn](https://linkedin.com/in/abdou-khadir-dia-284012130) · [GitHub](https://github.com/akhadirdia)
