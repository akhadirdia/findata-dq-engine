# CLAUDE.md — Buzzelli Extended: Financial & Insurance Data Quality Engine

> Ce fichier est lu automatiquement par Claude Code à chaque session.
> Source de vérité absolue du projet. Ne jamais dévier de cette architecture sans validation explicite.

---

## 1. OBJECTIF DU PROJET

Tu construis **findata-dq-engine** : un pipeline de qualité des données de niveau production
qui implémente les 8 dimensions de Brian Buzzelli, **étendues de 4 dimensions propres à
l'assurance et à la gouvernance IA** (12 dimensions au total).

**La promesse** : Transformer des données d'assurance brutes, imparfaites et risquées en
données certifiées (V/S/IV), avec un audit de biais IA, une remédiation intelligente par LLM,
et un rapport exécutif automatique — le tout en < 2 minutes sur un portefeuille de 10 000 polices.

**Domaines couverts** :
- A. Données d'assurance de dommages (polices, sinistres, clients, véhicules)
- B. Données de sécurité & fraude (logs applicatifs, transactions)
- C. Données de risques opérationnels (incidents, ESG, conformité)
- D. Données de performance en souscription (tarification, rentabilité)
- E. Données de gouvernance IA & modèles (biais, drift, explicabilité)

**Référence académique** : Buzzelli, B. — *Data Quality Assessment* (framework MDM)

---

## 2. ARCHITECTURE GLOBALE

```
DONNÉES BRUTES (Raw)
[policies.csv / claims.csv / access_logs.csv / model_metadata.csv / ...]
        │
        ▼
┌───────────────────────────────────────────────────────────┐
│  COUCHE 1 — VALIDATION DÉTERMINISTE (Buzzelli Extended)   │
│                                                           │
│  8 dimensions Buzzelli :                                  │
│  Completeness · Timeliness · Accuracy · Precision        │
│  Conformity · Congruence · Collection · Cohesion         │
│                                                           │
│  4 dimensions Extended :                                  │
│  BusinessRules · Fairness · ModelDrift · Privacy          │
│                                                           │
│  → Chaque datum reçoit : status (V/S/IV) + impact (H/M/L)│
└───────────────────────┬───────────────────────────────────┘
                        │
                        ▼
┌───────────────────────────────────────────────────────────┐
│  COUCHE 2 — DÉTECTION ML (Isolation Forest — v1)          │
│  Complète la Congruence sur données multivariées          │
│  → anomaly_score, outlier_flag                            │
└───────────────────────┬───────────────────────────────────┘
                        │
                        ▼
┌───────────────────────────────────────────────────────────┐
│  COUCHE 3 — INTELLIGENCE LLM (Claude API)                 │
│                                                           │
│  v1 — Remédiation : suggestion de correction par donnée  │
│  v2 — Rapport exécutif : texte pour Comité des Risques   │
│  v2 — Audit Fairness : SHAP + explication réglementaire  │
│  v3 — RAG Assistant : chatbot NL sur la scorecard        │
│  v3 — Predictive DQ : anticipation des dégradations      │
└───────────────────────┬───────────────────────────────────┘
                        │
                        ▼
┌───────────────────────────────────────────────────────────┐
│  OUTPUT                                                   │
│  • Scorecard heatmap interactive (Streamlit)              │
│  • Données Mastered (certifiées, en base)                 │
│  • Rapport exécutif .docx / JSON                          │
│  • Audit AI Act / Loi 25 PDF                              │
│  • API REST FastAPI (intégration systèmes tiers)          │
└───────────────────────────────────────────────────────────┘
```

---

## 3. LES 3 ÉTATS DU PIPELINE (Buzzelli)

| État | Description | Stockage |
|---|---|---|
| **Raw** | Données reçues, aucune altération | `data/raw/` |
| **Staged** | Zone de quarantaine — validations DQ appliquées | `data/staged/` |
| **Mastered** | Données certifiées V, approuvées pour production | `data/mastered/` |

**Règle absolue** : aucune donnée IV n'atteint l'état Mastered sans révision humaine explicite.
**Stratégie pre-use** : les contrôles bloquent la donnée AVANT qu'elle entre dans les calculs.

---

## 4. LES 12 DIMENSIONS — ALGORITHMES COMPLETS

### PARTIE A — LES 8 DIMENSIONS BUZZELLI

---

#### Dimension 1 — Completeness (Complétude)

**Logique** : Détecter les valeurs nulles, vides, ou placeholders sur les champs requis.

```
Statuts :
  M (Mandatory) + null → IV, Impact H
  O (Optional)  + null → S,  Impact L

Calcul :
  completeness_rate = (nb_lignes - nb_nulls) / nb_lignes * 100
  V  si completeness_rate = 100%
  S  si completeness_rate entre 95% et 100%
  IV si completeness_rate < 95%
```

**Variables clés** :
- Polices : `num_police` (M), `date_effet` (M), `prime_annuelle` (M), `type_couverture` (M)
- Sinistres : `id_sinistre` (M), `date_sinistre` (M), `montant_reclame` (M)
- Logs : `timestamp` (M), `user_id` (M), `action_type` (M)

---

#### Dimension 2 — Timeliness (Actualité)

**Logique** : Vérifier la fraîcheur des données par rapport à la date du jour.

```
Calcul :
  delta_jours = date_today - date_donnee

Polices / Sinistres (données assurantielles) :
  V  si delta_jours < 30
  S  si 30 <= delta_jours < 90
  IV si delta_jours >= 90

Logs de sécurité (données temps-réel) :
  V  si delta_minutes < 60
  S  si 60 <= delta_minutes < 240
  IV si delta_minutes >= 240

Métadonnées modèles IA :
  V  si delta_jours < 7   (modèle récemment audité)
  S  si 7 <= delta_jours < 30
  IV si delta_jours >= 30  (retraining ou audit requis)
```

---

#### Dimension 3 — Accuracy (Exactitude)

**Logique 1 — Source d'autorité** : Comparaison avec une table de référence officielle.
**Logique 2 — Triangulation mathématique** : Vérification par calcul indirect.

```
Triangulation assurance :
  Position calculée = nb_sinistres_ouverts - nb_sinistres_fermes
  Si position_calculee != position_portee → IV, Impact H

Triangulation portefeuille :
  prime_totale_portefeuille = SUM(prime_annuelle)
  Si |prime_totale - valeur_controle| / valeur_controle > 0.01 → IV
```

**Variables clés** :
- `ratio_sinistre_prime` = `montant_reclame` / `prime_annuelle` (vérifiable mathématiquement)
- `montant_reclame` vs table de référence barème (source d'autorité)

---

#### Dimension 4 — Precision (Précision numérique)

**Logique** : Vérifier le nombre de décimales pour les champs financiers.

```
Montants assurance (primes, sinistres) :
  V  si nb_decimales >= 2
  IV si nb_decimales < 2

Taux de change (si données FX présentes) :
  V  si nb_decimales >= 6
  S  si nb_decimales == 5
  IV si nb_decimales < 5

Scores risque (0.0 à 1.0) :
  V  si nb_decimales >= 4
  IV si nb_decimales < 2
```

---

#### Dimension 5 — Conformity (Conformité de format)

**Logique** : Validation par Regex des formats standards.

```python
PATTERNS = {
    # Assurance
    "num_police":       r"^[A-Z]{2,3}-\d{6,10}$",
    "type_couverture":  r"^(auto|habitation|vie|sante|entreprise)$",
    "statut_sinistre":  r"^(ouvert|ferme|en_cours|rejete|paye)$",
    "code_postal_ca":   r"^[A-Z]\d[A-Z]\s?\d[A-Z]\d$",  # format canadien
    "numero_vin":       r"^[A-HJ-NPR-Z0-9]{17}$",

    # Logs sécurité
    "ip_address":       r"^(\d{1,3}\.){3}\d{1,3}$",
    "action_type":      r"^(login|logout|read|write|delete|transfer)$",

    # Gouvernance IA
    "model_version":    r"^\d+\.\d+\.\d+$",
    "ai_act_flag":      r"^(compliant|non_compliant|under_review)$",
}

# Statuts :
#   format valide → V
#   format invalide → IV, Impact = selon le champ (H pour num_police, M pour code_postal)
```

---

#### Dimension 6 — Congruence (Détection d'outliers — LA PLUS TECHNIQUE)

S'applique aux séries temporelles et distributions numériques. **Trois algorithmes** :

```
1. Prior Value Comparison (évolution jour-à-jour) :
   formule : |valeur_J - valeur_J-1| / moyenne(valeur_J, valeur_J-1) * 100
   V  si < 10%
   S  si 10% <= x < 20%
   IV si >= 20%

2. Comparison to Average (écart à la moyenne historique) :
   formule : |valeur - moyenne_historique| / moyenne_historique * 100
   V  si < 5%   (assurance, plus tolérant que FX)
   S  si 5% <= x < 15%
   IV si >= 15%

3. Z-Score (le plus robuste — fenêtre glissante 30 jours) :
   formule : Z = (valeur_actuelle - moyenne_30j) / ecart_type_30j
   V  si |Z| <= 2
   S  si 2 < |Z| < 3.5
   IV si |Z| >= 3.5
```

**Variables clés** :
- `montant_reclame` par sinistre (détecter les sinistres frauduleux)
- `prime_annuelle` par segment (détecter les erreurs de tarification)
- `anomaly_score` dans les logs (détecter les intrusions)
- `evolution_prime_mensuelle` (détecter les décrochages de performance)

---

#### Dimension 7 — Collection (Intégrité de l'ensemble)

**Logique** : Un ensemble de données doit être complet (tous les éléments attendus sont présents).

```
Algorithme :
  1. record_count_attendu = valeur de contrôle officielle (ou config)
  2. record_count_recu    = nb_lignes_fichier
  3. ecart = |record_count_recu - record_count_attendu| / record_count_attendu * 100

  V  si ecart < 1%
  S  si 1% <= ecart < 3%
  IV si ecart >= 3%, Impact H

Contrôle additionnel — Somme de contrôle :
  somme_primes_calculee vs somme_primes_attendue → même règle 1%/3%
```

**Cas d'usage assurance** :
- Portefeuille de polices : toutes les polices actives doivent être présentes
- Fichier de sinistres mensuel : vérifier le nombre de sinistres vs registre

---

#### Dimension 8 — Cohesion (Intégrité référentielle)

**Logique** : Les clés étrangères doivent référencer des enregistrements valides.

```
Algorithme :
  clef_etrangere IN table_reference → V
  clef_etrangere NOT IN table_reference → IV, Impact H

Contrôles assurance :
  sinistre.num_police      → policies.num_police      (chaque sinistre a une police valide)
  client.id_client         → policies.id_client       (chaque police a un client valide)
  log.user_id              → users.user_id            (chaque log a un utilisateur valide)
  model.model_id           → model_registry.model_id  (chaque run référence un modèle)
```

---

### PARTIE B — LES 4 DIMENSIONS EXTENDED (PROPRES À CE PROJET)

---

#### Dimension 9 — BusinessRules (Règles Métier Inter-Tables)

**Logique** : Contraintes métier qui ne peuvent pas être vérifiées champ par champ.
Elles requièrent une jointure ou une relation entre plusieurs champs/tables.

```
Règles assurance — sinistres :
  R1 : date_sinistre DOIT être dans [date_effet, date_expiration]
       → IV si date_sinistre < date_effet ou date_sinistre > date_expiration, Impact H

  R2 : montant_reclame NE PEUT PAS dépasser montant_assure
       → IV si montant_reclame > montant_assure, Impact H

  R3 : une police EXPIREE ne peut pas avoir de sinistre OUVERT
       → IV si statut_sinistre == 'ouvert' ET date_expiration < date_today, Impact H

Règles sécurité — logs :
  R4 : une action 'delete' requiert session_id actif dans la même heure
       → IV si action == 'delete' ET session manquante, Impact H

  R5 : montant_transaction > 50000 requiert status_code == 200 (validé)
       → S si status_code != 200, Impact M

Règles IA — modèles :
  R6 : un modèle en production NE PEUT PAS avoir drift_score IV
       → IV si modele_statut == 'production' ET drift_score > seuil_psi, Impact H
```

**Output** : `rule_id`, `status`, `impact`, `description_violation`, `champs_impliques`

---

#### Dimension 10 — Fairness (Équité & Biais IA)

**Logique** : Détecter les discriminations dans les décisions algorithmiques
(tarification, scoring risque) selon les attributs protégés.

```
Métriques calculées :

1. Disparate Impact Ratio :
   DI = P(decision=defavorable | groupe_A) / P(decision=defavorable | groupe_B)
   V  si DI dans [0.80, 1.25]   (seuil légal 4/5ths rule + AI Act)
   S  si DI dans [0.70, 0.80] ou [1.25, 1.30]
   IV si DI < 0.70 ou DI > 1.30, Impact H

2. Demographic Parity :
   DP = |P(score_haut | sexe=H) - P(score_haut | sexe=F)|
   V  si DP < 0.05
   S  si 0.05 <= DP < 0.10
   IV si DP >= 0.10, Impact H

3. Equalized Odds :
   EOdds = |TPR_groupe_A - TPR_groupe_B|
   V  si EOdds < 0.05
   IV si EOdds >= 0.10, Impact H

Attributs protégés à surveiller :
  age, sexe, code_postal (proxy revenu/ethnie), revenu_estime
```

**Référence réglementaire** : AI Act Article 10(3) · Loi 25 Québec · RGPD Article 22

---

#### Dimension 11 — ModelDrift (Dérive des Modèles IA)

**Logique** : Détecter quand un modèle en production se dégrade
parce que les données de production divergent des données d'entraînement.

```
1. PSI — Population Stability Index (feature drift) :
   PSI = Σ (actual_pct - expected_pct) × ln(actual_pct / expected_pct)
   V  si PSI < 0.10   (stable)
   S  si 0.10 <= PSI < 0.25  (changement modéré)
   IV si PSI >= 0.25  (retraining requis), Impact H

2. Performance Drift (dégradation métrique) :
   delta_accuracy = accuracy_production - accuracy_baseline
   V  si |delta_accuracy| < 0.02
   S  si 0.02 <= |delta_accuracy| < 0.05
   IV si |delta_accuracy| >= 0.05, Impact H

3. Prediction Distribution Drift (KL divergence) :
   KL = Σ P(x) × log(P(x) / Q(x))
   V  si KL < 0.05
   IV si KL >= 0.20

Variables surveillées :
  drift_score, accuracy, fairness_metrics par version de modèle
```

---

#### Dimension 12 — PrivacyCompliance (Conformité PII / Loi 25)

**Logique** : Détecter les données personnelles non protégées et valider
la conformité aux règles de rétention et de consentement.

```
1. Détection PII non masquée (environnements non-prod) :
   Patterns détectés :
     NAS canadien    : r"\d{3}[-\s]?\d{3}[-\s]?\d{3}"
     Email           : r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
     Téléphone CA    : r"(\+1[-\s]?)?\(?\d{3}\)?[-\s]?\d{3}[-\s]?\d{4}"
     Carte crédit    : r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b"

   → IV si PII détectée en environnement dev/staging, Impact H

2. Consentement :
   → IV si id_client dans la table de refus de consentement, Impact H

3. Rétention des données :
   age_donnee = date_today - date_creation
   → IV si age_donnee > retention_max_jours (configurable par type), Impact H

4. Pseudonymisation :
   → S si id_client non haché dans les exports analytiques, Impact M

Variables sensibles surveillées :
  age, sexe, code_postal, revenu_estime, id_client, ip_address, numero_vin
```

---

## 5. INTÉGRATIONS IA — ROADMAP COMPLÈTE

### V1 — À CONSTRUIRE MAINTENANT

#### IA-1 : Isolation Forest (Congruence ML)
**Fichier** : `findata_dq/ai/anomaly_detector.py`

```python
# Étend la dimension Congruence avec une détection multivariée
# sklearn.ensemble.IsolationForest sur les colonnes numériques

from sklearn.ensemble import IsolationForest

class MLAnomalyDetector:
    """
    Entraîne sur les données Mastered (normales) et détecte
    les outliers multivariés dans le Staged.
    
    Variables : montant_reclame, prime_annuelle, score_risque,
                nb_sinistres_historiques, delta_jours_sinistre
    
    Output : anomaly_score (-1=anomalie, +1=normal),
             contamination estimée
    """
    contamination: float = 0.05   # 5% d'anomalies attendues
    n_estimators: int = 200
```

**Quand utiliser** : données sinistres (fraude), logs de sécurité (intrusion)

---

#### IA-2 : Remédiation LLM (Claude API)
**Fichier** : `findata_dq/ai/remediation.py`

```python
# Pour chaque donnée IV, génère une suggestion de correction
# avec explication en langage naturel

REMEDIATION_PROMPT = """
Tu es un expert en qualité des données d'assurance.

Une donnée a été classifiée IV (invalide) par notre pipeline :
- Champ : {field_name}
- Valeur actuelle : {current_value}
- Dimension DQ violée : {dimension}
- Règle violée : {rule_description}
- Contexte de l'enregistrement : {record_context}

Génère en JSON strict :
{
  "explication": "pourquoi cette valeur est invalide (max 2 phrases)",
  "valeur_suggeree": "la correction la plus probable",
  "confiance": 0.0 à 1.0,
  "action_recommandee": "corriger_auto | soumettre_revision | rejeter",
  "impact_si_non_corrige": "description de l'impact métier"
}
"""
```

**Modèle** : `claude-sonnet-4-6` (défaut) ou `claude-haiku-4-5` (batch économique)
**Coût estimé** : ~$0.002 par donnée IV remédiée

---

### V2 — PROCHAINE ITÉRATION

#### IA-3 : Autoencoder (Fraude Logs)
**Fichier** : `findata_dq/ai/autoencoder.py`

```
Architecture : Encoder(128 → 64 → 32) + Decoder(32 → 64 → 128)
Entraîné sur logs normaux → reconstruction_error élevé = anomalie
Variables : montant_transaction, heure_transaction, payload_size,
            nb_actions_session, delta_ip
Framework : PyTorch ou Keras
```

---

#### IA-4 : Rapport Exécutif LLM
**Fichier** : `findata_dq/ai/report_generator.py`

```
Input  : scorecard JSON + métriques agrégées
Output : rapport .docx / email structuré pour le Comité des Risques
         → paragraphe 1 : situation globale
         → paragraphe 2 : risques critiques (IV, Impact H)
         → paragraphe 3 : actions prioritaires + ownership
Format : hebdomadaire, envoi automatique via email ou Slack webhook
```

---

#### IA-5 : Audit Fairness SHAP + LLM
**Fichier** : `findata_dq/ai/fairness_auditor.py`

```
1. SHAP values sur le modèle de scoring risque
2. Identification des features qui causent le biais (top 3)
3. Claude génère le rapport d'audit au format AI Act (Article 13)
   + recommandations de mitigation (ex: retirer code_postal du modèle)
```

---

### V3 — VISION LONG TERME

#### IA-6 : RAG DQ Assistant
```
Interface chat NL sur la scorecard :
Q : "Quelles polices ont le plus de problèmes de qualité ce mois-ci ?"
Q : "Explique-moi l'alerte Congruence sur le sinistre SIN-2024-8821"
Q : "Quel est l'impact financier estimé des IV non corrigés ?"

Stack : ChromaDB (vector store) + Claude API (RAG)
```

#### IA-7 : Predictive DQ
```
Modèle XGBoost entraîné sur l'historique des IV :
Prédit quels enregistrements vont devenir IV dans les 7 prochains jours
→ permet une correction proactive avant l'impact
```

#### IA-8 : Streaming Real-Time DQ
```
Kafka / Kinesis → validation DQ en temps réel sur les logs de sécurité
SLA : < 500ms par événement
Alerting : PagerDuty / Slack sur IV + Impact H
```

---

## 6. STRUCTURE DES DOSSIERS

```
findata-dq-engine/
├── CLAUDE.md                         ← ce fichier
├── .env                              ← jamais commité
├── .env.example
├── pyproject.toml                    ← packaging Python + deps
├── requirements.txt                  ← version fixée (pip freeze)
├── requirements-dev.txt              ← pytest, black, mypy, ruff
├── README.md                         ← avec GIF demo + badges CI
├── .gitignore
├── Dockerfile
├── docker-compose.yml
│
├── .github/
│   └── workflows/
│       ├── ci.yml                    ← lint + tests + coverage
│       └── docker.yml                ← build + push image
│
├── findata_dq/                       ← package principal
│   ├── __init__.py
│   │
│   ├── dimensions/                   ← les 12 validators
│   │   ├── __init__.py
│   │   ├── base.py                   ← classe abstraite BaseDimension
│   │   ├── completeness.py
│   │   ├── timeliness.py
│   │   ├── accuracy.py
│   │   ├── precision.py
│   │   ├── conformity.py
│   │   ├── congruence.py             ← Z-score + hook Isolation Forest
│   │   ├── collection.py
│   │   ├── cohesion.py
│   │   ├── business_rules.py         ← EXTENDED: règles inter-tables
│   │   ├── fairness.py               ← EXTENDED: biais IA
│   │   ├── model_drift.py            ← EXTENDED: PSI + KL divergence
│   │   └── privacy.py                ← EXTENDED: PII + Loi 25
│   │
│   ├── models/                       ← Pydantic v2
│   │   ├── __init__.py
│   │   ├── dq_result.py              ← DQResult, DQStatus, ImpactLevel
│   │   ├── insurance.py              ← Policy, Claim, Client, Vehicle
│   │   ├── security.py               ← AccessLog, Transaction, ThreatLog
│   │   ├── operational_risk.py       ← Incident, ESGRecord, AuditControl
│   │   ├── ai_governance.py          ← ModelMetadata, FairnessMetrics, ShapResult
│   │   └── scorecard.py              ← Scorecard, DQReport, FinancialImpact
│   │
│   ├── ai/                           ← couche intelligence artificielle
│   │   ├── __init__.py
│   │   ├── anomaly_detector.py       ← v1: Isolation Forest
│   │   ├── remediation.py            ← v1: LLM Claude remédiation
│   │   ├── report_generator.py       ← v2: rapport exécutif LLM
│   │   ├── autoencoder.py            ← v2: détection fraude logs
│   │   ├── fairness_auditor.py       ← v2: SHAP + audit AI Act
│   │   └── rag_assistant.py          ← v3: chatbot NL sur scorecard
│   │
│   ├── pipeline/
│   │   ├── __init__.py
│   │   ├── orchestrator.py           ← Raw → Staged → Mastered
│   │   ├── ingestion.py              ← chargement CSV / JSON / DB
│   │   └── remediation_engine.py     ← orchestration des corrections
│   │
│   ├── tools/
│   │   ├── __init__.py
│   │   ├── export.py                 ← JSON, CSV, Excel, PDF
│   │   └── cost_tracker.py           ← suivi coût API Claude
│   │
│   └── utils/
│       ├── __init__.py
│       ├── json_parser.py            ← parse_json_response robuste
│       └── pii_detector.py           ← regex PII multi-patterns
│
├── api/
│   ├── __init__.py
│   ├── main.py                       ← FastAPI app
│   ├── routers/
│   │   ├── validate.py               ← POST /validate
│   │   ├── scorecard.py              ← GET /scorecard/{dataset_id}
│   │   └── remediation.py            ← POST /remediate
│   └── schemas/
│       └── requests.py               ← Pydantic schemas API
│
├── dashboard/
│   └── app.py                        ← Streamlit scorecard interactive
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py                   ← fixtures pytest
│   ├── fixtures/
│   │   ├── policies_valid.csv
│   │   ├── policies_invalid.csv
│   │   ├── claims_fraud.csv
│   │   ├── access_logs.csv
│   │   └── model_metadata.csv
│   ├── unit/
│   │   └── test_dimensions/
│   │       ├── test_completeness.py
│   │       ├── test_timeliness.py
│   │       ├── test_congruence.py
│   │       ├── test_business_rules.py
│   │       ├── test_fairness.py
│   │       └── test_privacy.py
│   └── integration/
│       ├── test_pipeline_insurance.py
│       └── test_pipeline_security.py
│
└── docs/
    ├── case_study_fraud.ipynb        ← Isolation Forest sur sinistres réels
    └── case_study_bias.ipynb         ← Audit fairness sur tarification
```

---

## 7. MODÈLES PYDANTIC — SPÉCIFICATIONS COMPLÈTES

```python
# findata_dq/models/dq_result.py
from pydantic import BaseModel
from typing import Optional, Literal
from datetime import datetime

class DQStatus(str):
    VALID   = "V"    # Vert — dans la tolérance
    SUSPECT = "S"    # Jaune — à surveiller
    INVALID = "IV"   # Rouge — hors tolérance, bloqué

class ImpactLevel(str):
    HIGH   = "H"   # Impact critique (amende, perte financière)
    MEDIUM = "M"   # Processus ralenti, correction requise
    LOW    = "L"   # Anomalie mineure, système fonctionnel

class DQResult(BaseModel):
    datum_id: str                      # identifiant unique du champ testé
    dataset: str                       # "policies", "claims", "logs", etc.
    record_id: str                     # identifiant de la ligne
    field_name: str                    # nom de la colonne
    field_value: Optional[str]
    dimension: str                     # nom de la dimension (ex: "Completeness")
    status: Literal["V", "S", "IV"]
    impact: Literal["H", "M", "L"]
    score: float                       # 0.0 à 1.0
    rule_applied: str                  # description de la règle
    details: dict                      # formule, valeurs intermédiaires
    financial_impact_usd: Optional[float]  # impact $ estimé si IV
    remediation: Optional["RemediationResult"] = None
    evaluated_at: datetime

class RemediationResult(BaseModel):
    suggested_value: Optional[str]
    confidence: float
    action: Literal["auto_fix", "human_review", "reject"]
    explanation: str
    impact_if_not_fixed: str
    generated_by: str                  # "LLM" ou "rule_based"
```

```python
# findata_dq/models/insurance.py
class Policy(BaseModel):
    num_police: str
    date_effet: date
    date_expiration: date
    type_couverture: str
    prime_annuelle: float
    montant_assure: float
    id_client: str
    statut: str

class Claim(BaseModel):
    id_sinistre: str
    num_police: str
    date_sinistre: date
    montant_reclame: float
    type_dommage: str
    statut_sinistre: str
    code_postal_lieu: str
    cause_sinistre: str

class Client(BaseModel):
    id_client: str
    age: int
    sexe: str
    code_postal: str
    revenu_estime: Optional[float]
    historique_sinistres: int
    score_risque_client: float

class Vehicle(BaseModel):
    numero_vin: str
    marque_modele: str
    annee_fabrication: int
    valeur_estimee: float
    id_client: str
```

```python
# findata_dq/models/ai_governance.py
class ModelMetadata(BaseModel):
    model_id: str
    model_version: str
    training_date: date
    drift_score: float
    accuracy: float
    fairness_metrics: dict             # disparate_impact, equalized_odds, etc.
    shap_top_features: list[str]
    ai_act_compliance_flag: str
    risque_vie_privee: str
    statut_production: str

class FairnessMetrics(BaseModel):
    model_id: str
    protected_attribute: str
    disparate_impact: float
    demographic_parity: float
    equalized_odds: float
    status: Literal["V", "S", "IV"]
    violation_details: Optional[str]
```

```python
# findata_dq/models/scorecard.py
class Scorecard(BaseModel):
    dataset: str
    evaluated_at: datetime
    total_records: int
    results: list[DQResult]
    
    # Agrégats par dimension
    by_dimension: dict[str, dict]      # {"Completeness": {"V": 95, "S": 3, "IV": 2}}
    
    # Agrégats par record
    by_record: dict[str, dict]         # {record_id: {"worst_status": "IV", ...}}
    
    global_dq_score: float             # 0 à 100
    financial_impact_total_usd: float
    nb_iv_total: int
    nb_iv_high_impact: int
```

---

## 8. DONNÉES SYNTHÉTIQUES — SCHÉMAS DE FIXTURES

Les fixtures de test sont dans `tests/fixtures/`. Elles doivent couvrir :

| Fichier | Contenu | Usage |
|---|---|---|
| `policies_valid.csv` | 500 polices, toutes V | Baseline pipeline |
| `policies_invalid.csv` | 500 polices, 20% IV intentionnels | Tests dimensions |
| `claims_fraud.csv` | 200 sinistres avec patterns frauduleux | Test Isolation Forest |
| `access_logs.csv` | 10 000 logs avec 5% anomalies | Test Congruence + IA |
| `model_metadata.csv` | 20 runs modèle, drift progressif | Test ModelDrift |

**Règle** : Générer les fixtures avec `Faker` + `numpy` pour avoir des distributions réalistes.
**Script** : `tests/fixtures/generate_fixtures.py` — à lancer une seule fois.

---

## 9. VARIABLES D'ENVIRONNEMENT

```bash
# .env.example

# Anthropic — Claude API (remédiation LLM, rapports exécutifs)
ANTHROPIC_API_KEY=              # console.anthropic.com

# Modèles configurables
CLAUDE_DEFAULT_MODEL=claude-sonnet-4-6      # analyses complexes
CLAUDE_BATCH_MODEL=claude-haiku-4-5-20251001  # batch économique ($)
MAX_LLM_REMEDIATION_BATCH=50   # nb max de remédiation LLM par run

# Pipeline
PIPELINE_ENV=development        # development | staging | production
DATA_RAW_PATH=data/raw/
DATA_STAGED_PATH=data/staged/
DATA_MASTERED_PATH=data/mastered/

# Seuils DQ configurables (overrides les défauts du code)
TIMELINESS_V_DAYS=30
TIMELINESS_S_DAYS=90
ZSCORE_V_THRESHOLD=2.0
ZSCORE_IV_THRESHOLD=3.5
PSI_S_THRESHOLD=0.10
PSI_IV_THRESHOLD=0.25
FAIRNESS_IV_THRESHOLD=0.70

# Isolation Forest
ISOLATION_FOREST_CONTAMINATION=0.05
ISOLATION_FOREST_N_ESTIMATORS=200

# Export & Alerting
EXPORT_PATH=outputs/
SLACK_WEBHOOK_URL=              # optionnel — alertes IV critiques
REPORT_EMAIL_RECIPIENTS=        # optionnel — rapport hebdomadaire
```

---

## 10. INTERFACE STREAMLIT — SCORECARD INTERACTIVE

```
┌─────────────────────────────────────────────────────────────────┐
│  Buzzelli Extended — Financial & Insurance DQ Engine            │
├─────────────────────────────────────────────────────────────────┤
│  Dataset : [policies.csv ▼]  [Lancer l'analyse]                 │
│  Env     : ○ Development  ● Staging  ○ Production               │
├─────────────────────────────────────────────────────────────────┤
│  ✅ Ingestion (847 records)           0.4s                       │
│  ✅ 12 dimensions DQ                  2.1s                       │
│  ✅ Isolation Forest (fraud scan)     3.7s                       │
│  🔄 Remédiation LLM                  [████░░] 23/41 IV          │
│     Coût LLM : $0.046 / Estimé total : $0.082                   │
├─────────────────────────────────────────────────────────────────┤
│  SCORECARD HEATMAP                                              │
│                                                                 │
│                   Comp  Time  Acc  Prec  Conf  Cong  Coll  Coh │
│  Policy-001        V     V    V    V      V     S     V     V  │
│  Policy-002        V     V    IV   V      V     V     V     V  │ ← IV rouge
│  Claim-8821        V     S    V    V      IV    IV    V     V  │ ← 2 IV
│  ...                                                            │
│                                                                 │
│  Filtres : [Tous ▼] [IV seulement] [Impact H] [Fraude ML]      │
│  Trier : [Score global ▼]                                       │
├─────────────────────────────────────────────────────────────────┤
│  RÉSUMÉ EXÉCUTIF                                                │
│  Score global DQ : 87.3/100                                     │
│  IV critiques (Impact H) : 12  |  Impact financier : ~$84,000  │
│  Anomalies ML (fraude probable) : 7                             │
├─────────────────────────────────────────────────────────────────┤
│  [📄 Rapport exécutif]  [📦 Export CSV]  [🔗 API endpoint]      │
└─────────────────────────────────────────────────────────────────┘
```

---

## 11. API FASTAPI — ENDPOINTS

```python
# api/routers/validate.py

POST /validate
  Body: { "dataset": "claims", "records": [...] }
  Response: { "scorecard_id": "...", "results": [...], "summary": {...} }

POST /validate/single
  Body: { "dataset": "claims", "record": {...} }
  Response: DQResult complet avec remédiation

GET /scorecard/{scorecard_id}
  Response: Scorecard complète avec heatmap data

POST /remediate
  Body: { "dq_result_id": "...", "apply": false }
  Response: RemediationResult (suggestion ou application)

GET /health
  Response: { "status": "ok", "dimensions_loaded": 12 }
```

---

## 12. DOCKER & CI/CD

### Dockerfile
```dockerfile
FROM python:3.12-slim

WORKDIR /app

# Dépendances système pour sklearn (numpy)
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY findata_dq/ ./findata_dq/
COPY api/ ./api/
COPY dashboard/ ./dashboard/

# Créer les dossiers de données
RUN mkdir -p data/raw data/staged data/mastered outputs

EXPOSE 8000 8501

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### docker-compose.yml
```yaml
services:
  api:
    build: .
    ports: ["8000:8000"]
    env_file: .env
    volumes:
      - ./data:/app/data
      - ./outputs:/app/outputs

  dashboard:
    build: .
    command: streamlit run dashboard/app.py --server.port 8501
    ports: ["8501:8501"]
    env_file: .env
    depends_on: [api]
```

### .github/workflows/ci.yml
```yaml
name: CI Pipeline

on: [push, pull_request]

jobs:
  quality:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.12" }
      - run: pip install -r requirements-dev.txt
      - run: ruff check findata_dq/           # linting
      - run: mypy findata_dq/                  # type checking
      - run: pytest tests/unit/ -v --cov=findata_dq --cov-report=xml
      - uses: codecov/codecov-action@v4

  integration:
    runs-on: ubuntu-latest
    needs: quality
    steps:
      - uses: actions/checkout@v4
      - run: pip install -r requirements.txt
      - run: pytest tests/integration/ -v
        env:
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
          PIPELINE_ENV: staging
```

---

## 13. REQUIREMENTS

```
# requirements.txt — versions fixées pour reproductibilité

# Core DQ engine
pydantic>=2.7.0
pandas>=2.2.0
numpy>=1.26.0
scipy>=1.13.0

# ML — Isolation Forest
scikit-learn>=1.5.0

# ML — Autoencoder (v2)
# torch>=2.3.0     ← décommenter en v2

# AI — Claude API
anthropic>=0.40.0

# Fairness (v2)
# shap>=0.45.0     ← décommenter en v2
# aif360>=0.6.0    ← décommenter en v2

# API
fastapi>=0.115.0
uvicorn[standard]>=0.30.0

# Dashboard
streamlit>=1.40.0
plotly>=5.22.0

# Data synthetic (fixtures)
faker>=25.0.0

# Utils
python-dotenv>=1.0.0
tenacity>=8.3.0   # retry API calls
httpx>=0.27.0

# Dev (requirements-dev.txt)
pytest>=8.2.0
pytest-cov>=5.0.0
pytest-asyncio>=0.23.0
ruff>=0.4.0
mypy>=1.10.0
black>=24.0.0
```

---

## 14. ORDRE DE DÉVELOPPEMENT

### Étape 1 — Fondations ✅ TERMINÉE
- [x] Lire `pipeline.md` — contexte Buzzelli complet
- [x] Lire les formules des 12 dimensions dans la section 4 de ce fichier
- [x] Comprendre les modèles Pydantic de la section 7

### Étape 2 — Structure et modèles ✅ TERMINÉE
Créer : `.gitignore`, `.env.example`, `pyproject.toml`, `requirements.txt`,
tous les fichiers `models/`, `dimensions/base.py`.
**Stop — attendre validation.**

### Étape 3 — Fixtures synthétiques ✅ TERMINÉE
Coder `tests/fixtures/generate_fixtures.py`.
Générer `policies_valid.csv`, `policies_invalid.csv`, `claims_fraud.csv`.
Valider : ouvrir les CSV, vérifier les distributions avec pandas.
**Stop.**

### Étape 4 — Les 8 dimensions Buzzelli ✅ TERMINÉE
Pour chaque dimension :
1. Coder le validator dans `findata_dq/dimensions/`
2. Écrire le test unitaire dans `tests/unit/test_dimensions/`
3. Tester sur les fixtures
4. Passer à la suivante

Ordre recommandé : Completeness → Conformity → Timeliness → Cohesion → BusinessRules → Congruence → Accuracy → Precision → Collection
**Stop après les 8. Attendre validation.**

### Étape 5 — Les 4 dimensions Extended ✅ TERMINÉE
Même processus que l'étape 4.
Ordre : BusinessRules → Privacy → Fairness → ModelDrift
**Stop. Attendre validation.**

### Étape 6 — Isolation Forest (IA v1) ✅ TERMINÉE
Coder `findata_dq/ai/anomaly_detector.py`.
Tester sur `claims_fraud.csv` — les 7 sinistres frauduleux doivent être détectés.
Objectif : recall >= 85% sur les fixtures de fraude.
**Stop. Tester avant d'aller plus loin.**

### Étape 7 — Remédiation LLM (IA v1) ✅ TERMINÉE
Coder `findata_dq/ai/remediation.py`.
Tester sur 5 donnée IV maximum (coût API).
Valider que le JSON retourné est valide et le `confidence` est cohérent.
**Stop.**

### Étape 8 — Pipeline orchestrateur ✅ TERMINÉE
Coder `findata_dq/pipeline/orchestrator.py`.
Test end-to-end : `policies_invalid.csv` → Raw → Staged → résultats DQ → Mastered.
**Stop.**

### Étape 9 — Scorecard & Dashboard ✅ TERMINÉE
Coder `findata_dq/models/scorecard.py` + `dashboard/app.py`.
Lancer Streamlit et vérifier la heatmap manuellement.
**Stop.**

### Étape 10 — FastAPI ✅ TERMINÉE
Coder `api/main.py` + routers.
Tester avec `curl` ou Postman : POST /validate sur 10 records.

### Étape 11 — Docker + CI ✅ TERMINÉE
Coder `Dockerfile` + `docker-compose.yml` + `.github/workflows/ci.yml`.
Tester `docker-compose up` localement.
Vérifier que les tests passent dans GitHub Actions.

### Étape 12 — Tests & Coverage
Compléter les tests d'intégration.
Objectif : couverture >= 80%.

### Étape 13 — Case Studies (notebooks)
`docs/case_study_fraud.ipynb` : Isolation Forest sur sinistres.
`docs/case_study_bias.ipynb` : Dimension Fairness sur tarification.

---

## 15. RÈGLES DE DÉVELOPPEMENT

### Pattern de base — Classe dimension
```python
# findata_dq/dimensions/base.py
from abc import ABC, abstractmethod
from findata_dq.models.dq_result import DQResult

class BaseDimension(ABC):
    name: str
    default_impact: str = "M"

    @abstractmethod
    def validate(self, record: dict, config: dict = {}) -> list[DQResult]:
        """Retourne une liste de DQResult (un par champ testé)"""
        ...
    
    def _make_result(self, **kwargs) -> DQResult:
        """Helper pour créer un DQResult avec les champs obligatoires"""
        ...
```

### Retry sur les appels LLM
```python
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
def call_claude_remediation(prompt: str) -> dict:
    ...
```

### Parsing JSON LLM robuste
```python
# findata_dq/utils/json_parser.py
import json, re

def parse_json_response(text: str) -> dict:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            return json.loads(match.group())
        raise ValueError(f"Cannot parse JSON from LLM response: {text[:300]}")
```

### Règle de sécurité PII
```python
# Jamais logger une valeur de champ en clair si PII potentielle
# Toujours masquer dans les logs :
logger.info(f"Validating field {field_name}: [MASKED]")

# Jamais dans les messages d'erreur :
raise ValueError(f"Invalid value for {field_name}")  # PAS la valeur elle-même
```

### Tests — règle des 3 cas
Chaque dimension doit avoir un test pour :
1. Cas V (donnée valide) — doit retourner `status == "V"`
2. Cas S (suspect) — doit retourner `status == "S"`
3. Cas IV (invalide) — doit retourner `status == "IV"` avec l'impact correct

---

## 16. COÛTS API

```python
# findata_dq/tools/cost_tracker.py
COSTS_USD = {
    "claude-sonnet-4-6":           0.003,    # par 1K tokens input
    "claude-haiku-4-5-20251001":   0.00025,  # par 1K tokens input (batch)
}

# Estimation remédiation LLM :
# 50 donnée IV × 500 tokens × $0.003/1K = $0.075 par run
# Acceptable — afficher avant le lancement
```

Afficher le coût estimé de la remédiation LLM AVANT de lancer.
Afficher le coût en temps réel dans Streamlit pendant l'exécution.

---

## 17. .GITIGNORE

```
.env
.env.local
__pycache__/
*.py[cod]
.venv/
venv/
.DS_Store
data/raw/
data/staged/
data/mastered/
outputs/
*.csv
!tests/fixtures/*.csv
```

---

## 18. CHECKLIST AVANT CHAQUE SESSION CLAUDE CODE

- [ ] Ai-je relu la section 4 (les formules des dimensions) pour la dimension que je code ?
- [ ] Est-ce que mon validator hérite de `BaseDimension` ?
- [ ] Est-ce que je retourne bien `list[DQResult]` (et non un booléen) ?
- [ ] Est-ce que j'ai les 3 cas de test (V, S, IV) pour chaque dimension ?
- [ ] Est-ce que les appels LLM ont un `@retry` ?
- [ ] Est-ce que je masque les PII dans les logs ?
- [ ] Est-ce que le coût LLM est affiché avant le lancement ?
- [ ] Est-ce que `docker-compose up` tourne sans erreur ?
- [ ] Est-ce que les GitHub Actions passent en vert ?

---

*Source de vérité du projet. Mettre à jour ce fichier à chaque décision architecturale majeure.*
