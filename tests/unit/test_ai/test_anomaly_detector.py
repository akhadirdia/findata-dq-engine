"""
Tests Isolation Forest — MLAnomalyDetector.

Structure
---------
  Bloc 1 : Tests unitaires (enregistrements synthétiques)
  Bloc 2 : Test d'intégration sur claims_fraud.csv (recall >= 85 %)
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from findata_dq.ai.anomaly_detector import MLAnomalyDetector, _extract_features
from findata_dq.models.dq_result import DQStatus

# ── Fixtures shared ───────────────────────────────────────────────────────────

FIXTURES_DIR = Path(__file__).parents[3] / "tests" / "fixtures"

# Enregistrements « normaux » pour entraînement rapide dans tests unitaires
_NORMAL = [
    {
        "id_sinistre": f"SIN-N{i:03d}",
        "dataset": "claims",
        "montant_reclame": 3000.0 + i * 100,
        "montant_assure_police": 50_000.0,
        "date_sinistre": "2026-01-10",
        "date_declaration": "2026-01-12",
        "date_effet_police": "2025-06-01",
    }
    for i in range(60)
]

_OUTLIER = {
    "id_sinistre": "SIN-OUTLIER",
    "dataset": "claims",
    "montant_reclame": 980_000.0,   # montant extrême
    "montant_assure_police": 40_000.0,
    "date_sinistre": "2026-01-10",
    "date_declaration": "2026-01-12",
    "date_effet_police": "2025-06-01",
}


# ── Bloc 1 — Tests unitaires ──────────────────────────────────────────────────

class TestMLAnomalyDetectorUnit:

    def test_fit_retourne_self(self):
        detector = MLAnomalyDetector(contamination=0.05, n_estimators=50)
        result = detector.fit(_NORMAL)
        assert result is detector

    def test_is_fitted_apres_fit(self):
        detector = MLAnomalyDetector(n_estimators=50)
        assert not detector.is_fitted
        detector.fit(_NORMAL)
        assert detector.is_fitted

    def test_predict_retourne_n_resultats(self):
        detector = MLAnomalyDetector(n_estimators=50).fit(_NORMAL)
        results = detector.predict(_NORMAL[:10])
        assert len(results) == 10

    def test_statuts_valides_sur_normaux(self):
        """La majorité des normaux doivent être V ou S (pas tous IV)."""
        detector = MLAnomalyDetector(n_estimators=50).fit(_NORMAL)
        results = detector.predict(_NORMAL)
        iv_count = sum(1 for r in results if r.status == DQStatus.INVALID)
        # contamination=0.05 → environ 5% de IV maximum sur les données d'entraînement
        assert iv_count / len(results) <= 0.10  # seuil généreux = 10%

    def test_outlier_detecte_comme_invalid(self):
        """Un enregistrement avec montant × 25 doit être IV."""
        detector = MLAnomalyDetector(n_estimators=100).fit(_NORMAL)
        results = detector.predict([_OUTLIER])
        assert results[0].status == DQStatus.INVALID

    def test_result_contient_anomaly_score(self):
        detector = MLAnomalyDetector(n_estimators=50).fit(_NORMAL)
        results = detector.predict([_NORMAL[0]])
        assert "anomaly_score" in results[0].details
        assert isinstance(results[0].details["anomaly_score"], float)

    def test_fit_predict_avec_normal_mask(self):
        records = _NORMAL[:20] + [_OUTLIER]
        mask = [True] * 20 + [False]
        detector = MLAnomalyDetector(n_estimators=100)
        results = detector.fit_predict(records, normal_mask=mask)
        assert len(results) == 21
        # L'outlier doit être détecté (S ou IV) — avec 20 exemples d'entraînement
        # le modèle peut classer IV ou S selon la densité locale
        assert results[-1].status in (DQStatus.INVALID, DQStatus.SUSPECT)

    def test_fit_predict_sans_mask_entraine_sur_tout(self):
        records = _NORMAL[:20]
        detector = MLAnomalyDetector(n_estimators=50)
        results = detector.fit_predict(records)
        assert len(results) == 20

    def test_predict_avant_fit_leve_runtime_error(self):
        detector = MLAnomalyDetector()
        with pytest.raises(RuntimeError, match="entraîné"):
            detector.predict([_NORMAL[0]])

    def test_anomaly_score_retourne_floats(self):
        detector = MLAnomalyDetector(n_estimators=50).fit(_NORMAL)
        scores = detector.anomaly_score(_NORMAL[:5])
        assert len(scores) == 5
        assert all(isinstance(s, float) for s in scores)

    def test_extract_features_shape(self):
        X = _extract_features(_NORMAL[:10])
        assert X.shape[0] == 10
        assert X.shape[1] >= 3  # au moins montant_reclame, montant_assure_police, ratio

    def test_extract_features_vide(self):
        X = _extract_features([])
        assert X.size == 0


# ── Bloc 2 — Test d'intégration sur claims_fraud.csv ─────────────────────────

class TestIsolationForestFraudRecall:
    """
    Test de rappel (recall) sur le fichier claims_fraud.csv.

    Objectif : recall >= 85 % sur les MONTANT_OUTLIER_ISOLATION_FOREST.
    La fraude de type « montant outlier » est détectable par IF car elle crée
    une anomalie multivariée (ratio montant_reclame / montant_assure_police >> 1).
    """

    @pytest.fixture(scope="class")
    def fraud_data(self):
        path = FIXTURES_DIR / "claims_fraud.csv"
        if not path.exists():
            pytest.skip(f"Fixture manquante : {path}")

        records, labels = [], []
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                records.append(row)
                labels.append(row["_fraud_pattern"])
        return records, labels

    @pytest.fixture(scope="class")
    def trained_detector(self, fraud_data):
        records, labels = fraud_data
        normal_mask = [lbl == "NORMAL" for lbl in labels]
        detector = MLAnomalyDetector(contamination=0.05, n_estimators=200)
        detector.fit([r for r, m in zip(records, normal_mask) if m])
        return detector

    def test_fixture_charge(self, fraud_data):
        records, labels = fraud_data
        assert len(records) == 200
        assert labels.count("MONTANT_OUTLIER_ISOLATION_FOREST") == 40

    def test_recall_montant_outlier_85_pct(self, fraud_data, trained_detector):
        """
        Recall principal : les 40 enregistrements MONTANT_OUTLIER_ISOLATION_FOREST
        doivent être détectés (IV ou S) à au moins 85 %.
        """
        records, labels = fraud_data
        results = trained_detector.predict(records)

        detected = 0
        total_outlier = 0
        for lbl, result in zip(labels, results):
            if lbl == "MONTANT_OUTLIER_ISOLATION_FOREST":
                total_outlier += 1
                if result.status in (DQStatus.INVALID, DQStatus.SUSPECT):
                    detected += 1

        recall = detected / total_outlier
        assert recall >= 0.85, (
            f"Recall Isolation Forest insuffisant : {recall:.1%} "
            f"({detected}/{total_outlier}) — attendu >= 85 %"
        )

    def test_precision_sur_normaux(self, fraud_data, trained_detector):
        """
        Les enregistrements NORMAL ne doivent pas être marqués IV à plus de 15 %.
        (Contrôle du taux de faux positifs sur les normaux.)
        """
        records, labels = fraud_data
        results = trained_detector.predict(records)

        fp = sum(
            1 for lbl, r in zip(labels, results)
            if lbl == "NORMAL" and r.status == DQStatus.INVALID
        )
        total_normal = labels.count("NORMAL")
        fpr = fp / total_normal
        assert fpr <= 0.15, (
            f"Trop de faux positifs sur normaux : {fpr:.1%} ({fp}/{total_normal})"
        )

    def test_anomaly_scores_triables(self, fraud_data, trained_detector):
        """Les outliers ont un score moyen plus bas que les normaux."""
        records, labels = fraud_data
        scores = trained_detector.anomaly_score(records)

        outlier_scores = [s for s, l in zip(scores, labels) if l == "MONTANT_OUTLIER_ISOLATION_FOREST"]
        normal_scores  = [s for s, l in zip(scores, labels) if l == "NORMAL"]

        assert sum(outlier_scores) / len(outlier_scores) < sum(normal_scores) / len(normal_scores)

    def test_tous_resultats_ont_dimension_anomaly(self, fraud_data, trained_detector):
        records, _ = fraud_data
        results = trained_detector.predict(records)
        assert all(r.dimension == "AnomalyDetection" for r in results)
