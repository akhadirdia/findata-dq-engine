"""
Dimension IA — Isolation Forest : détection d'anomalies multivariées.

Principe
--------
- Entraînement sur les enregistrements « normaux » (fit)
- Prédiction sur tout enregistrement entrant (predict / fit_predict)
- Retourne une liste de DQResult avec status V / S / IV

Features extraites automatiquement
------------------------------------
Colonnes numériques directes :
  montant_reclame, montant_assure_police,
  prime_annuelle, score_risque, nb_sinistres_historiques

Features dérivées (calculées si colonnes sources présentes) :
  _ratio_montant_reclame_assure  = montant_reclame / montant_assure_police
  _delta_declaration_jours       = (date_declaration - date_sinistre).days
  _delta_effet_sinistre_jours    = (date_sinistre - date_effet_police).days

Seuils de décision (score anomalie = decision_function sklearn, plus élevé = plus normal)
  score >= THRESHOLD_VALID   → V
  score >= THRESHOLD_SUSPECT → S
  score <  THRESHOLD_SUSPECT → IV  (+ prédiction -1 de l'arbre)
"""

from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

import numpy as np

try:
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler
except ImportError as exc:  # pragma: no cover
    raise ImportError("scikit-learn requis : pip install scikit-learn") from exc

from findata_dq.models.dq_result import DQResult, DQStatus, ImpactLevel

# ── Paramètres par défaut ────────────────────────────────────────────────────

_DEFAULT_CONTAMINATION: float = float(os.environ.get("IF_CONTAMINATION", "0.05"))
_DEFAULT_N_ESTIMATORS: int = int(os.environ.get("IF_N_ESTIMATORS", "200"))

# Colonnes numériques natives à utiliser si présentes
_NUMERIC_COLS: list[str] = [
    "montant_reclame",
    "montant_assure_police",
    "prime_annuelle",
    "score_risque",
    "nb_sinistres_historiques",
]

# Score (decision_function sklearn) au-dessus duquel on considère normal (V)
# Valeur calibrée empiriquement sur claims_fraud avec contamination=0.05
THRESHOLD_VALID: float = float(os.environ.get("IF_THRESHOLD_VALID", "0.02"))
THRESHOLD_SUSPECT: float = float(os.environ.get("IF_THRESHOLD_SUSPECT", "-0.05"))


# ── Helpers ──────────────────────────────────────────────────────────────────

def _to_date(value: Any) -> date | None:
    """Convertit une valeur (str ISO ou date) en date, None si impossible."""
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.strip()).date()
        except ValueError:
            pass
    return None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _extract_features(records: list[dict]) -> np.ndarray:
    """
    Construit la matrice de features (n_samples × n_features).

    Pour chaque enregistrement, on extrait dans l'ordre :
    1. Les colonnes numériques natives présentes dans au moins un enregistrement
    2. _ratio_montant_reclame_assure
    3. _delta_declaration_jours
    4. _delta_effet_sinistre_jours
    """
    if not records:
        return np.empty((0, 0))

    # Colonnes numériques disponibles (intersection avec _NUMERIC_COLS)
    available_num = [c for c in _NUMERIC_COLS if any(c in r for r in records)]

    rows: list[list[float]] = []
    for r in records:
        row: list[float] = []

        # Colonnes numériques directes
        for col in available_num:
            row.append(_safe_float(r.get(col), default=0.0))

        # Feature dérivée 1 : ratio montant réclamé / montant assuré
        mr = _safe_float(r.get("montant_reclame"), 0.0)
        ma = _safe_float(r.get("montant_assure_police"), 1.0)  # éviter /0
        if "montant_reclame" in r or "montant_assure_police" in r:
            row.append(mr / max(ma, 1.0))

        # Feature dérivée 2 : délai déclaration (date_declaration - date_sinistre)
        d_sin = _to_date(r.get("date_sinistre"))
        d_dec = _to_date(r.get("date_declaration"))
        if d_sin and d_dec:
            row.append(float((d_dec - d_sin).days))

        # Feature dérivée 3 : position du sinistre dans la période d'effet
        d_eff = _to_date(r.get("date_effet_police"))
        if d_sin and d_eff:
            row.append(float((d_sin - d_eff).days))

        rows.append(row)

    return np.array(rows, dtype=float)


# ── Classe principale ─────────────────────────────────────────────────────────

class MLAnomalyDetector:
    """
    Détecteur d'anomalies multivariées basé sur Isolation Forest.

    Usage typique
    -------------
    >>> detector = MLAnomalyDetector()
    >>> detector.fit(normal_records)
    >>> results = detector.predict(new_records)

    Ou en une passe :
    >>> results = detector.fit_predict(all_records, normal_mask=[True, True, False, ...])
    """

    def __init__(
        self,
        contamination: float = _DEFAULT_CONTAMINATION,
        n_estimators: int = _DEFAULT_N_ESTIMATORS,
        random_state: int = 42,
    ) -> None:
        self.contamination = contamination
        self.n_estimators = n_estimators
        self.random_state = random_state

        self._forest: IsolationForest | None = None
        self._scaler: StandardScaler | None = None
        self._n_features: int = 0

    # ── Entraînement ────────────────────────────────────────────────────────

    def fit(self, records: list[dict]) -> "MLAnomalyDetector":
        """
        Entraîne l'Isolation Forest sur des enregistrements supposés normaux.

        Parameters
        ----------
        records : enregistrements de référence (dicts)
        """
        X = _extract_features(records)
        if X.size == 0:
            raise ValueError("Aucune feature extraite — vérifiez les colonnes des enregistrements.")

        self._n_features = X.shape[1]
        self._scaler = StandardScaler()
        X_scaled = self._scaler.fit_transform(X)

        self._forest = IsolationForest(
            contamination=self.contamination,
            n_estimators=self.n_estimators,
            random_state=self.random_state,
        )
        self._forest.fit(X_scaled)
        return self

    # ── Prédiction ──────────────────────────────────────────────────────────

    def predict(self, records: list[dict]) -> list[DQResult]:
        """
        Prédit le statut DQ de chaque enregistrement.

        Returns
        -------
        list[DQResult] — un résultat par enregistrement
        """
        if self._forest is None or self._scaler is None:
            raise RuntimeError("Le détecteur n'a pas été entraîné. Appelez fit() d'abord.")

        X = _extract_features(records)
        if X.shape[1] != self._n_features:
            raise ValueError(
                f"Nombre de features incompatible : attendu {self._n_features}, "
                f"obtenu {X.shape[1]}."
            )

        X_scaled = self._scaler.transform(X)
        scores = self._forest.decision_function(X_scaled)   # plus élevé = plus normal
        predictions = self._forest.predict(X_scaled)         # -1 = anomalie, +1 = normal

        _score_map = {DQStatus.VALID: 1.0, DQStatus.SUSPECT: 0.5, DQStatus.INVALID: 0.0}

        results: list[DQResult] = []
        for record, if_score, pred in zip(records, scores, predictions):
            record_id = str(record.get("record_id", record.get("id_sinistre", "?")))
            dataset = str(record.get("dataset", "claims"))

            if pred == -1 or if_score < THRESHOLD_SUSPECT:
                status = DQStatus.INVALID
                impact = ImpactLevel.HIGH
                rule_note = (
                    f"Anomalie multivariée détectée (score={if_score:.4f}). "
                    "Probable fraude ou erreur de données — investigation requise."
                )
            elif if_score < THRESHOLD_VALID:
                status = DQStatus.SUSPECT
                impact = ImpactLevel.MEDIUM
                rule_note = (
                    f"Profil de sinistre inhabituel (score={if_score:.4f}). "
                    "Vérification manuelle recommandée."
                )
            else:
                status = DQStatus.VALID
                impact = ImpactLevel.LOW
                rule_note = f"Profil normal (score={if_score:.4f})."

            results.append(
                DQResult(
                    datum_id=f"{record_id}_multivariate_profile_AnomalyDetection_{uuid4().hex[:6]}",
                    record_id=record_id,
                    dataset=dataset,
                    dimension="AnomalyDetection",
                    field_name="multivariate_profile",
                    field_value=None,
                    status=status,
                    impact=impact,
                    score=_score_map[status],
                    rule_applied=(
                        f"IsolationForest(contamination={self.contamination}, "
                        f"n_estimators={self.n_estimators}) — {rule_note}"
                    ),
                    details={
                        "anomaly_score": round(float(if_score), 6),
                        "if_prediction": int(pred),
                        "threshold_valid": THRESHOLD_VALID,
                        "threshold_suspect": THRESHOLD_SUSPECT,
                    },
                    evaluated_at=datetime.now(tz=timezone.utc),
                )
            )
        return results

    # ── Méthode combinée ────────────────────────────────────────────────────

    def fit_predict(
        self,
        records: list[dict],
        normal_mask: list[bool] | None = None,
    ) -> list[DQResult]:
        """
        Entraîne sur les enregistrements marqués normaux, puis prédit sur tous.

        Parameters
        ----------
        records     : tous les enregistrements
        normal_mask : booléens indiquant les lignes d'entraînement.
                      Si None, entraîne sur tous (utile pour exploration).
        """
        if normal_mask is None:
            normal_mask = [True] * len(records)

        train = [r for r, is_normal in zip(records, normal_mask) if is_normal]
        if not train:
            raise ValueError("normal_mask ne contient aucun enregistrement True.")

        self.fit(train)
        return self.predict(records)

    # ── Propriétés ──────────────────────────────────────────────────────────

    @property
    def is_fitted(self) -> bool:
        return self._forest is not None

    def anomaly_score(self, records: list[dict]) -> list[float]:
        """Retourne uniquement les scores bruts (utile pour calibration)."""
        if self._forest is None or self._scaler is None:
            raise RuntimeError("Le détecteur n'est pas entraîné.")
        X = _extract_features(records)
        X_scaled = self._scaler.transform(X)
        return self._forest.decision_function(X_scaled).tolist()
