"""
Générateur de fixtures synthétiques pour le pipeline findata-dq-engine.
Lancer une seule fois : python tests/fixtures/generate_fixtures.py

Fichiers générés :
  policies_valid.csv     — 500 polices, toutes V (baseline)
  policies_invalid.csv   — 500 polices, 20% IV intentionnels (tests dimensions)
  claims_fraud.csv       — 200 sinistres avec patterns frauduleux (Isolation Forest)
  access_logs.csv        — 10 000 logs avec 5% anomalies (Congruence + IA)
  model_metadata.csv     — 20 runs modèle avec drift progressif (ModelDrift)
"""

from __future__ import annotations

import csv
import os
import random
import re
import string
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
from faker import Faker
from faker.providers import internet

# ─── Configuration ────────────────────────────────────────────────────────────

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

fake = Faker("fr_CA")
fake.add_provider(internet)
Faker.seed(SEED)

OUTPUT_DIR = Path(__file__).parent
TODAY = date.today()


# ─── Helpers ──────────────────────────────────────────────────────────────────

def rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, max(delta, 0)))


def rand_policy_num() -> str:
    prefix = random.choice(["AU", "HA", "VIE", "SA", "EN"])
    num = random.randint(100000, 9999999)
    return f"{prefix}-{num}"


def rand_vin() -> str:
    chars = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"  # exclut I, O, Q
    return "".join(random.choices(chars, k=17))


def rand_postal_ca() -> str:
    letters = "ABCEGHJKLMNPRSTVXY"
    l1 = random.choice(letters)
    d1 = random.randint(0, 9)
    l2 = random.choice(string.ascii_uppercase)
    d2 = random.randint(0, 9)
    l3 = random.choice(string.ascii_uppercase)
    d3 = random.randint(0, 9)
    return f"{l1}{d1}{l2} {d2}{l3}{d3}"


def rand_ip() -> str:
    return f"{random.randint(1,254)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"


def rand_client_id() -> str:
    return "CLI-" + "".join(random.choices(string.digits, k=7))


def rand_sinistre_id(year: int = 2024) -> str:
    return f"SIN-{year}-" + "".join(random.choices(string.digits, k=6))


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  [OK] {path.name} -- {len(rows)} lignes")


# ─── 1. policies_valid.csv ────────────────────────────────────────────────────

def generate_policies_valid(n: int = 500) -> list[dict]:
    """
    500 polices, 100% valides.
    Toutes les dimensions DQ doivent retourner V sur ce fichier.
    """
    types = ["auto", "habitation", "vie", "sante", "entreprise"]
    statuts = ["active", "active", "active", "active", "suspendue"]  # majorité active

    records = []
    for i in range(n):
        client_id = rand_client_id()
        num_police = rand_policy_num()
        effet = rand_date(TODAY - timedelta(days=60), TODAY - timedelta(days=10))
        expiration = effet + timedelta(days=random.choice([365, 730, 1095]))
        prime = round(np.random.lognormal(mean=7.5, sigma=0.4), 2)  # ~1 800 CAD médiane
        montant_assure = round(prime * random.uniform(10, 40), 2)
        records.append({
            "num_police": num_police,
            "id_client": client_id,
            "date_effet": effet.isoformat(),
            "date_expiration": expiration.isoformat(),
            "type_couverture": random.choice(types),
            "prime_annuelle": prime,
            "montant_assure": montant_assure,
            "statut_police": random.choice(statuts),
            "franchise": round(random.choice([0, 250, 500, 1000, 2500]), 2),
            "date_creation": (TODAY - timedelta(days=random.randint(1, 90))).isoformat(),
        })
    return records


# ─── 2. policies_invalid.csv ─────────────────────────────────────────────────

def generate_policies_invalid(n: int = 500) -> list[dict]:
    """
    500 polices avec exactement 20% (100 enregistrements) de défauts IV répartis :
      - 20 × Completeness IV  : num_police null ou prime_annuelle null
      - 20 × Timeliness IV    : date_creation > 90 jours (données périmées)
      - 20 × Conformity IV    : num_police format invalide
      - 20 × Congruence IV    : prime_annuelle outlier (Z-score > 3.5)
      - 20 × BusinessRules IV : date_expiration < date_effet (incohérence)
    Les 400 restants sont valides (même logique que policies_valid).
    """
    types = ["auto", "habitation", "vie", "sante", "entreprise"]
    records = []

    # 400 valides
    for i in range(400):
        client_id = rand_client_id()
        effet = rand_date(TODAY - timedelta(days=60), TODAY - timedelta(days=10))
        expiration = effet + timedelta(days=random.choice([365, 730]))
        prime = round(np.random.lognormal(mean=7.5, sigma=0.4), 2)
        records.append({
            "num_police": rand_policy_num(),
            "id_client": client_id,
            "date_effet": effet.isoformat(),
            "date_expiration": expiration.isoformat(),
            "type_couverture": random.choice(types),
            "prime_annuelle": prime,
            "montant_assure": round(prime * random.uniform(10, 40), 2),
            "statut_police": "active",
            "franchise": round(random.choice([0, 250, 500, 1000]), 2),
            "date_creation": (TODAY - timedelta(days=random.randint(1, 89))).isoformat(),
            "_defect": "NONE",
        })

    # 20 × Completeness IV — champ obligatoire manquant
    for i in range(20):
        effet = rand_date(TODAY - timedelta(days=60), TODAY - timedelta(days=10))
        expiration = effet + timedelta(days=365)
        prime = round(np.random.lognormal(mean=7.5, sigma=0.4), 2)
        row = {
            "num_police": rand_policy_num() if i < 10 else "",  # alternance null/vide
            "id_client": rand_client_id(),
            "date_effet": effet.isoformat(),
            "date_expiration": expiration.isoformat(),
            "type_couverture": random.choice(types),
            "prime_annuelle": "" if i >= 10 else prime,  # null sur prime_annuelle
            "montant_assure": round(prime * 20, 2),
            "statut_police": "active",
            "franchise": 500,
            "date_creation": (TODAY - timedelta(days=10)).isoformat(),
            "_defect": "COMPLETENESS_IV",
        }
        records.append(row)

    # 20 × Timeliness IV — date_creation > 90 jours
    for i in range(20):
        effet = rand_date(TODAY - timedelta(days=200), TODAY - timedelta(days=120))
        expiration = effet + timedelta(days=365)
        prime = round(np.random.lognormal(mean=7.5, sigma=0.4), 2)
        records.append({
            "num_police": rand_policy_num(),
            "id_client": rand_client_id(),
            "date_effet": effet.isoformat(),
            "date_expiration": expiration.isoformat(),
            "type_couverture": random.choice(types),
            "prime_annuelle": prime,
            "montant_assure": round(prime * 20, 2),
            "statut_police": "active",
            "franchise": 500,
            "date_creation": (TODAY - timedelta(days=random.randint(91, 365))).isoformat(),
            "_defect": "TIMELINESS_IV",
        })

    # 20 × Conformity IV — format num_police invalide
    bad_formats = [
        "123456", "police-abc", "XX", "AU_123456", "AU123456",
        "ZZ-0000000001", "AUTO-1", "12-345678", "AB-1234", "XXXX-12345"
    ]
    for i in range(20):
        effet = rand_date(TODAY - timedelta(days=30), TODAY - timedelta(days=5))
        expiration = effet + timedelta(days=365)
        prime = round(np.random.lognormal(mean=7.5, sigma=0.4), 2)
        records.append({
            "num_police": bad_formats[i % len(bad_formats)],
            "id_client": rand_client_id(),
            "date_effet": effet.isoformat(),
            "date_expiration": expiration.isoformat(),
            "type_couverture": random.choice(types),
            "prime_annuelle": prime,
            "montant_assure": round(prime * 20, 2),
            "statut_police": "active",
            "franchise": 500,
            "date_creation": (TODAY - timedelta(days=10)).isoformat(),
            "_defect": "CONFORMITY_IV",
        })

    # 20 × Congruence IV — prime_annuelle outlier (> 3.5 × écart-type)
    # Moyenne log-normale ~1 800 CAD, std ~800 → outliers > 8 000 CAD ou < 50 CAD
    outlier_primes = (
        [round(random.uniform(80_000, 250_000), 2) for _ in range(10)]  # très hautes
        + [round(random.uniform(1, 10), 2) for _ in range(10)]          # très basses
    )
    for prime in outlier_primes:
        effet = rand_date(TODAY - timedelta(days=30), TODAY - timedelta(days=5))
        expiration = effet + timedelta(days=365)
        records.append({
            "num_police": rand_policy_num(),
            "id_client": rand_client_id(),
            "date_effet": effet.isoformat(),
            "date_expiration": expiration.isoformat(),
            "type_couverture": random.choice(types),
            "prime_annuelle": prime,
            "montant_assure": round(prime * 20, 2),
            "statut_police": "active",
            "franchise": 500,
            "date_creation": (TODAY - timedelta(days=10)).isoformat(),
            "_defect": "CONGRUENCE_IV",
        })

    # 20 × BusinessRules IV — date_expiration < date_effet
    for i in range(20):
        effet = rand_date(TODAY - timedelta(days=30), TODAY - timedelta(days=5))
        expiration = effet - timedelta(days=random.randint(1, 100))  # incohérence !
        prime = round(np.random.lognormal(mean=7.5, sigma=0.4), 2)
        records.append({
            "num_police": rand_policy_num(),
            "id_client": rand_client_id(),
            "date_effet": effet.isoformat(),
            "date_expiration": expiration.isoformat(),  # antérieure à effet → IV
            "type_couverture": random.choice(types),
            "prime_annuelle": prime,
            "montant_assure": round(prime * 20, 2),
            "statut_police": "active",
            "franchise": 500,
            "date_creation": (TODAY - timedelta(days=10)).isoformat(),
            "_defect": "BUSINESS_RULES_IV",
        })

    random.shuffle(records)
    return records


# ─── 3. claims_fraud.csv ──────────────────────────────────────────────────────

def generate_claims_fraud(n: int = 200) -> list[dict]:
    """
    200 sinistres dont 7 patterns de fraude distincts pour l'Isolation Forest.
    Distribution des patterns :
      - 30 × montant_reclame > montant_assure (BusinessRules R2)
      - 30 × date_sinistre hors période de couverture (BusinessRules R1)
      - 30 × sinistre ouvert sur police expirée (BusinessRules R3)
      - 40 × montant_reclame outlier Z-score > 3.5 (Congruence + fraude)
      - 70 × sinistres normaux (baseline)
    """
    types_dommage = [
        "collision", "vol", "incendie", "degats_eau", "bris_glace",
        "responsabilite_civile", "vandalisme"
    ]
    causes = [
        "Accident de la route", "Vol du véhicule", "Incendie accidentel",
        "Dégâts des eaux", "Bris de glace", "Collision avec tiers",
        "Vandalisme dans parking"
    ]
    records = []

    # 70 sinistres normaux
    for i in range(70):
        montant_assure = round(np.random.lognormal(mean=10, sigma=0.5), 2)
        montant_reclame = round(montant_assure * random.uniform(0.01, 0.3), 2)
        effet = rand_date(TODAY - timedelta(days=365), TODAY - timedelta(days=60))
        expiration = effet + timedelta(days=365)
        date_sin = rand_date(effet, TODAY - timedelta(days=5))
        records.append({
            "id_sinistre": rand_sinistre_id(),
            "num_police": rand_policy_num(),
            "id_client": rand_client_id(),
            "date_sinistre": date_sin.isoformat(),
            "date_declaration": (date_sin + timedelta(days=random.randint(1, 15))).isoformat(),
            "montant_reclame": montant_reclame,
            "montant_rembourse": round(montant_reclame * random.uniform(0.7, 1.0), 2),
            "type_dommage": random.choice(types_dommage),
            "cause_sinistre": random.choice(causes),
            "statut_sinistre": random.choice(["paye", "ferme", "en_cours"]),
            "code_postal_lieu": rand_postal_ca(),
            "expert_assigne": f"EXP-{random.randint(100,999)}",
            "montant_assure_police": montant_assure,
            "date_effet_police": effet.isoformat(),
            "date_expiration_police": expiration.isoformat(),
            "_fraud_pattern": "NORMAL",
        })

    # 30 × montant_reclame > montant_assure (BR-R2)
    for i in range(30):
        montant_assure = round(random.uniform(5_000, 50_000), 2)
        montant_reclame = round(montant_assure * random.uniform(1.5, 5.0), 2)  # dépasse !
        effet = rand_date(TODAY - timedelta(days=300), TODAY - timedelta(days=60))
        expiration = effet + timedelta(days=365)
        date_sin = rand_date(effet, TODAY - timedelta(days=5))
        records.append({
            "id_sinistre": rand_sinistre_id(),
            "num_police": rand_policy_num(),
            "id_client": rand_client_id(),
            "date_sinistre": date_sin.isoformat(),
            "date_declaration": (date_sin + timedelta(days=random.randint(1, 3))).isoformat(),
            "montant_reclame": montant_reclame,
            "montant_rembourse": None,
            "type_dommage": random.choice(types_dommage),
            "cause_sinistre": random.choice(causes),
            "statut_sinistre": "ouvert",
            "code_postal_lieu": rand_postal_ca(),
            "expert_assigne": None,
            "montant_assure_police": montant_assure,
            "date_effet_police": effet.isoformat(),
            "date_expiration_police": expiration.isoformat(),
            "_fraud_pattern": "MONTANT_DEPASSE_ASSURE",
        })

    # 30 × date_sinistre hors période de couverture (BR-R1)
    for i in range(30):
        montant_assure = round(random.uniform(10_000, 80_000), 2)
        montant_reclame = round(montant_assure * random.uniform(0.05, 0.25), 2)
        effet = rand_date(TODAY - timedelta(days=365), TODAY - timedelta(days=180))
        expiration = effet + timedelta(days=180)  # police expirée depuis > 30 jours
        # sinistre APRÈS expiration → hors couverture
        date_sin = expiration + timedelta(days=random.randint(10, 90))
        records.append({
            "id_sinistre": rand_sinistre_id(),
            "num_police": rand_policy_num(),
            "id_client": rand_client_id(),
            "date_sinistre": date_sin.isoformat(),
            "date_declaration": (date_sin + timedelta(days=1)).isoformat(),
            "montant_reclame": montant_reclame,
            "montant_rembourse": None,
            "type_dommage": random.choice(types_dommage),
            "cause_sinistre": random.choice(causes),
            "statut_sinistre": "ouvert",
            "code_postal_lieu": rand_postal_ca(),
            "expert_assigne": None,
            "montant_assure_police": montant_assure,
            "date_effet_police": effet.isoformat(),
            "date_expiration_police": expiration.isoformat(),
            "_fraud_pattern": "DATE_HORS_COUVERTURE",
        })

    # 30 × sinistre ouvert sur police expirée (BR-R3)
    for i in range(30):
        montant_assure = round(random.uniform(10_000, 80_000), 2)
        montant_reclame = round(montant_assure * 0.1, 2)
        effet = rand_date(TODAY - timedelta(days=730), TODAY - timedelta(days=400))
        expiration = effet + timedelta(days=365)  # expirée
        date_sin = rand_date(effet, expiration - timedelta(days=10))
        records.append({
            "id_sinistre": rand_sinistre_id(),
            "num_police": rand_policy_num(),
            "id_client": rand_client_id(),
            "date_sinistre": date_sin.isoformat(),
            "date_declaration": (date_sin + timedelta(days=2)).isoformat(),
            "montant_reclame": montant_reclame,
            "montant_rembourse": None,
            "type_dommage": random.choice(types_dommage),
            "cause_sinistre": random.choice(causes),
            "statut_sinistre": "ouvert",   # ouvert mais police expirée → IV
            "code_postal_lieu": rand_postal_ca(),
            "expert_assigne": None,
            "montant_assure_police": montant_assure,
            "date_effet_police": effet.isoformat(),
            "date_expiration_police": expiration.isoformat(),
            "_fraud_pattern": "SINISTRE_OUVERT_POLICE_EXPIREE",
        })

    # 40 × montants outlier (Congruence + Isolation Forest fraude)
    for i in range(40):
        montant_assure = round(random.uniform(20_000, 100_000), 2)
        # montants extrêmes : 200k-500k sur des polices de 20k-100k
        montant_reclame = round(random.uniform(200_000, 500_000), 2)
        effet = rand_date(TODAY - timedelta(days=300), TODAY - timedelta(days=60))
        expiration = effet + timedelta(days=365)
        date_sin = rand_date(effet, TODAY - timedelta(days=5))
        records.append({
            "id_sinistre": rand_sinistre_id(),
            "num_police": rand_policy_num(),
            "id_client": rand_client_id(),
            "date_sinistre": date_sin.isoformat(),
            "date_declaration": (date_sin + timedelta(days=random.randint(0, 2))).isoformat(),
            "montant_reclame": montant_reclame,
            "montant_rembourse": None,
            "type_dommage": "incendie",
            "cause_sinistre": "Incendie d'origine inconnue",
            "statut_sinistre": "ouvert",
            "code_postal_lieu": rand_postal_ca(),
            "expert_assigne": None,
            "montant_assure_police": montant_assure,
            "date_effet_police": effet.isoformat(),
            "date_expiration_police": expiration.isoformat(),
            "_fraud_pattern": "MONTANT_OUTLIER_ISOLATION_FOREST",
        })

    random.shuffle(records)
    return records


# ─── 4. access_logs.csv ───────────────────────────────────────────────────────

def generate_access_logs(n: int = 10_000) -> list[dict]:
    """
    10 000 logs avec 5% d'anomalies (500 enregistrements) :
      - 100 × delete sans session_id active (BusinessRules R4)
      - 100 × montant_transaction > 50k avec status != 200 (BusinessRules R5)
      - 100 × Z-score payload_size extrême (Congruence)
      - 100 × IP privée inexistante (Conformity)
      - 100 × login massif 401 (brute force — Isolation Forest)
    """
    actions = ["login", "logout", "read", "write", "delete", "transfer", "export"]
    action_weights = [0.20, 0.18, 0.35, 0.15, 0.05, 0.04, 0.03]
    devices = ["web", "mobile", "api", "desktop"]
    status_normal = [200, 200, 200, 200, 201, 204, 301, 304]

    now = datetime.utcnow()
    records = []

    # 9 500 logs normaux
    for i in range(9_500):
        action = random.choices(actions, weights=action_weights)[0]
        ts = now - timedelta(minutes=random.randint(0, 59))
        records.append({
            "log_id": f"LOG-{i+1:07d}",
            "timestamp": ts.isoformat(),
            "user_id": f"USR-{random.randint(1000, 9999)}",
            "ip_address": rand_ip(),
            "action_type": action,
            "session_id": f"SES-{random.randint(10000, 99999)}" if action != "logout" else None,
            "device_type": random.choice(devices),
            "status_code": random.choice(status_normal),
            "payload_size": random.randint(100, 50_000),
            "anomaly_score": None,
            "_anomaly_pattern": "NORMAL",
        })

    # 100 × delete sans session → BusinessRules R4
    for i in range(100):
        ts = now - timedelta(minutes=random.randint(0, 59))
        records.append({
            "log_id": f"LOG-ANOM-DEL-{i+1:04d}",
            "timestamp": ts.isoformat(),
            "user_id": f"USR-{random.randint(1000, 9999)}",
            "ip_address": rand_ip(),
            "action_type": "delete",
            "session_id": None,    # ← violation R4 : delete sans session
            "device_type": random.choice(devices),
            "status_code": 200,
            "payload_size": random.randint(100, 5_000),
            "anomaly_score": None,
            "_anomaly_pattern": "DELETE_SANS_SESSION",
        })

    # 100 × transfer montant > 50k avec status != 200 → BusinessRules R5
    for i in range(100):
        ts = now - timedelta(minutes=random.randint(0, 59))
        records.append({
            "log_id": f"LOG-ANOM-TRF-{i+1:04d}",
            "timestamp": ts.isoformat(),
            "user_id": f"USR-{random.randint(1000, 9999)}",
            "ip_address": rand_ip(),
            "action_type": "transfer",
            "session_id": f"SES-{random.randint(10000, 99999)}",
            "device_type": "api",
            "status_code": random.choice([401, 403, 500, 503]),  # != 200 → S/IV
            "payload_size": random.randint(1_000, 20_000),
            "anomaly_score": None,
            "_anomaly_pattern": "TRANSFER_HIGH_VALUE_FAILED",
        })

    # 100 × payload_size outlier Z-score > 3.5 (Congruence)
    for i in range(100):
        ts = now - timedelta(minutes=random.randint(0, 59))
        records.append({
            "log_id": f"LOG-ANOM-PLD-{i+1:04d}",
            "timestamp": ts.isoformat(),
            "user_id": f"USR-{random.randint(1000, 9999)}",
            "ip_address": rand_ip(),
            "action_type": random.choice(["export", "read"]),
            "session_id": f"SES-{random.randint(10000, 99999)}",
            "device_type": "api",
            "status_code": 200,
            "payload_size": random.randint(500_000, 10_000_000),  # outlier
            "anomaly_score": None,
            "_anomaly_pattern": "PAYLOAD_OUTLIER",
        })

    # 100 × format IP invalide (Conformity)
    bad_ips = [
        "999.999.999.999", "256.1.1.1", "192.168.1",
        "abc.def.ghi.jkl", "0.0.0.0", "localhost",
        "300.1.1.1", "192.168.1.1.1", "1.2.3", "::1",
    ]
    for i in range(100):
        ts = now - timedelta(minutes=random.randint(0, 59))
        records.append({
            "log_id": f"LOG-ANOM-IP-{i+1:04d}",
            "timestamp": ts.isoformat(),
            "user_id": f"USR-{random.randint(1000, 9999)}",
            "ip_address": bad_ips[i % len(bad_ips)],   # format invalide
            "action_type": random.choice(actions),
            "session_id": f"SES-{random.randint(10000, 99999)}",
            "device_type": random.choice(devices),
            "status_code": 200,
            "payload_size": random.randint(100, 50_000),
            "anomaly_score": None,
            "_anomaly_pattern": "IP_FORMAT_INVALID",
        })

    # 100 × brute force : login répétés avec 401 (Isolation Forest)
    for i in range(100):
        # concentrés sur 5 user_id suspects
        ts = now - timedelta(seconds=random.randint(0, 3600))
        records.append({
            "log_id": f"LOG-ANOM-BRF-{i+1:04d}",
            "timestamp": ts.isoformat(),
            "user_id": f"USR-{random.choice([1111, 2222, 3333, 4444, 5555])}",
            "ip_address": random.choice(["185.220.101.1", "94.102.49.1", "185.220.102.1"]),
            "action_type": "login",
            "session_id": None,
            "device_type": "api",
            "status_code": 401,   # échec authentification répété → brute force
            "payload_size": random.randint(200, 800),
            "anomaly_score": None,
            "_anomaly_pattern": "BRUTE_FORCE_LOGIN",
        })

    random.shuffle(records)
    return records


# ─── 5. model_metadata.csv ────────────────────────────────────────────────────

def generate_model_metadata(n: int = 20) -> list[dict]:
    """
    20 runs du modèle 'scoring_risque_auto' avec drift progressif.
    Les 8 premiers runs : V (PSI < 0.10).
    Runs 9-14 : S (0.10 <= PSI < 0.25).
    Runs 15-20 : IV (PSI >= 0.25 → retraining requis).
    Accuracy dégrade en parallèle.
    """
    records = []
    base_accuracy = 0.92
    base_date = TODAY - timedelta(days=n * 14)  # un run tous les 14 jours

    for i in range(n):
        run_num = i + 1
        run_date = base_date + timedelta(days=i * 14)
        deployment_date = run_date + timedelta(days=random.randint(1, 5))

        # PSI croissant au fil du temps (drift simulé)
        if run_num <= 8:
            psi = round(random.uniform(0.01, 0.09), 4)          # V
            drift_status = "V"
            statut_prod = "production"
        elif run_num <= 14:
            psi = round(random.uniform(0.10, 0.24), 4)          # S
            drift_status = "S"
            statut_prod = "production"
        else:
            psi = round(random.uniform(0.25, 0.55), 4)          # IV
            drift_status = "IV"
            statut_prod = "production" if run_num <= 17 else "retire"

        # Accuracy dégrade corrélée au drift
        noise = random.uniform(-0.005, 0.005)
        accuracy = round(max(0.60, base_accuracy - (psi * 0.8) + noise), 4)
        delta_accuracy = round(accuracy - base_accuracy, 4)

        # Fairness : disparate_impact se dégrade aussi
        if drift_status == "V":
            di = round(random.uniform(0.82, 1.20), 4)
            compliance = "compliant"
        elif drift_status == "S":
            di = round(random.uniform(0.72, 0.82), 4)
            compliance = "under_review"
        else:
            di = round(random.uniform(0.55, 0.72), 4)
            compliance = "non_compliant"

        version_major = 2
        version_minor = run_num // 5
        version_patch = run_num % 5
        model_version = f"{version_major}.{version_minor}.{version_patch}"

        records.append({
            "model_id": f"MDL-{run_num:03d}",
            "model_name": "scoring_risque_auto",
            "model_version": model_version,
            "training_date": (run_date - timedelta(days=7)).isoformat(),
            "deployment_date": deployment_date.isoformat(),
            "statut_production": statut_prod,
            "accuracy": accuracy,
            "precision": round(accuracy + random.uniform(-0.02, 0.02), 4),
            "recall": round(accuracy + random.uniform(-0.03, 0.03), 4),
            "f1_score": round(accuracy + random.uniform(-0.01, 0.01), 4),
            "auc_roc": round(min(0.99, accuracy + random.uniform(0.02, 0.05)), 4),
            "drift_score": psi,
            "drift_status": drift_status,
            "last_drift_check": run_date.isoformat(),
            "delta_accuracy_vs_baseline": delta_accuracy,
            "disparate_impact_sexe": di,
            "ai_act_compliance_flag": compliance,
            "risque_vie_privee": "modere" if di > 0.80 else "eleve",
            "features_sensibles": "age,sexe,code_postal",
            "shap_top_features": "historique_sinistres,age,prime_annuelle,code_postal,type_couverture",
            "owner_equipe": "data-science-actuariat",
            "date_creation": run_date.isoformat(),
        })

    return records


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("\n[*] Generation des fixtures synthetiques -- findata-dq-engine")
    print(f"   Repertoire de sortie : {OUTPUT_DIR.resolve()}\n")

    # --- policies_valid.csv ---
    rows = generate_policies_valid(500)
    _write_csv(
        OUTPUT_DIR / "policies_valid.csv",
        rows,
        fieldnames=[
            "num_police", "id_client", "date_effet", "date_expiration",
            "type_couverture", "prime_annuelle", "montant_assure",
            "statut_police", "franchise", "date_creation",
        ],
    )

    # --- policies_invalid.csv ---
    rows = generate_policies_invalid(500)
    _write_csv(
        OUTPUT_DIR / "policies_invalid.csv",
        rows,
        fieldnames=[
            "num_police", "id_client", "date_effet", "date_expiration",
            "type_couverture", "prime_annuelle", "montant_assure",
            "statut_police", "franchise", "date_creation", "_defect",
        ],
    )

    # --- claims_fraud.csv ---
    rows = generate_claims_fraud(200)
    _write_csv(
        OUTPUT_DIR / "claims_fraud.csv",
        rows,
        fieldnames=[
            "id_sinistre", "num_police", "id_client", "date_sinistre",
            "date_declaration", "montant_reclame", "montant_rembourse",
            "type_dommage", "cause_sinistre", "statut_sinistre",
            "code_postal_lieu", "expert_assigne",
            "montant_assure_police", "date_effet_police", "date_expiration_police",
            "_fraud_pattern",
        ],
    )

    # --- access_logs.csv ---
    rows = generate_access_logs(10_000)
    _write_csv(
        OUTPUT_DIR / "access_logs.csv",
        rows,
        fieldnames=[
            "log_id", "timestamp", "user_id", "ip_address", "action_type",
            "session_id", "device_type", "status_code", "payload_size",
            "anomaly_score", "_anomaly_pattern",
        ],
    )

    # --- model_metadata.csv ---
    rows = generate_model_metadata(20)
    _write_csv(
        OUTPUT_DIR / "model_metadata.csv",
        rows,
        fieldnames=[
            "model_id", "model_name", "model_version", "training_date",
            "deployment_date", "statut_production", "accuracy", "precision",
            "recall", "f1_score", "auc_roc", "drift_score", "drift_status",
            "last_drift_check", "delta_accuracy_vs_baseline",
            "disparate_impact_sexe", "ai_act_compliance_flag", "risque_vie_privee",
            "features_sensibles", "shap_top_features", "owner_equipe", "date_creation",
        ],
    )

    print("\n[DONE] Toutes les fixtures sont generees.")
    print("   Prochaine etape : valider les distributions avec pandas.")
    print("   Commande : python -c \"import pandas as pd; print(pd.read_csv('tests/fixtures/policies_invalid.csv')['_defect'].value_counts())\"")


if __name__ == "__main__":
    main()
