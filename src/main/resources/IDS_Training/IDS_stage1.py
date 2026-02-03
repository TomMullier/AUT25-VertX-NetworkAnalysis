import pandas as pd
import numpy as np
import pickle
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import RobustScaler
from sklearn.pipeline import Pipeline
from sklearn2pmml import sklearn2pmml, PMMLPipeline

# ===============================
# CONFIG
# ===============================

INPUT_CSV = "/home/tommullier/Documents/AUT25_Projet/AUT25-VertX-NetworkAnalysis/src/main/resources/IDS_Training/data/vertx_flows_normal_data.csv"
PMML_OUT = "/home/tommullier/Documents/AUT25_Projet/AUT25-VertX-NetworkAnalysis/src/main/resources/IDS_Training/IDS_Stage1_Detector.pmml"
PICKLE_OUT = "/home/tommullier/Documents/AUT25_Projet/AUT25-VertX-NetworkAnalysis/src/main/resources/IDS_Training/IDS_Stage1_Detector.pkl"

BASE_FEATURES = [
    "srcPort",
    "dstPort",
    "protocol",
    # "flowDurationMs",  # Commenté car utilisé seulement pour les calculs
    "totalPacketsUpstream",
    "totalPacketsDownstream",
    "minPacketLength",
    "maxPacketLength",
    "meanPacketLength",
    "stddevPacketLength",
    "bytesPerSecond",
    "packetsPerSecond",
    "interArrivalTimeMean",
    "interArrivalTimeStdDev",
    "interArrivalTimeMin",
    "interArrivalTimeMax",
    "ratioBytesUpDown",
    "synCount",
    "finCount",
    "rstCount",
    "ackCount",
    "pshCount",
]

DERIVED_FEATURES = [
    "avgPacketIntervalMs",
    "synFinDiff",
    "ackRatePerMs",
    "iatMaxToMeanRatio",
]

FEATURES = BASE_FEATURES + DERIVED_FEATURES

# ===============================
# LOAD DATA
# ===============================

print("📥 Chargement trafic NORMAL...")
df = pd.read_csv(INPUT_CSV)
print(f"📄 {len(df)} flows chargés")

# ===============================
# PREPROCESSING
# ===============================

print("🧹 Nettoyage des données...")

# 1. Convertir et nettoyer BASE_FEATURES
for col in BASE_FEATURES:
    if col not in df.columns:
        print(f"⚠️ Feature manquante : {col}")
        df[col] = 0
    df[col] = pd.to_numeric(df[col], errors="coerce")

# 2. Nettoyer les valeurs infinies et NaN
df.replace([np.inf, -np.inf], np.nan, inplace=True)
df.fillna(0, inplace=True)

# 3. Remplacer 0 par 1 pour éviter divisions par zéro
cols_to_replace = ["flowDurationMs", "interArrivalTimeMean"]
for col in cols_to_replace:
    if col in df.columns:
        df[col] = df[col].replace(0, 1)
    else:
        df[col] = 1  # Valeur par défaut

print(f"✅ {len(df)} flows après nettoyage")

# ===============================
# FEATURE ENGINEERING
# ===============================

print("🔧 Calcul des features dérivées...")

df["avgPacketIntervalMs"] = df["flowDurationMs"] / (
    df["totalPacketsUpstream"] + df["totalPacketsDownstream"] + 1e-6
)
df["synFinDiff"] = df["synCount"] - df["finCount"]
df["ackRatePerMs"] = df["ackCount"] / (df["flowDurationMs"] + 1e-6)
df["iatMaxToMeanRatio"] = df["interArrivalTimeMax"] / (
    df["interArrivalTimeMean"] + 1e-6
)

# Sélectionner TOUTES les features
X = df[FEATURES].copy()

# Log transform (stabiliser la variance)
log_cols = ["bytesPerSecond", "packetsPerSecond", "avgPacketIntervalMs"]
for col in log_cols:
    X[col] = np.log1p(X[col])

print(f"✅ {len(FEATURES)} features préparées")

# ===============================
# MODEL TRAINING
# ===============================

print("\n🚀 Entraînement Isolation Forest...")

# Pipeline pour Pickle (sklearn natif)
pipeline_sklearn = Pipeline(
    [
        ("scaler", RobustScaler()),
        (
            "model",
            IsolationForest(
                n_estimators=300,
                contamination=0.001,  # 0.1% d'anomalies attendues
                random_state=42,
                max_samples="auto",
                n_jobs=-1,  # Utiliser tous les CPU
            ),
        ),
    ]
)

# Pipeline pour PMML (compatible sklearn2pmml)
pipeline_pmml = PMMLPipeline(
    [
        ("scaler", RobustScaler()),
        (
            "model",
            IsolationForest(
                n_estimators=300,
                contamination=0.001,
                random_state=42,
                max_samples="auto",
            ),
        ),
    ]
)

# Entraînement
pipeline_sklearn.fit(X)
pipeline_pmml.fit(X)

print("✅ Modèle entraîné")

# ===============================
# EXPORT MODELS
# ===============================

print("\n📦 Export des modèles...")

# 1. Export Pickle (recommandé pour Python)
with open(PICKLE_OUT, "wb") as f:
    pickle.dump(pipeline_sklearn, f)
print(f"✅ Modèle Pickle exporté : {PICKLE_OUT}")

# 2. Export PMML (pour Java/production)
try:
    sklearn2pmml(pipeline_pmml, PMML_OUT, with_repr=True)
    print(f"✅ Modèle PMML exporté : {PMML_OUT}")
except Exception as e:
    print(f"⚠️ Erreur export PMML : {e}")

# ===============================
# STATISTICS
# ===============================

print("\n📊 Statistiques des features (NORMAL) :")
print("=" * 80)

stats = X.describe().T[["mean", "std", "min", "max"]]

for f in FEATURES:
    s = stats.loc[f]
    print(
        f"- {f:25s} mean={s['mean']:10.2f} "
        f"std={s['std']:10.2f} "
        f"min={s['min']:10.2f} "
        f"max={s['max']:10.2f}"
    )

# ===============================
# VALIDATION
# ===============================

print("\n📈 Scores de décision sur données d'entraînement :")
scores = pipeline_sklearn.decision_function(X)
predictions = pipeline_sklearn.predict(X)

anomalies_count = (predictions == -1).sum()
anomalies_pct = anomalies_count / len(X) * 100

print(f"  - Min score  : {scores.min():.4f}")
print(f"  - Mean score : {scores.mean():.4f}")
print(f"  - Max score  : {scores.max():.4f}")
print(f"  - Std score  : {scores.std():.4f}")
print(
    f"\n  - Anomalies détectées : {anomalies_count} / {len(X)} ({anomalies_pct:.2f}%)"
)

# Percentiles
percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]
perc_values = np.percentile(scores, percentiles)
print(f"\n  - Percentiles des scores :")
for p, v in zip(percentiles, perc_values):
    print(f"    {p:2d}% : {v:8.4f}")

# ===============================
# FINAL INFO
# ===============================

print("\n" + "=" * 80)
print("🎯 Stage 1 Detector entraîné sur trafic NORMAL")
print(f"   - {len(X)} flows")
print(f"   - {len(FEATURES)} features")
print(f"   - Contamination : 0.1%")
print(f"   - Random state : 42")
print("=" * 80)

# ===============================
# SAVE PREPROCESSING CONFIG
# ===============================

preprocessing_config = {
    "BASE_FEATURES": BASE_FEATURES,
    "DERIVED_FEATURES": DERIVED_FEATURES,
    "FEATURES": FEATURES,
    "log_cols": log_cols,
    "cols_to_replace": cols_to_replace,
}

config_path = "/home/tommullier/Documents/AUT25_Projet/AUT25-VertX-NetworkAnalysis/src/main/resources/IDS_Training/preprocessing_config.pkl"
with open(config_path, "wb") as f:
    pickle.dump(preprocessing_config, f)

print(f"\n✅ Configuration preprocessing sauvegardée : {config_path}")
print("\n✅ Entraînement terminé avec succès !")
