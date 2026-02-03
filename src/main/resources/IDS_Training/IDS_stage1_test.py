import pandas as pd
import numpy as np
import pickle
from sklearn.metrics import classification_report, confusion_matrix

# ===============================
# CONFIG
# ===============================

PICKLE_PATH = "/home/tommullier/Documents/AUT25_Projet/AUT25-VertX-NetworkAnalysis/src/main/resources/IDS_Training/IDS_Stage1_Detector.pkl"
CONFIG_PATH = "/home/tommullier/Documents/AUT25_Projet/AUT25-VertX-NetworkAnalysis/src/main/resources/IDS_Training/preprocessing_config.pkl"
TEST_CSV = "/home/tommullier/Documents/AUT25_Projet/AUT25-VertX-NetworkAnalysis/src/main/resources/IDS_Training/data/vertx_flows_test2.csv"

# ===============================
# LOAD CONFIG & MODEL
# ===============================

print("📦 Chargement configuration preprocessing...")
with open(CONFIG_PATH, "rb") as f:
    config = pickle.load(f)

BASE_FEATURES = config["BASE_FEATURES"]
DERIVED_FEATURES = config["DERIVED_FEATURES"]
FEATURES = config["FEATURES"]
log_cols = config["log_cols"]
cols_to_replace = config["cols_to_replace"]

print("📦 Chargement modèle Stage1...")
with open(PICKLE_PATH, "rb") as f:
    pipeline = pickle.load(f)

print(f"✅ Modèle chargé : Isolation Forest avec {len(FEATURES)} features")

# ===============================
# LOAD & PREPROCESS (IDENTIQUE À L'ENTRAÎNEMENT)
# ===============================

print("\n📥 Chargement trafic TEST...")
df = pd.read_csv(TEST_CSV)
initial_count = len(df)
print(f"📄 {initial_count} flows chargés")

# 🔍 Debug initial
print(f"\n🔍 Colonnes disponibles : {list(df.columns)[:15]}...")
print(f"🔍 flowDurationMs présent : {'flowDurationMs' in df.columns}")

if "flowDurationMs" in df.columns:
    print(f"🔍 Valeurs flowDurationMs (avant nettoyage) :")
    print(f"   - Count : {df['flowDurationMs'].count()}")
    print(f"   - Mean  : {df['flowDurationMs'].mean():.2f}")
    print(f"   - Min   : {df['flowDurationMs'].min():.2f}")
    print(f"   - Max   : {df['flowDurationMs'].max():.2f}")
    print(
        f"   - Zéros : {(df['flowDurationMs'] == 0).sum()} ({(df['flowDurationMs'] == 0).sum() / len(df) * 100:.1f}%)"
    )

# ===============================
# PREPROCESSING
# ===============================

print("\n🧹 Nettoyage des données...")

# 1. Convertir et nettoyer BASE_FEATURES
for col in BASE_FEATURES:
    if col not in df.columns:
        print(f"⚠️ Feature manquante ajoutée : {col}")
        df[col] = 0
    df[col] = pd.to_numeric(df[col], errors="coerce")

# 2. Nettoyer les valeurs infinies et NaN
df.replace([np.inf, -np.inf], np.nan, inplace=True)
df.fillna(0, inplace=True)

# 3. Remplacer 0 par 1 pour éviter divisions par zéro (IDENTIQUE à l'entraînement)
for col in cols_to_replace:
    if col in df.columns:
        df[col] = df[col].replace(0, 1)
    else:
        df[col] = 1

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

# Log transform (IDENTIQUE à l'entraînement)
for col in log_cols:
    X[col] = np.log1p(X[col])

# Sauvegarder X pour les stats
X_for_stats = X.copy()

print(f"✅ {len(FEATURES)} features préparées")

# ===============================
# PREDICTION
# ===============================

print("\n🚀 Analyse trafic...")

# Calcul des scores et prédictions
scores = pipeline.decision_function(X)
predictions = pipeline.predict(X)

df["stage1_score"] = scores
df["stage1_prediction"] = predictions
df["stage1_label"] = predictions
df["stage1_label"] = df["stage1_label"].map({-1: "ANOMALY", 1: "NORMAL"})

# ===============================
# STATS & MÉTRIQUES
# ===============================

print("\n" + "=" * 80)
print("📊 RÉSULTATS DE DÉTECTION")
print("=" * 80)

counts = df["stage1_label"].value_counts()
print(f"\n📊 Distribution des prédictions :")
print(counts)
print(f"\n   Taux d'anomalies : {counts.get('ANOMALY', 0) / len(df) * 100:.2f}%")

print(f"\n📊 Statistiques des scores de décision :")
print(f"   - Min     : {scores.min():.6f}")
print(f"   - Mean    : {scores.mean():.6f}")
print(f"   - Median  : {np.median(scores):.6f}")
print(f"   - Max     : {scores.max():.6f}")
print(f"   - Std     : {scores.std():.6f}")

# Percentiles
percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]
perc_values = np.percentile(scores, percentiles)
print(f"\n   Percentiles des scores :")
for p, v in zip(percentiles, perc_values):
    marker = " ← Seuil anomalie" if p == 1 else ""
    print(f"      {p:2d}% : {v:10.6f}{marker}")

# ===============================
# ÉVALUATION (SI LABELS DISPONIBLES)
# ===============================

if "label" in df.columns or "attackType" in df.columns:
    print("\n" + "=" * 80)
    print("📈 ÉVALUATION DU MODÈLE")
    print("=" * 80)

    # Mapper les labels réels
    label_col = "label" if "label" in df.columns else "attackType"
    y_true = df[label_col].map(
        lambda x: -1 if str(x).lower() in ["anomaly", "malicious", "attack"] else 1
    )
    y_pred = df["stage1_prediction"]

    # Classification report
    print("\n📋 Classification Report :")
    print(
        classification_report(
            y_true, y_pred, target_names=["NORMAL", "ANOMALY"], zero_division=0
        )
    )

    # Confusion matrix
    print("\n📊 Confusion Matrix :")
    cm = confusion_matrix(y_true, y_pred, labels=[1, -1])
    print(f"                Prédit NORMAL  Prédit ANOMALY")
    print(f"Réel NORMAL     {cm[0][0]:>13}  {cm[0][1]:>15}")
    print(f"Réel ANOMALY    {cm[1][0]:>13}  {cm[1][1]:>15}")

    # Métriques personnalisées
    tn, fp, fn, tp = cm[0][0], cm[0][1], cm[1][0], cm[1][1]
    accuracy = (tp + tn) / (tp + tn + fp + fn)
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = (
        2 * (precision * recall) / (precision + recall)
        if (precision + recall) > 0
        else 0
    )

    print(f"\n📊 Métriques détaillées :")
    print(f"   - Accuracy  : {accuracy:.4f}")
    print(f"   - Precision : {precision:.4f}")
    print(f"   - Recall    : {recall:.4f}")
    print(f"   - F1-Score  : {f1:.4f}")

# ===============================
# TOP ANOMALIES
# ===============================

anomalies = df[df["stage1_label"] == "ANOMALY"].sort_values("stage1_score")

if len(anomalies) > 0:
    print("\n" + "=" * 80)
    print(f"🚨 TOP 20 ANOMALIES (sur {len(anomalies)} détectées)")
    print("=" * 80)

    cols_to_show = [
        "srcPort",
        "dstPort",
        "protocol",
        "flowDurationMs",
        "bytesPerSecond",
        "synCount",
        "finCount",
        "stage1_score",
    ]

    # Filtrer les colonnes existantes
    available_cols = [col for col in cols_to_show if col in anomalies.columns]

    print(anomalies[available_cols].head(20).to_string(index=False))
else:
    print("\n⚠️ Aucune anomalie détectée")

# ===============================
# FEATURE COMPARISON
# ===============================

print("\n" + "=" * 80)
print("📊 COMPARAISON DES FEATURES (Normal vs Anomaly)")
print("=" * 80)

normal_count = (df["stage1_label"] == "NORMAL").sum()
anomaly_count = (df["stage1_label"] == "ANOMALY").sum()

print(f"\nNombre de flows : Normal={normal_count}, Anomaly={anomaly_count}")

if normal_count > 0 and anomaly_count > 0:
    print(f"\n{'Feature':<25} {'Normal Mean':>12} {'Anomaly Mean':>12} {'Diff':>10}")
    print("-" * 65)

    for col in FEATURES:
        if col in X_for_stats.columns:
            normal_mean = X_for_stats.loc[df["stage1_label"] == "NORMAL", col].mean()
            anomaly_mean = X_for_stats.loc[df["stage1_label"] == "ANOMALY", col].mean()
            diff = abs(anomaly_mean - normal_mean)
            print(f"{col:<25} {normal_mean:>12.4f} {anomaly_mean:>12.4f} {diff:>10.4f}")
else:
    print("\n⚠️ Impossible de comparer : tous les flows sont dans la même catégorie")

# ===============================
# ANALYSE PAR TYPE D'ATTAQUE
# ===============================

if "attackType" in df.columns:
    print("\n" + "=" * 80)
    print("🔍 ANALYSE PAR TYPE D'ATTAQUE")
    print("=" * 80)

    attack_analysis = df.groupby("attackType").agg(
        {
            "stage1_label": lambda x: (x == "ANOMALY").sum(),
            "stage1_score": ["mean", "min", "max"],
        }
    )

    attack_analysis.columns = ["Détections", "Score Moyen", "Score Min", "Score Max"]
    attack_analysis["Total"] = df.groupby("attackType").size()
    attack_analysis["Taux Détection %"] = (
        attack_analysis["Détections"] / attack_analysis["Total"] * 100
    ).round(2)

    print(attack_analysis.to_string())

# ===============================
# EXPORT RÉSULTATS
# ===============================

output_csv = TEST_CSV.replace(".csv", "_results.csv")
df.to_csv(output_csv, index=False)
print(f"\n💾 Résultats exportés : {output_csv}")

# ===============================
# RÉSUMÉ FINAL
# ===============================

print("\n" + "=" * 80)
print("✅ ANALYSE TERMINÉE")
print("=" * 80)
print(f"   - Flows analysés      : {initial_count}")
print(f"   - Flows valides       : {len(df)}")
print(
    f"   - Anomalies détectées : {anomaly_count} ({anomaly_count / len(df) * 100:.2f}%)"
)
print(f"   - Score min/max       : [{scores.min():.4f}, {scores.max():.4f}]")
print("=" * 80)
