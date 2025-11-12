import os
import pandas as pd

# --------------------------------------------------
# Ce script lit le CSV source et le dépose dans
# un "Data Lake brut" local au format Parquet.
#
# Objectif : avoir une source unique, cohérente,
# prête à être chargée dans PostgreSQL / BigQuery.
# --------------------------------------------------

# 1. Déterminer le chemin du projet (racine)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

# 2. Chemin vers le fichier source (zone "raw" externe)
SOURCE_CSV_PATH = os.path.join(BASE_DIR, "data", "raw", "dataset_don_sang.csv")

# 3. Dossier cible : Data Lake brut
LAKE_RAW_DIR = os.path.join(BASE_DIR, "data", "lake", "raw")
os.makedirs(LAKE_RAW_DIR, exist_ok=True)  # crée le dossier si besoin

# 4. Fichier de sortie dans le Data Lake
TARGET_PARQUET_PATH = os.path.join(LAKE_RAW_DIR, "don_sang_raw.parquet")

# Listes de référence (facultatives ici, juste pour info/contrôle léger)
VALID_SEXE = ["M", "F"]
VALID_GROUPES = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]

def standardize_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique les conversions de types minimales pour avoir
    un schéma cohérent dans le Data Lake brut.
    On ne "nettoie" pas les valeurs anormales ici.
    """

    # Conversion des dates en datetime
    df["date_naissance"] = pd.to_datetime(df["date_naissance"], errors="coerce")
    df["date_dernier_don"] = pd.to_datetime(df["date_dernier_don"], errors="coerce")

    # Conversion téléphone en texte propre
    df["telephone"] = (
        df["telephone"]
        .astype("Int64")          # gère NaN + entiers
        .astype(str)              # convertit en string
        .str.replace("<NA>", "")  # remplace les NaN textuels
        .str.strip()
    )

    # IMPORTANT : on ne filtre pas ici les groupes sanguins invalides
    # ni les dates manquantes. On les garde pour traçabilité.
    return df

def main():
    # --- 1) Charger le CSV source ---
    print(f"📥 Lecture du fichier source : {SOURCE_CSV_PATH}")
    df = pd.read_csv(SOURCE_CSV_PATH, encoding="utf-8")

    print(f"✔ Données chargées : {df.shape[0]} lignes, {df.shape[1]} colonnes")

    # --- 2) Standardiser le schéma ---
    df = standardize_schema(df)
    print("🔧 Schéma standardisé (types) :")
    print(df.dtypes)

    # --- 3) Sauvegarder dans le Data Lake brut ---
    df.to_parquet(TARGET_PARQUET_PATH, index=False)
    print(f"💾 Données enregistrées dans le Data Lake brut : {TARGET_PARQUET_PATH}")

if __name__ == "__main__":
    main()
