import os
import pandas as pd

# 1. Définir le chemin vers le fichier brut
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
CSV_PATH = os.path.join(BASE_DIR, "data", "raw", "dataset_don_sang.csv")

# 2. Listes de référence pour quelques contrôles
VALID_SEXE = ["M", "F"]  # à adapter plus tard si besoin
VALID_GROUPES = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]

def main():
    # --- Chargement du fichier brut ---
    df = pd.read_csv(CSV_PATH, encoding="utf-8")

    print("\n=== 1) Info initiale ===")
    print(df.dtypes)

    # --- Conversion des types ---

    # a) Dates : on demande à pandas de convertir,
    #    errors='coerce' => les valeurs invalides deviendront NaT (date manquante)
    df["date_naissance"] = pd.to_datetime(df["date_naissance"], errors="coerce")
    df["date_dernier_don"] = pd.to_datetime(df["date_dernier_don"], errors="coerce")

    # b) Téléphone : convertir en texte
    #    astype("Int64") gère les NaN, puis conversion en string propre.
    df["telephone"] = (
        df["telephone"]
        .astype("Int64")              # garde les entiers + NaN
        .astype(str)                  # convertit en texte
        .str.replace("<NA>", "")      # nettoie les NaN affichés
        .str.strip()
    )

    print("\n=== 2) Types après conversion ===")
    print(df.dtypes)

    # --- Contrôles simples de qualité ---

    # 1) Valeurs manquantes
    print("\n=== 3) Valeurs manquantes par colonne ===")
    print(df.isna().sum())

    # 2) Valeurs sexe non reconnues
    invalid_sexe = df[~df["sexe"].isin(VALID_SEXE) & df["sexe"].notna()]
    print("\n=== 4) Valeurs 'sexe' non conformes (hors M/F) ===")
    print(invalid_sexe[["id", "sexe"]].head(10))

    # 3) Groupes sanguins non reconnus
    invalid_groupes = df[~df["groupe_sanguin"].isin(VALID_GROUPES) & df["groupe_sanguin"].notna()]
    print("\n=== 5) Groupes sanguins non conformes ===")
    print(invalid_groupes[["id", "groupe_sanguin"]].head(10))

    # 4) Dates de naissance ou de don invalides (NaT après conversion)
    invalid_dates_naissance = df[df["date_naissance"].isna()]
    invalid_dates_don = df[df["date_dernier_don"].isna()]

    print("\n=== 6) Lignes avec date_naissance invalide (après conversion) ===")
    print(invalid_dates_naissance[["id", "date_naissance"]].head(10))

    print("\n=== 7) Lignes avec date_dernier_don invalide (après conversion) ===")
    print(invalid_dates_don[["id", "date_dernier_don"]].head(10))

if __name__ == "__main__":
    main()
