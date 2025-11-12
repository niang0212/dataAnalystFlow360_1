import pandas as pd
from sqlalchemy import create_engine, text
from src.db.config import get_pg_url

# --------------------------------------------------
# Ce script lit la table brute "don_sang_raw"
# depuis PostgreSQL, applique un nettoyage basique,
# et sauvegarde le résultat dans "don_sang_clean".
# --------------------------------------------------

def clean_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoyage basique :
      - Suppression des groupes sanguins invalides
      - Suppression des dates manquantes
      - Suppression des noms vides
    """
    print("🔧 Nettoyage du dataset...")

    # 1. Supprimer les lignes avec groupe_sanguin invalide
    valid_groups = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
    before = len(df)
    df = df[df["groupe_sanguin"].isin(valid_groups)]
    after = len(df)
    print(f"  ➜ Lignes supprimées (groupe invalide) : {before - after}")

    # 2. Supprimer les lignes avec date_naissance ou date_dernier_don manquantes
    before = len(df)
    df = df.dropna(subset=["date_naissance", "date_dernier_don"])
    after = len(df)
    print(f"  ➜ Lignes supprimées (dates manquantes) : {before - after}")

    # 3. Supprimer les noms vides ou nuls
    before = len(df)
    df = df[df["nom_complet"].notna()]
    df = df[df["nom_complet"].str.strip() != ""]
    after = len(df)
    print(f"  ➜ Lignes supprimées (nom vide) : {before - after}")

    print(f"✅ Total final : {len(df)} lignes conservées.")
    return df

def main():
    pg_url = get_pg_url()
    engine = create_engine(pg_url)

    # --- Lire la table brute ---
    print("📥 Lecture de la table brute 'don_sang_raw'...")
    df = pd.read_sql("SELECT * FROM don_sang_raw", engine)
    print(f"✔ {len(df)} lignes lues.")

    # --- Nettoyage ---
    df_clean = clean_dataset(df)

    # --- Écriture vers la table clean ---
    print("⬆️ Chargement de la table 'don_sang_clean'...")
    df_clean.to_sql("don_sang_clean", engine, if_exists="replace", index=False)
    print("✅ Table 'don_sang_clean' créée avec succès.")

    # --- Vérification ---
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM don_sang_clean;"))
        count = result.scalar_one()
    print(f"🔍 Vérification : {count} lignes dans 'don_sang_clean'.")

if __name__ == "__main__":
    main()
