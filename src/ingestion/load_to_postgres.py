import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import Integer, String, Float, Date, text

from src.db.config import get_pg_url

# --------------------------------------------------
# Ce script lit le fichier Parquet du Data Lake brut
# et le charge dans une table PostgreSQL "don_sang_raw".
#
# C'est notre zone RAW dans le Data Warehouse.
# --------------------------------------------------

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
PARQUET_PATH = os.path.join(BASE_DIR, "data", "lake", "raw", "don_sang_raw.parquet")

TABLE_NAME = "don_sang_raw"

def main():
    # 1) Lire les données du Data Lake brut
    print(f"📥 Lecture du Parquet : {PARQUET_PATH}")
    df = pd.read_parquet(PARQUET_PATH)

    print(f"✔ Données lues : {df.shape[0]} lignes, {df.shape[1]} colonnes")

    # 2) Créer l'engine de connexion PostgreSQL
    pg_url = get_pg_url()
    print(f"🔗 Connexion à PostgreSQL avec : {pg_url}")
    engine = create_engine(pg_url)

    # 3) Définir le schéma SQL cible pour la table
    #    On mappe chaque colonne à un type PostgreSQL adapté.
    dtype_mapping = {
        "id": Integer(),
        "nom_complet": String(255),
        "sexe": String(10),
        "date_naissance": Date(),
        "groupe_sanguin": String(5),
        "adresse": String(255),
        "latitude_region": Float(),
        "longitude_region": Float(),
        "telephone": String(50),
        "bilan_sante": String(255),
        "date_dernier_don": Date(),
    }

    # 4) Charger dans la table PostgreSQL
    # if_exists="replace" : pour le dev, on recrée la table à chaque run.
    # Plus tard : on changera la stratégie (append / incrémental).
    print(f"⬆️ Chargement dans la table PostgreSQL : {TABLE_NAME}")
    df.to_sql(
        TABLE_NAME,
        engine,
        if_exists="replace",
        index=False,
        dtype=dtype_mapping,
    )

    print("✅ Chargement terminé.")

    # 5) Contrôle rapide : compter les lignes dans la table
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME};"))
        row_count = result.scalar_one()

    print(f"🔍 Vérification : {row_count} lignes présentes dans {TABLE_NAME}.")

if __name__ == "__main__":
    main()
