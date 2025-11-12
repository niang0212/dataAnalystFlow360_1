import os
import pandas as pd
from sqlalchemy import create_engine
from src.db.config import get_pg_url
import pandas_gbq

# --------------------------------------------------
# Ce script lit la table "don_sang_clean" dans PostgreSQL
# et la charge dans BigQuery dans une table du même nom.
#
# Pattern : PostgreSQL = staging/clean local
#           BigQuery   = entrepôt analytique cible
# --------------------------------------------------

# 1. Récupérer la config BigQuery via variables d'environnement
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")      # ex: "mon-projet-gcp"
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID", "don_sang")
BQ_TABLE_ID = f"{BQ_DATASET_ID}.don_sang_clean"  # dataset.table

def main():
    if not GCP_PROJECT_ID:
        raise ValueError("La variable d'environnement GCP_PROJECT_ID n'est pas définie.")

    # 2. Connexion à PostgreSQL
    pg_url = get_pg_url()
    engine = create_engine(pg_url)

    print("📥 Lecture de la table 'don_sang_clean' depuis PostgreSQL...")
    df = pd.read_sql("SELECT * FROM don_sang_clean", engine)
    print(f"✔ {len(df)} lignes lues.")

    # 3. Envoi vers BigQuery
    print(f"⬆️ Chargement vers BigQuery : {GCP_PROJECT_ID}.{BQ_TABLE_ID} ...")

    # if_exists="replace" : on remplace la table à chaque run (simple pour début).
    # Plus tard : on fera de l'incrémental.
    pandas_gbq.to_gbq(
        df,
        destination_table=BQ_TABLE_ID,
        project_id=GCP_PROJECT_ID,
        if_exists="replace"
    )

    print("✅ Chargement terminé dans BigQuery.")

if __name__ == "__main__":
    main()
