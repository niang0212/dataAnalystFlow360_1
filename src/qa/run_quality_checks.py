"""
Tests de qualité "légers" pour notre pipeline.
- S'exécutent localement et (ensuite) dans GitHub Actions.
- Font échouer le job si une règle est violée (exit code != 0).

Vérifications:
1) La table PostgreSQL don_sang_clean existe et contient assez de lignes.
2) Colonnes clés non nulles: id, nom_complet, groupe_sanguin, date_naissance, date_dernier_don
3) Valeurs valides pour groupe_sanguin.
4) Optionnel: unicité de id (alerte souple: on loggue mais on n'échoue pas le job ici).
"""

import sys
import traceback
import pandas as pd
from sqlalchemy import create_engine, text
from src.db.config import get_pg_url

VALID_GROUPS = {"A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"}

def fail(msg: str):
    print(f"❌ {msg}")
    sys.exit(1)

def main():
    try:
        # 1) Connexion PostgreSQL
        pg_url = get_pg_url()
        print(f"🔗 Connexion PostgreSQL: {pg_url}")
        engine = create_engine(pg_url)

        # 2) Lire la table clean
        print("📥 Lecture: table 'don_sang_clean'...")
        df = pd.read_sql("SELECT * FROM don_sang_clean", engine)
        print(f"✔ {len(df)} lignes, {df.shape[1]} colonnes")

        # 3) Règle 1: nombre minimum de lignes
        #    (seuil pédagogique: 900 ; ajustable)
        if len(df) < 900:
            fail(f"Nombre de lignes insuffisant: {len(df)} (< 900)")

        # 4) Règle 2: colonnes clés non nulles
        required_cols = ["id", "nom_complet", "groupe_sanguin", "date_naissance", "date_dernier_don"]
        for col in required_cols:
            if col not in df.columns:
                fail(f"Colonne manquante: {col}")
            nulls = df[col].isna().sum()
            if nulls > 0:
                fail(f"Valeurs manquantes dans {col}: {nulls}")

        # 5) Règle 3: groupes sanguins valides
        invalid = df[~df["groupe_sanguin"].isin(VALID_GROUPS)]
        if not invalid.empty:
            # On échoue: ces valeurs ne devraient plus apparaître en 'clean'
            examples = invalid["groupe_sanguin"].unique()[:5]
            fail(f"Groupes sanguins invalides détectés (exemples: {list(examples)})")

        # 6) Règle 4 (souple): alerte si id dupliqués (on n'échoue pas le job)
        dup_count = df["id"].duplicated().sum()
        if dup_count > 0:
            print(f"⚠️  Alerte: {dup_count} doublon(s) sur 'id' (non bloquant).")

        print("✅ Tous les tests de qualité de base sont PASS.")
        sys.exit(0)

    except Exception as e:
        print("💥 Exception pendant les tests:")
        print(e)
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
