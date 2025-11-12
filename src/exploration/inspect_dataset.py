import os
import pandas as pd

# 1. Construire un chemin robuste vers le fichier CSV
#    (pour que le script marche peu importe d'où on l'exécute)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
CSV_PATH = os.path.join(BASE_DIR, "data", "raw", "dataset_don_sang.csv")

def main():
    # 2. Charger le fichier
    #    encoding="utf-8" est le plus courant ; à ajuster si erreur
    df = pd.read_csv(CSV_PATH, encoding="utf-8")

    print("\n✅ Fichier chargé avec succès.")
    print(f"Nombre de lignes : {len(df)}")
    print(f"Nombre de colonnes : {df.shape[1]}")

    # 3. Aperçu des données
    print("\n🔍 Aperçu des 5 premières lignes :")
    print(df.head())

    # 4. Types détectés par pandas
    print("\n📊 Types de données détectés :")
    print(df.dtypes)

    # 5. Compter les valeurs manquantes par colonne
    print("\n⚠️ Valeurs manquantes par colonne :")
    print(df.isna().sum())

if __name__ == "__main__":
    main()
