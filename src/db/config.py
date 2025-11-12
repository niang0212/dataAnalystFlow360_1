import os

# --------------------------------------------------
# Centralise la configuration PostgreSQL
# On lit d'abord les variables d'environnement,
# avec des valeurs par défaut pratiques pour le dev.
# --------------------------------------------------

PG_USER = os.getenv("PG_USER", "aminata")
PG_PASSWORD = os.getenv("PG_PASSWORD", "aminata")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5433")
PG_DB = os.getenv("PG_DB", "dwh_don_sang")

def get_pg_url() -> str:
    """
    Construit l'URL de connexion PostgreSQL
    au format compris par SQLAlchemy.
    """
    return f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
