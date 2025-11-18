import os

def get_pg_url() -> str:
    """
    Construit l'URL de connexion PostgreSQL à partir des variables d'environnement.
    Si une variable n'est pas définie, on prend une valeur par défaut pour ton local.
    """
    user = os.getenv("PG_USER", "aminata")
    password = os.getenv("PG_PASSWORD", "aminata")
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5433")  # 🔴 5433 = ton port local Docker
    db = os.getenv("PG_DB", "dwh_don_sang")

    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
