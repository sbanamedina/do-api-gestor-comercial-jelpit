# connect_sql.py
import os
import json
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from google.cloud import secretmanager
from google.cloud.sql.connector import Connector, IPTypes


# Leer entorno de ejecución
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "")
ENV = os.environ.get("ENVIRONMENT")
if ENV not in ("stage", "prod"):
    # Fallback seguro: stage si el proyecto termina en 'stage', prod de lo contrario
    ENV = "stage" if PROJECT_ID.endswith("stage") else "prod"


def get_secret(secret_id: str, project_id: str, version_id: str = "latest") -> dict:
    """
    Recupera las credenciales desde Secret Manager y las retorna como diccionario.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return json.loads(response.payload.data.decode("UTF-8"))

def get_engine():
    """
    Crea el engine de SQLAlchemy usando Google Cloud SQL Connector.
    """
    # Elegir secreto según el entorno
    if ENV == "stage":
        secret_id = "postgres-db-stage-credentials-usr_dev_stage"
        project_id = "911414108629"
    else:  # prod
        secret_id = "postgres-db-prod-credentials-api_dataops_user"
        project_id = "263751840195"

    print(f"[connect_sql] Usando secreto: {secret_id} del proyecto {project_id}")
    creds = get_secret(secret_id, project_id)

    INSTANCE_CONNECTION_NAME = creds["host"] 
    DB_USER = creds["user"]
    DB_PASS = creds["password"]
    DB_NAME = creds["database"]
    IP_TYPE = IPTypes.PRIVATE 

    connector = Connector()

    def getconn():
        """
        Función que retorna la conexión pg8000 para SQLAlchemy.
        """
        conn = connector.connect(
            INSTANCE_CONNECTION_NAME,
            "pg8000",
            user=DB_USER,
            password=DB_PASS,
            db=DB_NAME,
            ip_type=IP_TYPE
        )
        return conn

    engine = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
    )
    return engine

# Crear engine y sesión
engine = get_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """
    Dependency para FastAPI.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
