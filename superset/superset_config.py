import os
from urllib.parse import quote_plus


SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://"
    f"{quote_plus(os.environ['POSTGRES_USER'])}:"
    f"{quote_plus(os.environ['POSTGRES_PASSWORD'])}"
    "@postgres-1:5432/superset"
)