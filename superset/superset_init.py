import os
from urllib.parse import quote_plus

from superset import db
from superset.app import create_app


app = create_app()

with app.app_context():
    from superset.models.core import Database

    security_manager = app.appbuilder.sm
    username = os.environ["SUPERSET_ADMIN_USERNAME"]
    user = security_manager.find_user(username=username)

    if user is None:
        admin_role = security_manager.find_role("Admin")
        if admin_role is None:
            raise RuntimeError("Superset Admin role was not initialized")
        security_manager.add_user(
            username=username,
            first_name=os.environ["SUPERSET_ADMIN_FIRST_NAME"],
            last_name=os.environ["SUPERSET_ADMIN_LAST_NAME"],
            email=os.environ["SUPERSET_ADMIN_EMAIL"],
            role=admin_role,
            password=os.environ["SUPERSET_ADMIN_PASSWORD"],
        )

    clickhouse_uri = (
        "clickhousedb://"
        f"{quote_plus(os.environ['CLICKHOUSE_USER'])}:"
        f"{quote_plus(os.environ['CLICKHOUSE_PASSWORD'])}"
        "@clickhouse-1:8123/default"
    )
    database = (
        db.session.query(Database)
        .filter_by(database_name="ClickHouse")
        .one_or_none()
    )
    if database is None:
        database = Database(
            database_name="ClickHouse",
            expose_in_sqllab=True,
        )
        database.set_sqlalchemy_uri(clickhouse_uri)
        db.session.add(database)
    else:
        database.set_sqlalchemy_uri(clickhouse_uri)

    db.session.commit()