import os
import subprocess
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import quote_plus

import yaml

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
    print("Registered ClickHouse", flush=True)

    old_postgres_database = (
        db.session.query(Database)
        .filter_by(database_name="Traffic Postgres")
        .one_or_none()
    )
    if old_postgres_database is not None:
        db.session.delete(old_postgres_database)
        db.session.commit()
        print("Removed Traffic Postgres data source", flush=True)

    database_uuids = {
        "CLICKHOUSE_DATABASE_UUID": str(database.uuid),
    }
    assets_dir = Path("/app/dashboard-assets")
    with tempfile.TemporaryDirectory() as temp_dir:
        bundle_path = Path(temp_dir) / "traffic-operations.zip"
        with zipfile.ZipFile(bundle_path, "w", zipfile.ZIP_DEFLATED) as bundle:
            for asset_path in sorted(assets_dir.rglob("*.yaml")):
                asset = yaml.safe_load(asset_path.read_text(encoding="utf-8"))
                if asset.get("uuid") in database_uuids:
                    asset["uuid"] = database_uuids[asset["uuid"]]
                if asset.get("database_uuid") in database_uuids:
                    asset["database_uuid"] = database_uuids[asset["database_uuid"]]
                bundle.writestr(
                    "traffic-operations/"
                    + asset_path.relative_to(assets_dir).as_posix(),
                    yaml.safe_dump(asset, sort_keys=False),
                )
        subprocess.run(
            [
                "superset",
                "import-dashboards",
                "--path",
                str(bundle_path),
                "--username",
                username,
            ],
            check=True,
        )
    print("Imported dashboard: Tehran Traffic Operations", flush=True)
