import clickhouse_connect
from pathlib import Path
import yaml 

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
class ClickHouseClient():
    def __init__(self) -> None:
        self.client = self.connect()

    def connect(self):
        with open(PROJECT_ROOT / 'creds.yml', 'r') as f:
            data = yaml.safe_load(f) or {}
        creds = data.get('neo4j_user', data)

        client = clickhouse_connect.get_client(
        host='e4h24gda5p.eu-central-1.aws.clickhouse.cloud',
        user=creds['username'],
        password=creds['password'],
        secure=True
        )

        return client