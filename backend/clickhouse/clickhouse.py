import clickhouse_connect

class ClickHouseClient():
    def __init__(self) -> None:
        self.client = self.connect()

    def connect(self):
        client = clickhouse_connect.get_client(
        host='e4h24gda5p.eu-central-1.aws.clickhouse.cloud',
        user='default',
        password='<password>',
        secure=True
        )

        return client