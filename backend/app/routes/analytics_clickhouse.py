from flask import Blueprint, jsonify
from backend.clickhouse.clickhouse import ClickHouseClient
import json
from flask import Response


analytics_ch_bp = Blueprint(
    "analytics_clickhouse",
    __name__,
    url_prefix="/api/v1/analytics/ch"
)

ch = ClickHouseClient().client
DB = "default"

def decode_row(row):
    return [c.decode("utf-8") if isinstance(c, bytes) else c for c in row]


# dienos pajamos
@analytics_ch_bp.get("/revenue-per-day")
def revenue_per_day():
    query = f"""
        SELECT
            toDate(uzsakymo_data) AS diena,
            sum(kiekis * kaina) AS pajamos
        FROM {DB}.uzsakymai_bilietai
        GROUP BY diena
        ORDER BY diena
    """
    rows = ch.query(query).result_rows
    return jsonify(rows)

# pajamos pagal miestą 

@analytics_ch_bp.get("/revenue-by-city")
def revenue_by_city():
    query = f"""
        SELECT
            r.miestas,
            sum(u.kiekis * u.kaina) AS pajamos
        FROM {DB}.uzsakymai_bilietai u
        JOIN {DB}.renginiai r
            ON u.renginys_id = r.event_id
        GROUP BY r.miestas
        ORDER BY pajamos DESC
    """
    rows = ch.query(query).result_rows
    list_rows = [[str(r[0]), float(r[1])] for r in rows]

    return Response(
        json.dumps(list_rows, ensure_ascii=False, indent=2),
        mimetype="application/json; charset=utf-8"
    )

# valandos, kai nuperkama daugiausiai bilietų
@analytics_ch_bp.get("/top_5_hours")
def top_5_hours():
    query = f"""
        SELECT
            toHour(uzsakymo_data) AS hour,
            count(*) AS orders_count
        FROM {DB}.uzsakymai_bilietai
        GROUP BY hour
        ORDER BY orders_count DESC
        LIMIT 5
    """
    rows = ch.query(query).result_rows
    result = [{"hour": row[0], "orders_count": row[1]} for row in rows]
    return jsonify(result)

# penki nepopuliariausi renginiai
@analytics_ch_bp.get("/bottom-events")
def top_events():
    query = f"""
        SELECT
            renginys_id,
            sum(kiekis) AS parduota
        FROM {DB}.uzsakymai_bilietai
        GROUP BY renginys_id
        ORDER BY parduota ASC
        LIMIT 5
    """
    return jsonify(ch.query(query).result_rows)


