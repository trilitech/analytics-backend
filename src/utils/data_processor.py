import datetime
from decimal import Decimal

async def fetch_non_time_series_data(conn, query, table_name, column_names, *args):
    rows = await conn.fetch(query, *args)
    column_data = [[] for _ in column_names]
    for row in rows:
        if len(row) != len(column_names):
            raise ValueError(f"Query returned {len(row)} columns, expected {len(column_names)}")
        for i in range(len(column_names)):
            value = row[i]
            if isinstance(value, (int, float, Decimal, str)):
                column_data[i].append(value)
            elif isinstance(value, (datetime.date, datetime.datetime)):
                column_data[i].append(value.isoformat())
            else:
                column_data[i].append(None)
    return {
        "series": [
            {
                "name": table_name,
                "columns": column_names,
                "values": column_data
            }
        ]
    }