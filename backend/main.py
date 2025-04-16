from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from clickhouse_driver import Client as ClickHouseClient

app = FastAPI()

class ClickHouseConnection(BaseModel):
    host: str
    port: int
    database: str
    user: str
    jwt_token: str
    secure: bool = True

@app.post("/connect-clickhouse")
async def connect_clickhouse(conn: ClickHouseConnection):
    try:
        client = ClickHouseClient(
            host=conn.host,
            port=conn.port,
            database=conn.database,
            user=conn.user,
            password=conn.jwt_token,
            secure=conn.secure
        )
        tables = client.execute("SHOW TABLES")
        return {"status": "success", "tables": [table[0] for table in tables]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))