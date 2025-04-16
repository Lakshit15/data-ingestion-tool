from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import clickhouse_connect
from typing import List, Optional
import pandas as pd
import io
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class ClickHouseConnection(BaseModel):
    host: str
    port: int
    database: str
    username: str
    password: str
    secure: bool = False

class ColumnSelection(BaseModel):
    table: str
    columns: List[str]
    join_tables: Optional[List[str]] = None
    join_condition: Optional[str] = None

@app.post("/connect-clickhouse")
async def connect_clickhouse(conn: ClickHouseConnection):
    try:
        logger.info(f"Connecting to ClickHouse at {conn.host}:{conn.port}")
        client = clickhouse_connect.get_client(
            host=conn.host,
            port=conn.port,
            username=conn.username,
            password=conn.password,
            database=conn.database,
            secure=conn.secure
        )
        tables = client.query('SHOW TABLES').result_rows
        table_names = [table[0] for table in tables]
        logger.info(f"Found tables: {table_names}")
        return {"status": "success", "tables": table_names}
    except Exception as e:
        logger.error(f"Connection failed: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/get-columns")
async def get_columns(conn: ClickHouseConnection, table: str):
    try:
        logger.info(f"Getting columns for table {table}")
        client = clickhouse_connect.get_client(
            host=conn.host,
            port=conn.port,
            username=conn.username,
            password=conn.password,
            database=conn.database
        )
        columns = client.query(f"DESCRIBE TABLE {table}").result_rows
        column_names = [col[0] for col in columns]
        return {"status": "success", "columns": column_names}
    except Exception as e:
        logger.error(f"Failed to get columns: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/clickhouse-to-flatfile")
async def clickhouse_to_flatfile(conn: ClickHouseConnection, selection: ColumnSelection):
    try:
        logger.info(f"Exporting data from table {selection.table}")
        client = clickhouse_connect.get_client(
            host=conn.host,
            port=conn.port,
            username=conn.username,
            password=conn.password,
            database=conn.database
        )
        
        # Build query based on whether we're joining tables or not
        if selection.join_tables and selection.join_condition:
            tables_str = ", ".join([selection.table] + selection.join_tables)
            query = f"SELECT {', '.join(selection.columns)} FROM {tables_str} WHERE {selection.join_condition}"
        else:
            query = f"SELECT {', '.join(selection.columns)} FROM {selection.table}"
        
        logger.info(f"Executing query: {query}")
        result = client.query(query)
        df = pd.DataFrame(result.result_rows, columns=selection.columns)
        csv_data = df.to_csv(index=False)
        
        logger.info(f"Exported {len(result.result_rows)} rows")
        return {
            "status": "success",
            "data": csv_data,
            "count": len(result.result_rows),
            "query": query
        }
    except Exception as e:
        logger.error(f"Export failed: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/flatfile-to-clickhouse")
async def flatfile_to_clickhouse(
    conn: ClickHouseConnection,
    file: UploadFile = File(...),
    table: str = "imported_data",
    delimiter: str = ","
):
    try:
        logger.info(f"Importing data to table {table}")
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')), delimiter=delimiter)
        
        client = clickhouse_connect.get_client(
            host=conn.host,
            port=conn.port,
            username=conn.username,
            password=conn.password,
            database=conn.database
        )
        
        # Create table if not exists with inferred schema
        columns_def = []
        for col in df.columns:
            dtype = 'String'  # Default to String
            if pd.api.types.is_numeric_dtype(df[col]):
                dtype = 'Float64'
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                dtype = 'DateTime'
            columns_def.append(f"{col} {dtype}")
        
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(columns_def)}) ENGINE Memory"
        logger.info(f"Creating table with: {create_table_sql}")
        client.command(create_table_sql)
        
        # Insert data
        logger.info(f"Inserting {len(df)} rows into {table}")
        client.insert(table, df.values.tolist(), column_names=df.columns.tolist())
        
        return {
            "status": "success",
            "count": len(df),
            "columns": df.columns.tolist(),
            "table_created": create_table_sql
        }
    except Exception as e:
        logger.error(f"Import failed: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/health")
async def health_check():
    return {"status": "healthy", "message": "Data ingestion service is running"}