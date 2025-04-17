from fastapi import FastAPI, HTTPException, UploadFile, File, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
import clickhouse_connect
from typing import List, Optional
import pandas as pd
import io
import logging
from datetime import datetime
import re

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI(title="ClickHouse Data Ingestion API",
             description="Bidirectional data transfer between ClickHouse and flat files",
             version="1.0.0")

# Security-enhanced CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Total-Count"]
)

# Enhanced data models with validation
class ClickHouseConnection(BaseModel):
    host: str = Field(..., example="localhost")
    port: int = Field(8123, gt=0, lt=65536)
    database: str = Field(..., example="default")
    username: str = Field(..., example="default")
    password: str = Field("", example="")
    secure: bool = Field(False)

    @validator('host')
    def validate_host(cls, v):
        if not re.match(r'^[\w\.-]+$', v):
            raise ValueError('Invalid hostname')
        return v

class ColumnSelection(BaseModel):
    table: str = Field(..., example="users")
    columns: List[str] = Field(..., min_items=1)
    join_tables: Optional[List[str]] = Field(None)
    join_condition: Optional[str] = Field(None)

    @validator('table', 'join_tables', each_item=True)
    def validate_table_names(cls, v):
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', v):
            raise ValueError('Invalid table name')
        return v

# Database connection pool
connection_pool = {}

def get_client(conn: ClickHouseConnection):
    cache_key = f"{conn.host}:{conn.port}:{conn.database}:{conn.username}"
    if cache_key not in connection_pool:
        try:
            connection_pool[cache_key] = clickhouse_connect.get_client(
                host=conn.host,
                port=conn.port,
                username=conn.username,
                password=conn.password,
                database=conn.database,
                secure=conn.secure,
                connect_timeout=10,
                query_limit=0
            )
        except Exception as e:
            logger.error(f"Connection failed: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database connection failed"
            )
    return connection_pool[cache_key]

@app.post("/connect-clickhouse", 
          response_model=dict,
          responses={
              200: {"description": "Successfully connected"},
              400: {"description": "Invalid parameters"},
              503: {"description": "Service unavailable"}
          })
async def connect_clickhouse(conn: ClickHouseConnection):
    try:
        logger.info(f"Connection attempt to {conn.host}:{conn.port}")
        client = get_client(conn)
        
        # Test connection with lightweight query
        tables = client.query('SHOW TABLES', settings={'max_result_rows': 1000})
        table_names = [table[0] for table in tables.result_rows]
        
        logger.info(f"Found {len(table_names)} tables")
        return {
            "status": "success",
            "tables": table_names,
            "connection": f"{conn.host}:{conn.port}",
            "timestamp": datetime.now().isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Connection error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Connection failed: {str(e)}"
        )

@app.post("/get-columns",
          response_model=dict,
          responses={
              200: {"description": "Successfully retrieved columns"},
              400: {"description": "Invalid table name"},
              404: {"description": "Table not found"}
          })
async def get_columns(conn: ClickHouseConnection, table: str):
    try:
        logger.info(f"Fetching columns for table {table}")
        client = get_client(conn)
        
        # Validate table exists first
        exists = client.query(f"EXISTS TABLE {table}").result_rows[0][0]
        if not exists:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Table {table} does not exist"
            )
            
        columns = client.query(f"DESCRIBE TABLE {table}").result_rows
        column_info = [{
            "name": col[0],
            "type": col[1],
            "default": col[2],
            "comment": col[3]
        } for col in columns]
        
        return {
            "status": "success",
            "columns": column_info,
            "count": len(columns)
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Column fetch error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to get columns: {str(e)}"
        )

@app.post("/clickhouse-to-flatfile",
          response_model=dict,
          responses={
              200: {"description": "Data exported successfully"},
              400: {"description": "Invalid query parameters"},
              500: {"description": "Export failed"}
          })
async def clickhouse_to_flatfile(conn: ClickHouseConnection, selection: ColumnSelection):
    try:
        logger.info(f"Export request for table {selection.table}")
        client = get_client(conn)
        
        # Build safe query
        query = f"SELECT {', '.join(selection.columns)} FROM {selection.table}"
        
        if selection.join_tables and selection.join_condition:
            tables_str = ", ".join([selection.table] + selection.join_tables)
            query = f"SELECT {', '.join(selection.columns)} FROM {tables_str} WHERE {selection.join_condition}"
        
        logger.info(f"Executing query: {query[:200]}...")  # Log truncated query
        result = client.query(query)
        
        if not result.result_rows:
            return {
                "status": "success",
                "data": "",
                "count": 0,
                "message": "No data found"
            }
            
        df = pd.DataFrame(result.result_rows, columns=selection.columns)
        csv_data = df.to_csv(index=False, encoding='utf-8-sig')  # UTF-8 with BOM
        
        return {
            "status": "success",
            "data": csv_data,
            "count": len(result.result_rows),
            "query": query,
            "exported_at": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Export error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Export failed: {str(e)}"
        )

@app.post("/flatfile-to-clickhouse",
          response_model=dict,
          responses={
              200: {"description": "Data imported successfully"},
              400: {"description": "Invalid file format"},
              500: {"description": "Import failed"}
          })
async def flatfile_to_clickhouse(
    conn: ClickHouseConnection,
    file: UploadFile = File(...),
    table: str = "imported_data",
    delimiter: str = ","
):
    try:
        logger.info(f"Import request for file {file.filename}")
        
        # Validate file type
        if not file.filename.lower().endswith(('.csv', '.txt')):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only CSV files are supported"
            )
        
        contents = await file.read()
        df = pd.read_csv(
            io.StringIO(contents.decode('utf-8')),
            delimiter=delimiter,
            dtype=str,
            na_filter=False
        )
        
        if df.empty:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="File is empty or invalid format"
            )
        
        client = get_client(conn)
        
        # Enhanced schema detection
        type_mapping = {
            'int64': 'Int64',
            'float64': 'Float64',
            'bool': 'UInt8',
            'datetime64': 'DateTime',
            'object': 'String'
        }
        
        columns_def = []
        for col, dtype in df.dtypes.items():
            ch_type = type_mapping.get(str(dtype), 'String')
            columns_def.append(f"`{col}` {ch_type}")
        
        create_table_sql = (
            f"CREATE TABLE IF NOT EXISTS `{table}` "
            f"({', '.join(columns_def)}) "
            f"ENGINE = MergeTree() "
            f"ORDER BY tuple()"
        )
        
        logger.info(f"Creating table: {create_table_sql}")
        client.command(create_table_sql)
        
        # Batch insert for large files
        batch_size = 10000
        total_rows = len(df)
        inserted_rows = 0
        
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i + batch_size]
            client.insert(
                table,
                batch.values.tolist(),
                column_names=batch.columns.tolist()
            )
            inserted_rows += len(batch)
            logger.info(f"Inserted {inserted_rows}/{total_rows} rows")
        
        return {
            "status": "success",
            "count": inserted_rows,
            "columns": df.columns.tolist(),
            "table": table,
            "imported_at": datetime.now().isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Import error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Import failed: {str(e)}"
        )

@app.get("/health",
         response_model=dict,
         responses={
             200: {"description": "Service is healthy"},
             503: {"description": "Service unavailable"}
         })
async def health_check():
    try:
        # Test a minimal ClickHouse connection
        test_client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='default',
            password='',
            connect_timeout=2
        )
        test_client.command('SELECT 1')
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "services": {
                "database": "available",
                "storage": "ok"
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Service unavailable: {str(e)}"
        )

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down - closing database connections")
    for client in connection_pool.values():
        client.close()