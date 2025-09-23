from typing import Dict, Any, List, Optional
import os
import pyodbc
import pandas as pd
from datetime import datetime
import logging

class AzureRetrievalNode:
    """Node for retrieving data from Azure SQL Database using validated SQL queries"""
    
    def __init__(self):
        """Initialize Azure SQL Database connection parameters"""
        # Use single connection string from environment (just like diagnostic)
        self.connection_string = os.getenv("SQL_CONNECTION_STRING")
        
        # Configure logging only if not already configured
        if not logging.getLogger().handlers:
            logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def __call__(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Retrieve data from Azure SQL Database using validated SQL and KPI information
        
        Args:
            state: Current state containing validated SQL and KPI results
            
        Returns:
            Updated state with Azure retrieval results
        """
        self.logger.info("[AZURE RETRIEVAL] Processing Azure data retrieval...")
        print("[AZURE RETRIEVAL] Processing Azure data retrieval...")
        
        # Get validated results from SQL checker
        sql_validated = state.get("sql_validated", False)
        kpi_validated = state.get("kpi_validated", False)
        generated_sql = state.get("generated_sql", "")
        edited_kpi = state.get("edited_kpi", {})
        
        if sql_validated and generated_sql:
            self.logger.info(f"[AZURE RETRIEVAL] Executing SQL query: {generated_sql[:100]}...")
            print(f"[AZURE RETRIEVAL] Executing SQL query: {generated_sql[:100]}...")
            
            try:
                # Execute SQL query and get results
                query_results = self._execute_sql_query(generated_sql)
                
                if query_results is not None:
                    state["azure_retrieval_completed"] = True
                    state["azure_data"] = {
                        "query_executed": generated_sql,
                        "rows_returned": query_results.get("row_count", 0),
                        "execution_time": query_results.get("execution_time", "0.0s"),
                        "data": query_results.get("data", []),
                        "columns": query_results.get("columns", []),
                        "success": True
                    }
                    self.logger.info(f"[AZURE RETRIEVAL] Successfully retrieved {query_results.get('row_count', 0)} rows")
                    print(f"[AZURE RETRIEVAL] Successfully retrieved {query_results.get('row_count', 0)} rows")
                    
                    # Trigger insight generation
                    state["insights_triggered"] = True
                    self.logger.info("[AZURE RETRIEVAL] Triggering insight generation...")
                    print("[AZURE RETRIEVAL] Triggering insight generation...")
                else:
                    state["azure_retrieval_completed"] = False
                    state["azure_data"] = {
                        "query_executed": generated_sql,
                        "error": "Query execution failed",
                        "success": False
                    }
                    self.logger.error("[AZURE RETRIEVAL] Query execution failed")
                    print("[AZURE RETRIEVAL] Query execution failed")
                    
            except Exception as e:
                print(f"[AZURE RETRIEVAL] Error executing query: {str(e)}")
                state["azure_retrieval_completed"] = False
                state["azure_data"] = {
                    "query_executed": generated_sql,
                    "error": str(e),
                    "success": False
                }
        else:
            print("[AZURE RETRIEVAL] No validated SQL to execute")
            state["azure_retrieval_completed"] = False
            state["azure_data"] = {
                "error": "No validated SQL available",
                "success": False
            }
        
        if kpi_validated and edited_kpi:
            print(f"[AZURE RETRIEVAL] Processing KPI: {edited_kpi.get('metric_name', 'Unknown')}")
            # TODO: Implement KPI processing logic if needed
            state["kpi_processed"] = True
        else:
            print("[AZURE RETRIEVAL] No validated KPI to process")
            state["kpi_processed"] = False
        
        return state
    
    def _execute_sql_query(self, sql_query: str) -> Optional[Dict[str, Any]]:
        """
        Execute SQL query against Azure SQL Database
        
        Args:
            sql_query: SQL query to execute
            
        Returns:
            Dictionary containing query results or None if failed
        """
        if not self.connection_string:
            self.logger.error("Azure SQL Database connection string not configured")
            return None
        
        try:
            # Use the connection string directly
            
            # Record start time
            start_time = datetime.now()
            
            # Execute query
            with pyodbc.connect(self.connection_string) as conn:
                with conn.cursor() as cursor:
                    # Execute the query
                    cursor.execute(sql_query)
                    
                    # Get column names
                    columns = [column[0] for column in cursor.description] if cursor.description else []
                    
                    # Fetch all results
                    rows = cursor.fetchall()
                    
                    # Convert rows to list of dictionaries
                    data = []
                    for row in rows:
                        row_dict = {}
                        for i, value in enumerate(row):
                            # Convert datetime objects to strings for JSON serialization
                            if isinstance(value, datetime):
                                row_dict[columns[i]] = value.isoformat()
                            else:
                                row_dict[columns[i]] = value
                        data.append(row_dict)
                    
                    # Calculate execution time
                    execution_time = (datetime.now() - start_time).total_seconds()
                    
                    self.logger.info(f"Query executed successfully: {len(data)} rows in {execution_time:.2f}s")
                    
                    return {
                        "data": data,
                        "columns": columns,
                        "execution_time": f"{execution_time:.2f}s",
                        "row_count": len(data)
                    }
                    
        except pyodbc.Error as e:
            self.logger.error(f"Database error: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error: {str(e)}")
            return None
    
    def _validate_connection(self) -> bool:
        """
        Validate Azure SQL Database connection
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            if not self.connection_string:
                self.logger.error("Azure SQL Database connection string not configured")
                return False
            
            with pyodbc.connect(self.connection_string) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    return result is not None
                    
        except Exception as e:
            self.logger.error(f"Connection validation failed: {str(e)}")
            return False
    
    def get_table_schema(self, table_name: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get schema information for a specific table
        
        Args:
            table_name: Name of the table to get schema for
            
        Returns:
            List of column information dictionaries or None if failed
        """
        try:
            if not self.connection_string:
                self.logger.error("Azure SQL Database connection string not configured")
                return None
            
            with pyodbc.connect(self.connection_string) as conn:
                with conn.cursor() as cursor:
                    # Query to get column information
                    schema_query = """
                    SELECT 
                        COLUMN_NAME,
                        DATA_TYPE,
                        IS_NULLABLE,
                        CHARACTER_MAXIMUM_LENGTH,
                        NUMERIC_PRECISION,
                        NUMERIC_SCALE,
                        COLUMN_DEFAULT
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = ?
                    ORDER BY ORDINAL_POSITION
                    """
                    
                    cursor.execute(schema_query, (table_name,))
                    rows = cursor.fetchall()
                    
                    columns = []
                    for row in rows:
                        columns.append({
                            "column_name": row[0],
                            "data_type": row[1],
                            "is_nullable": row[2],
                            "max_length": row[3],
                            "precision": row[4],
                            "scale": row[5],
                            "default_value": row[6]
                        })
                    
                    return columns
                    
        except Exception as e:
            self.logger.error(f"Error getting table schema: {str(e)}")
            return None
