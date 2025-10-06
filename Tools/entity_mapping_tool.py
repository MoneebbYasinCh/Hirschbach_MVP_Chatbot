"""
Entity Mapping Tool
Maps entities to exact database values using column-based lookup from CSV
"""

import os
import pandas as pd
import json
import re
from typing import Dict, Any, List, Optional
from langchain_openai import AzureChatOpenAI
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class EntityMappingTool:
    """
    Tool for mapping entities to exact database values using column-based lookup
    """

    def __init__(self):
        self.llm = AzureChatOpenAI(
            azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-07-18"),
            temperature=0.0
        )

        self.csv_data = self._load_csv_data()
        
    def _load_csv_data(self) -> pd.DataFrame:
        """Load the CSV data for column value lookup"""
        try:
            csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Data', 'For_BM25.csv')
            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path)
                print(f" [ENTITY MAPPING] Loaded CSV data: {len(df)} rows")
                return df
            else:
                print(f" [ENTITY MAPPING] CSV file not found: {csv_path}")
                return pd.DataFrame()
        except Exception as e:
            print(f" [ENTITY MAPPING] Error loading CSV: {str(e)}")
            return pd.DataFrame()
    
    def get_column_values(self, column_name: str) -> Dict[str, Any]:
        """
        Get all available values for a specific column
        
        Args:
            column_name: Name of the column to get values for
            
        Returns:
            Dictionary with column values and metadata
        """
        print(f"[ENTITY MAPPING] Getting values for column: '{column_name}'")
        
        if self.csv_data.empty:
            print(" [ENTITY MAPPING] No CSV data available")
            return {"error": "No CSV data available", "values": [], "column_name": column_name}
        
        try:
            # Get all available values for the specified column
            column_info = self._get_column_values(column_name)
            available_values = column_info.get("values", [])
            
            if not available_values:
                print(f" [ENTITY MAPPING] No values found for column '{column_name}'")
                return {
                    "error": f"No values found for column '{column_name}'",
                    "values": [],
                    "column_name": column_name
                }
            
            result = {
                "column_name": column_name,
                "values": available_values,
                "column_info": column_info,
                "success": True
            }
            
            # Show only first 3 values and count
            preview = available_values[:3] if len(available_values) > 3 else available_values
            print(f" [ENTITY MAPPING] Found {len(available_values)} values for '{column_name}': {preview}{'...' if len(available_values) > 3 else ''}")
            return result
                    
        except Exception as e:
            print(f" [ENTITY MAPPING] Error getting values for '{column_name}': {str(e)}")
            return {
                "error": f"Error getting values: {str(e)}",
                "values": [],
                "column_name": column_name
            }
    
    
    def _get_column_values(self, column_name: str) -> Dict[str, Any]:
        """Get all available values for a specific column from CSV"""
        try:
            # Filter CSV data for the specific column
            column_data = self.csv_data[self.csv_data['COLUMNNAME'] == column_name]
            
            if column_data.empty:
                print(f" [ENTITY MAPPING] No data found for column '{column_name}'")
                return {"values": [], "source": "none", "distinct_count": 0}
            
            row = column_data.iloc[0]
            
            # Try to get distinct values first
            distinct_values_raw = row.get('Distinct', '')
            sample_values = row.get('sample values', '')
            
            # Parse distinct values first (the Distinct column contains the actual values, not a count)
            if pd.notna(distinct_values_raw) and distinct_values_raw != '' and str(distinct_values_raw) != '#N/A':
                # Parse distinct values from the Distinct column
                distinct_str = str(distinct_values_raw)
                # Remove quotes and split by comma
                parsed_values = [v.strip().strip('"') for v in distinct_str.split(',') if v.strip()]
                source = "distinct_values"
            else:
                # Fall back to sample values if no distinct values available
                if pd.notna(sample_values) and sample_values != '':
                    parsed_values = [v.strip() for v in str(sample_values).split(',') if v.strip()]
                    source = "sample_values"
                else:
                    parsed_values = []
                    source = "none"
            
            # Log the results
            if parsed_values:
                print(f" [ENTITY MAPPING] Column '{column_name}' has {len(parsed_values)} values from {source}")
            
            result = {
                "values": parsed_values,
                "source": source,
                "distinct_count": len(parsed_values),
                "sample_values_raw": sample_values
            }
            
            # Show only first 3 values and count
            preview = parsed_values[:3] if len(parsed_values) > 3 else parsed_values
            print(f" [ENTITY MAPPING] Found {len(parsed_values)} values for '{column_name}' (source: {source}): {preview}{'...' if len(parsed_values) > 3 else ''}")
            return result
            
        except Exception as e:
            print(f" [ENTITY MAPPING] Error getting column values: {str(e)}")
            return {"values": [], "source": "error", "distinct_count": 0}
    
    
    def get_available_columns(self) -> List[str]:
        """Get list of available columns from CSV"""
        if self.csv_data.empty:
            return []
        
        return self.csv_data['COLUMNNAME'].unique().tolist()
    
    def get_column_info(self, column_name: str) -> Dict[str, Any]:
        """Get information about a specific column"""
        if self.csv_data.empty:
            return {"error": "No CSV data available"}
        
        column_data = self.csv_data[self.csv_data['COLUMNNAME'] == column_name]
        
        if column_data.empty:
            return {"error": f"Column '{column_name}' not found"}
        
        row = column_data.iloc[0]
        return {
            "column_name": column_name,
            "sample_values": row['sample values'],
            "distinct_count": row['Distinct'],
            "unique_values": self._get_column_values(column_name)
        }
