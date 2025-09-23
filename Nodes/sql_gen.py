from typing import Dict, Any, List
import os
from langchain_openai import AzureChatOpenAI
from Tools.entity_mapping_tool import EntityMappingTool

class SQLGenerationNode:
    """Node for generating SQL queries using KPI editor pattern - analyze columns, get values, map intent, generate SQL"""
    
    def __init__(self):
            # Initialize Azure OpenAI
            self.llm = AzureChatOpenAI(
                azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o"),
                azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
                api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-07-18"),
                temperature=0.1
            )
            
            # Initialize entity mapping tool
            self.entity_tool = EntityMappingTool()

    def __call__(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate SQL queries using KPI editor pattern - analyze needed columns, get values, map intent, generate SQL
        
        Args:
            state: Current state containing user query, metadata, and LLM checker results
            
        Returns:
            Updated state with generated SQL
        """
        print("[SQL GENERATION] Processing SQL generation...")
        
        # Extract user query from state (preferred) or fallback to last message
        user_query = state.get("user_query", "")
        if not user_query:
            # Fallback to last message if user_query not in state
            messages = state.get("messages", [])
            if messages:
                user_query = messages[-1].content if hasattr(messages[-1], 'content') else str(messages[-1])
        
        if not user_query:
            print("❌ [SQL GENERATION] No user query found in state")
            state["sql_generation_status"] = "error"
            state["sql_generation_error"] = "No user query found in state"
            return state
        
        # Get metadata results
        metadata_results = state.get("metadata_rag_results", [])
        llm_check_result = state.get("llm_check_result", {})
        
        print(f"[SQL GENERATION] Debug - State keys: {list(state.keys())}")
        print(f"[SQL GENERATION] Debug - Metadata results count: {len(metadata_results)}")
        print(f"[SQL GENERATION] Debug - LLM check result: {llm_check_result}")
        
        if not metadata_results:
            print("[SQL GENERATION] No metadata available for SQL generation")
            state["sql_generation_status"] = "error"
            state["sql_generation_error"] = "No metadata available for SQL generation"
            return state
        
        try:
            # Step 1: Analyze what columns are needed (like KPI editor)
            needed_columns = self._analyze_needed_columns(user_query, metadata_results)
            
            # Step 2: Get entity mapping data for needed columns only
            entity_mapping_data = self._get_entity_mapping_data(needed_columns)
            
            # Step 3: Map user intent to exact values
            mapped_values = self._map_user_intent_to_values(user_query, needed_columns, entity_mapping_data)
            
            # Step 4: Generate final SQL with exact values
            final_sql = self._generate_final_sql(user_query, metadata_results, mapped_values)
            
            # Update state
            state["sql_generation_status"] = "completed"
            state["generated_sql"] = final_sql
            state["sql_validated"] = True
            state["sql_generation_result"] = {
                "success": True,
                "final_sql": final_sql,
                "needed_columns": needed_columns,
                "mapped_values": mapped_values
            }
            
            print("✅ [SQL GENERATION] SQL generation completed successfully")
            return state
            
        except Exception as e:
            print(f"❌ [SQL GENERATION] Error: {str(e)}")
            state["sql_generation_status"] = "error"
            state["sql_generation_error"] = str(e)
            state["sql_generation_result"] = {
                "success": False,
                "error": str(e)
            }
            return state
    
    def _analyze_needed_columns(self, user_query: str, metadata_results: List[Dict]) -> List[str]:
        """Intelligently pick columns from metadata results based on query and column descriptions"""
        needed_columns = []
        
        if not metadata_results:
            return needed_columns
        
        # Create detailed column information for LLM analysis
        column_details = []
        for col_data in metadata_results:
            col_name = col_data.get('column_name', '')
            col_desc = col_data.get('description', 'No description')
            col_type = col_data.get('data_type', 'Unknown')
            score = col_data.get('score', 0)
            column_details.append(f"- {col_name} ({col_type}): {col_desc} [relevance: {score:.2f}]")
        
        # Get available column names
        available_columns = [col.get('column_name', '') for col in metadata_results if col.get('column_name')]
        
        # Smart selection prompt using column descriptions
        analysis_prompt = f"""
        SQL Query Request: "{user_query}"
        
        Available columns from metadata retrieval:
        {chr(10).join(column_details)}
        
        Based on the query request and column descriptions, select which columns are needed for this SQL query.
        
        Available column names: {', '.join(available_columns)}
        
        Return only the exact column names from the list above, separated by commas. If no specific columns are needed for filtering, return "none".
        """
        
        try:
            response = self.llm.invoke(analysis_prompt)
            analysis_result = response.content.strip()
            
            if analysis_result.lower() != "none" and analysis_result:
                # Parse the selected column names
                selected_columns = [col.strip() for col in analysis_result.split(',') if col.strip()]
                
                # Only keep columns that exist in available_columns
                for col in selected_columns:
                    if col in available_columns and col not in needed_columns:
                        needed_columns.append(col)
                        print(f"🔧 [SQL_GEN] Selected column: {col}")
            
        except Exception as e:
            print(f"⚠️ [SQL_GEN] Error analyzing needed columns: {str(e)}")
        
        if not needed_columns:
            print("🔧 [SQL_GEN] No specific columns selected - will use query context")
        
        return needed_columns
    
    def _get_entity_mapping_data(self, needed_columns: List[str]) -> Dict[str, List[str]]:
        """Get exact values for the identified columns from CSV (like KPI editor)"""
        entity_data = {}
        
        if not needed_columns:
            return entity_data
        
        for column_name in needed_columns:
            try:
                result = self.entity_tool.get_column_values(column_name)
                if result.get("success", False):
                    values = result.get("values", [])
                    entity_data[column_name] = values
                    print(f"🔧 [SQL_GEN] Added entity mapping for {column_name}: {values}")
            except Exception as e:
                print(f"⚠️ [SQL_GEN] Error getting values for {column_name}: {str(e)}")
        
        return entity_data
    
    def _map_user_intent_to_values(self, user_query: str, needed_columns: List[str], entity_data: Dict[str, List[str]]) -> Dict[str, str]:
        """Map user intent to exact values using LLM (like KPI editor)"""
        if not needed_columns or not entity_data:
            return {}
        
        # Format entity data for prompt
        entity_text = ""
        for col, values in entity_data.items():
            entity_text += f"- {col}: {values}\n"
        
        prompt = f"""
        User request: "{user_query}"
        Available columns with values:
        {entity_text}
        
        Map user intent to exact values. Format: Column1: value1, Column2: value2
        If no specific values needed, return: none
        """
        
        try:
            response = self.llm.invoke(prompt)
            mapping_result = response.content.strip()
            
            # Parse the mapping result
            mapped_values = {}
            for line in mapping_result.split('\n'):
                if ':' in line:
                    column, values = line.split(':', 1)
                    column = column.strip()
                    values = values.strip()
                    
                    if values != "unclear" and values != "none":
                        # Parse multiple values if comma-separated
                        value_list = [v.strip() for v in values.split(',') if v.strip()]
                        if value_list:
                            mapped_values[column] = value_list[0]  # Use first value for now
                            print(f"🔧 [SQL_GEN] Mapped {column} to: {value_list[0]}")
                    else:
                        print(f"⚠️ [SQL_GEN] Could not map {column} - unclear intent")
            
            return mapped_values
            
        except Exception as e:
            print(f"⚠️ [SQL_GEN] Error mapping user intent to values: {str(e)}")
            return {}
    
    def _generate_final_sql(self, user_query: str, metadata_results: List[Dict], mapped_values: Dict[str, str]) -> str:
        """Generate final SQL with exact values (like KPI editor)"""
        
        # Format metadata for prompt
        if metadata_results:
            formatted_columns = []
            for col in metadata_results:
                col_name = col.get('column_name', '')
                col_desc = col.get('description', '')
                col_type = col.get('data_type', '')
                formatted_columns.append(f"- {col_name} ({col_type}): {col_desc}")
            metadata_text = "\n".join(formatted_columns)
        else:
            metadata_text = "No metadata available"
        
        # Format mapped values
        values_text = ""
        if mapped_values:
            values_text = f"Use these exact values: {mapped_values}"
        
        prompt = f"""
        Generate a SQL query for claims_summary table based on the user request.

        USER REQUEST: "{user_query}"
        
        AVAILABLE COLUMNS:
        {metadata_text}
        
        {values_text}
        
        Use AWS SQL syntax: wrap column names with spaces in square brackets [Column Name]
        Use table name: PRD.CLAIMS_SUMMARY
        Return only the SQL query.
        """
        
        try:
            response = self.llm.invoke(prompt)
            sql_query = response.content.strip()
            
            # Clean up the response - remove any markdown code blocks if present
            if sql_query.startswith("```sql"):
                sql_query = sql_query[6:]
            if sql_query.startswith("```"):
                sql_query = sql_query[3:]
            if sql_query.endswith("```"):
                sql_query = sql_query[:-3]
            sql_query = sql_query.strip()
            
            return sql_query
            
        except Exception as e:
            print(f"❌ [SQL_GEN] Error generating SQL: {str(e)}")
            return ""