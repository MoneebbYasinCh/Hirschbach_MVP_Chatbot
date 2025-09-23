from typing import Dict, Any, List
import os
from langchain_openai import AzureChatOpenAI
from Tools.entity_mapping_tool import EntityMappingTool

class KPIEditorNode:
    """
    Node for editing/modifying existing KPIs to better match the user's task.
    Uses metadata information to make intelligent adjustments to the KPI SQL.
    """
    
    def __init__(self):
        # Initialize Azure OpenAI
        self.llm = AzureChatOpenAI(
            azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-07-18"),
            temperature=0.1
        )
        
        # Initialize entity mapping tool
        self.entity_tool = EntityMappingTool()
    
    def __call__(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Edit the KPI SQL to better match the user's task using metadata information.
        """
        # Get user input from the latest message
        messages = state.get("messages", [])
        if not messages:
            print("[KPI_EDITOR] No messages found")
            state["kpi_editor_status"] = "error"
            state["kpi_editor_error"] = "No messages found in state"
            return state
        
        task = messages[-1].content if hasattr(messages[-1], 'content') else str(messages[-1])
        
        # Get KPI data from state
        top_kpi = state.get("top_kpi")
        if not top_kpi:
            print("[KPI_EDITOR] No KPI data available for editing")
            state["kpi_editor_status"] = "error"
            state["kpi_editor_error"] = "No KPI data available for editing"
            return state
        
        # Get metadata data from state
        metadata_results = state.get("metadata_rag_results", [])
        
        # Extract KPI information
        original_sql = top_kpi.get("sql_query", "")
        kpi_metric = top_kpi.get("metric_name", "")
        kpi_description = top_kpi.get("description", "")
        
        print(f"[KPI_EDITOR] Editing KPI: {kpi_metric}")
        print(f"[KPI_EDITOR] Original SQL: {original_sql[:100]}...")
        
        # Step 1: Analyze what columns are needed
        needed_columns = self._analyze_needed_columns_step1(task, metadata_results)
        
        # Step 2: Get exact values for needed columns
        entity_mapping_data = self._get_entity_mapping_data(needed_columns)
        
        # Step 3: Map user intent to exact values
        mapped_values = self._map_user_intent_to_values_step2(task, needed_columns, entity_mapping_data)
        
        # Step 4: Generate final SQL
        prompt = self._create_sql_generation_prompt_step3(task, kpi_metric, kpi_description, original_sql, metadata_results, mapped_values)
        
        try:
            response = self.llm.invoke(prompt)
            edited_sql = response.content.strip()
            
            # Clean up the response - remove any markdown code blocks if present
            if edited_sql.startswith("```sql"):
                edited_sql = edited_sql[6:]
            if edited_sql.startswith("```"):
                edited_sql = edited_sql[3:]
            if edited_sql.endswith("```"):
                edited_sql = edited_sql[:-3]
            edited_sql = edited_sql.strip()
            
            # Basic validation
            if not edited_sql or "SELECT" not in edited_sql.upper():
                print(f"❌ [KPI_EDITOR] Invalid SQL response from LLM")
                state["kpi_editor_status"] = "error"
                state["kpi_editor_error"] = "LLM returned invalid SQL"
                return state
            
            # Basic validation
            validation_result = self._validate_sql_columns(edited_sql, metadata_results)
            if not validation_result["valid"]:
                state["kpi_editor_status"] = "error"
                state["kpi_editor_error"] = "Invalid SQL generated"
                return state
            
            # Set success status
            if edited_sql == original_sql:
                print(f"⚠️ [KPI_EDITOR] No changes made to SQL")
                modifications = ["No changes needed"]
            else:
                print(f"✅ [KPI_EDITOR] Successfully modified KPI SQL")
                modifications = ["Modified SQL query to better match user requirements"]
            
            state["kpi_editor_status"] = "completed"
            state["kpi_editor_result"] = {
                "edited_sql": edited_sql,
                "modifications_made": modifications,
                "success": True,
                "confidence": "HIGH"
            }
            
            # Update the top_kpi in state with edited SQL
            state["top_kpi"]["sql_query"] = edited_sql
            return state
            
        except Exception as e:
            print(f"❌ [KPI_EDITOR] Error: {str(e)}")
            state["kpi_editor_status"] = "error"
            state["kpi_editor_error"] = str(e)
            state["kpi_editor_result"] = {
                "edited_sql": original_sql,
                "modifications_made": [],
                "success": False,
                "confidence": "LOW"
            }
            return state
    
    def _format_metadata_for_prompt(self, metadata_columns: List[Dict]) -> str:
        """Format metadata columns for the prompt in a readable way"""
        if not metadata_columns:
            return "No metadata available"
        
        formatted_columns = []
        for col in metadata_columns:
            col_name = col.get('column_name', '')
            col_desc = col.get('description', '')
            col_type = col.get('data_type', '')
            col_score = col.get('score', 0)
            
            formatted_columns.append(f"- {col_name} ({col_type}): {col_desc} (relevance: {col_score:.2f})")
        
        return "\n".join(formatted_columns)
    
    def _analyze_needed_columns_step1(self, task: str, metadata_results: List[Dict[str, Any]]) -> List[str]:
        """Analyze what additional columns are needed based on user query and metadata"""
        needed_columns = []
        
        # Get all available column names from metadata
        available_columns = [col.get('column_name', '') for col in metadata_results if col.get('column_name')]
        
        if not available_columns:
            return needed_columns
        
        # Simple analysis prompt
        analysis_prompt = f"""
        User request: "{task}"
        Available columns: {', '.join(available_columns)}
        
        Which columns might be needed? Return column names from the list above, separated by commas, or "none".
        """
        
        try:
            response = self.llm.invoke(analysis_prompt)
            analysis_result = response.content.strip().lower()
            
            if analysis_result != "none" and analysis_result:
                # Parse the column names
                suggested_columns = [col.strip() for col in analysis_result.split(',') if col.strip()]
                
                # Only include columns that actually exist in metadata
                for col in suggested_columns:
                    if col in available_columns:
                        needed_columns.append(col)
                        print(f"🔧 [KPI_EDITOR] Identified needed column: {col}")
                    else:
                        print(f"⚠️ [KPI_EDITOR] Ignoring hallucinated column: {col} (not in available columns)")
                        print(f"⚠️ [KPI_EDITOR] Available columns: {available_columns}")
            
        except Exception as e:
            print(f"⚠️ [KPI_EDITOR] Error analyzing needed columns: {str(e)}")
        
        return needed_columns
    
    def _map_user_intent_to_values_step2(self, task: str, needed_columns: List[str], entity_mapping_data: str) -> Dict[str, Any]:
        """Step 2: Map user intent to exact values using LLM"""
        if not needed_columns:
            return {}
        
        # Simple mapping prompt
        prompt = f"""
        User request: "{task}"
        Columns: {', '.join(needed_columns)}
        Available values: {entity_mapping_data}
        
        Map user intent to exact values. Format: Column1: value1, Column2: value2
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
                    
                    if values != "unclear":
                        # Parse multiple values if comma-separated
                        value_list = [v.strip() for v in values.split(',') if v.strip()]
                        mapped_values[column] = value_list
                        print(f"🔧 [KPI_EDITOR] Mapped {column} to: {value_list}")
                    else:
                        print(f"⚠️ [KPI_EDITOR] Could not map {column} - unclear intent")
            
            return mapped_values
            
        except Exception as e:
            print(f"⚠️ [KPI_EDITOR] Error mapping user intent to values: {str(e)}")
            return {}
    
    def _create_sql_generation_prompt_step3(self, task: str, kpi_metric: str, kpi_description: str, original_sql: str, metadata_results: List[Dict[str, Any]], mapped_values: Dict[str, Any]) -> str:
        """Step 3: Create focused SQL generation prompt"""
        
        # Format mapped values
        values_text = ""
        if mapped_values:
            values_text = f"Use these exact values: {mapped_values}"
        
        return f"""
        Modify this SQL to match the user request: "{task}"
        
        Original KPI: {kpi_metric}
        Original SQL: {original_sql}
        
        Available columns: {self._format_metadata_for_prompt(metadata_results)}
        {values_text}
        
        Return only the modified SQL query.
        """
    
    def _get_entity_mapping_data(self, needed_columns: List[str]) -> str:
        """Get entity mapping data for the specific columns that are needed"""
        entity_data = []
        
        if not needed_columns:
            return "No additional columns needed for filtering"
        
        for column_name in needed_columns:
            try:
                result = self.entity_tool.get_column_values(column_name)
                if result.get("success", False):
                    values = result.get("values", [])
                    entity_data.append(f"- {column_name}: {values}")
                    print(f"🔧 [KPI_EDITOR] Added entity mapping for {column_name}: {values}")
                else:
                    print(f"⚠️ [KPI_EDITOR] No values found for column: {column_name}")
            except Exception as e:
                print(f"⚠️ [KPI_EDITOR] Error getting values for {column_name}: {str(e)}")
        
        if not entity_data:
            return "No exact values available for the needed columns"
        
        return "\n".join(entity_data)
    
    def _validate_sql_columns(self, sql: str, metadata_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Simple validation that SQL contains basic SELECT structure"""
        # Basic validation - just check if it's a valid SELECT statement
        sql_upper = sql.upper().strip()
        
        if not sql_upper.startswith('SELECT'):
            return {
                "valid": False,
                "unauthorized_columns": ["Not a SELECT statement"],
                "allowed_columns": [],
                "potential_columns": []
            }
        
        # For now, just return valid - let the LLM handle the column validation
        return {
            "valid": True,
            "unauthorized_columns": [],
            "allowed_columns": [col.get('column_name', '') for col in metadata_results if col.get('column_name')],
            "potential_columns": []
        }
