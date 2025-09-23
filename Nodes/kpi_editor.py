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
        Edit the KPI SQL to better match the user's task using metadata information.
        """
        # Get user input from the latest message
        messages = state.get("messages", [])
        if not messages:
            print("[KPI_EDITOR] No messages found")
            return self._set_error_state(state, "No messages found in state")
        
        task = messages[-1].content if hasattr(messages[-1], 'content') else str(messages[-1])
        
        # Get KPI data from state
        top_kpi = state.get("top_kpi")
        if not top_kpi:
            print("[KPI_EDITOR] No KPI data available for editing")
            return self._set_error_state(state, "No KPI data available for editing")
        
        # Get metadata data from state
        metadata_results = state.get("metadata_rag_results", [])
        
        # Extract KPI information
        original_sql = top_kpi.get("sql_query", "")
        kpi_metric = top_kpi.get("metric_name", "")
        kpi_description = top_kpi.get("description", "")
        
        print(f"[KPI_EDITOR] Editing KPI: {kpi_metric}")
        print(f"[KPI_EDITOR] Original SQL: {original_sql[:100]}...")
        
        try:
            # Step 1: Analyze what columns are needed
            needed_columns = self._analyze_needed_columns_step1(task, metadata_results)
            
            # Step 2: Intelligently decide which columns need entity mapping
            columns_needing_mapping = self._analyze_columns_needing_mapping(task, needed_columns, metadata_results)
            
            # Step 3: Get exact values only for columns that need mapping
            entity_mapping_data = self._get_entity_mapping_data(columns_needing_mapping)
            
            # Step 4: Map user intent to exact values (only for relevant columns)
            mapped_values = self._map_user_intent_to_values_step2(task, columns_needing_mapping, entity_mapping_data)
            
            # Step 5: Generate final SQL
            prompt = self._create_sql_generation_prompt_step3(task, kpi_metric, kpi_description, original_sql, metadata_results, mapped_values)
            
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
            
            # Set success status
            if edited_sql == original_sql:
                print(f"⚠️ [KPI_EDITOR] No changes made to SQL")
                print(f"📝 [KPI_EDITOR] Final SQL: {edited_sql}")
                modifications = ["No changes needed"]
            else:
                print(f"✅ [KPI_EDITOR] Successfully modified KPI SQL")
                print(f"📝 [KPI_EDITOR] Modified SQL: {edited_sql}")
                modifications = ["Modified SQL query to better match user requirements"]
            
            return self._set_success_state(state, edited_sql, modifications)
            
        except Exception as e:
            print(f"❌ [KPI_EDITOR] Error: {str(e)}")
            return self._set_error_state(state, str(e))
    
    def _set_error_state(self, state: Dict, error_msg: str) -> Dict:
        """Centralized error state setting"""
        state["kpi_editor_status"] = "error"
        state["kpi_editor_error"] = error_msg
        state["kpi_editor_result"] = {
            "edited_sql": state.get("top_kpi", {}).get("sql_query", ""),
            "modifications_made": [],
            "success": False,
            "confidence": "LOW"
        }
        return state
    
    def _set_success_state(self, state: Dict, edited_sql: str, modifications: List[str]) -> Dict:
        """Centralized success state setting"""
        state["kpi_editor_status"] = "completed"
        state["kpi_editor_result"] = {
            "edited_sql": edited_sql,
            "modifications_made": modifications,
            "success": True,
            "confidence": "HIGH"
        }
        # Update the top_kpi in state with edited SQL
        state["top_kpi"]["sql_query"] = edited_sql
        # Set SQL as validated for Azure retrieval
        state["sql_validated"] = True
        state["generated_sql"] = edited_sql
        return state
    
    
    def _analyze_needed_columns_step1(self, task: str, metadata_results: List[Dict[str, Any]]) -> List[str]:
        """Intelligently pick columns from metadata results based on task and column descriptions"""
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
        Task: "{task}"
        
        Available columns from metadata retrieval:
        {chr(10).join(column_details)}
        
        Based on the task and column descriptions, select which columns are needed for filtering, grouping, or analyzing the data.
        
        Available column names: {', '.join(available_columns)}
        
        Return only the exact column names from the list above, separated by commas. If no additional columns are needed, return "none".
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
                        print(f"🔧 [KPI_EDITOR] Selected column: {col}")
            
        except Exception as e:
            print(f"⚠️ [KPI_EDITOR] Error analyzing needed columns: {str(e)}")
        
        if not needed_columns:
            print("🔧 [KPI_EDITOR] No additional columns needed")
        
        return needed_columns
    
    def _analyze_columns_needing_mapping(self, task: str, needed_columns: List[str], metadata_results: List[Dict[str, Any]]) -> List[str]:
        """Intelligently decide which columns actually need entity mapping based on user request"""
        if not needed_columns:
            return []
        
        # Create column details for LLM analysis
        column_details = []
        for col_data in metadata_results:
            col_name = col_data.get('column_name', '')
            if col_name in needed_columns:
                col_desc = col_data.get('description', 'No description')
                col_type = col_data.get('data_type', 'Unknown')
                column_details.append(f"- {col_name} ({col_type}): {col_desc}")
        
        analysis_prompt = f"""
        User request: "{task}"
        
        Available columns that might be used:
        {chr(10).join(column_details)}
        
        TASK: Determine which columns need entity mapping (exact value lookup from database).
        
        NEED MAPPING if user mentions:
        - Specific status values: "closed", "open", "pending", "resolved"
        - Specific customer names/codes: "customer ABC", "client XYZ" 
        - Specific claim types: "Work Comp", "Cargo", "Crash"
        - Specific locations/regions: "Texas", "North region", "warehouse A"
        - Specific departments: "IT department", "Sales team"
        - Any exact categorical values that need database lookup
        
        DON'T NEED MAPPING if request is generic:
        - "show claims by type" (generic grouping)
        - "group by status" (generic aggregation)  
        - "filter by date" (date/time filtering)
        - "count by customer" (generic counting)
        
        EXAMPLES:
        - "show closed claims" → Status Flag (needs mapping for "closed")
        - "claims for customer ABC" → Customer Code (needs mapping for "ABC")
        - "Work Comp claims by adjuster" → Claim Type (needs mapping for "Work Comp")
        - "show claims by type" → none (generic grouping)
        - "group by status" → none (generic grouping)
        
        ANALYSIS FOR: "{task}"
        Which specific values does the user mention that need exact database lookup?
        
        Return column names that need mapping, separated by commas. If none, return "none".
        """
        
        try:
            response = self.llm.invoke(analysis_prompt)
            analysis_result = response.content.strip()
            
            columns_needing_mapping = []
            if analysis_result.lower() != "none" and analysis_result:
                # Parse the selected column names
                selected_columns = [col.strip() for col in analysis_result.split(',') if col.strip()]
                
                # Only keep columns that exist in needed_columns
                for col in selected_columns:
                    if col in needed_columns and col not in columns_needing_mapping:
                        columns_needing_mapping.append(col)
                        print(f"🔍 [KPI_EDITOR] Column needs mapping: {col}")
            
            if not columns_needing_mapping:
                print("🔍 [KPI_EDITOR] No columns need entity mapping - using generic approach")
            
            return columns_needing_mapping
            
        except Exception as e:
            print(f"⚠️ [KPI_EDITOR] Error analyzing mapping needs: {str(e)}")
            # Fallback: assume all columns need mapping (current behavior)
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
        
        # Format metadata for prompt
        if metadata_results:
            formatted_columns = []
            for col in metadata_results:
                col_name = col.get('column_name', '')
                col_desc = col.get('description', '')
                col_type = col.get('data_type', '')
                col_score = col.get('score', 0)
                formatted_columns.append(f"- {col_name} ({col_type}): {col_desc} (relevance: {col_score:.2f})")
            metadata_text = "\n".join(formatted_columns)
        else:
            metadata_text = "No metadata available"
        
        return f"""
        DONOT REWRITE THE SQL QUERY, ONLY MODIFY IT TO MATCH THE USER REQUEST.
        If only a simple modification is needed, do not rewrite the entire SQL query. Only add the necessary modifications to the original SQL query. Keep the changes minimal and relevant.
        Modify this SQL to match the user request: "{task}"
        
        Original KPI: {kpi_metric}
        Original SQL: {original_sql}
        
        Available columns: {metadata_text}
        {values_text}
        
        IMPORTANT SQL SERVER BIT COLUMN HANDLING:
        If any columns are of type 'bit' (e.g., [Preventable Flag], [Is Critical Flag], [Is Divided Highway Flag]), 
        you cannot use aggregate functions like MAX(), MIN(), SUM(), AVG() directly on bit columns in SQL Server.
        Instead, convert bit to int first: MAX(CAST([column_name] AS INT)) or use CASE statements for filtering.
        
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
    
