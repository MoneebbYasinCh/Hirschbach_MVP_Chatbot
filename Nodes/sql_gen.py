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
        
        # Get user input from the latest message (same pattern as KPI editor)
        messages = state.get("messages", [])
        if not messages:
            print("[SQL GENERATION] No messages found")
            state["sql_generation_status"] = "error"
            state["sql_generation_error"] = "No messages found in state"
            return state
        
        # Get user input from the first HumanMessage (proper LangGraph pattern)
        user_query = ""
        for msg in messages:
            if hasattr(msg, 'content') and hasattr(msg, '__class__'):
                if 'Human' in str(msg.__class__):
                    user_query = msg.content
                    break
        
        if not user_query:
            # Fallback: use the last message
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
            print(f"🔍 [SQL GENERATION] Generated SQL: {final_sql}")
            print(f"🔍 [SQL GENERATION] Setting sql_validated = True")
            print(f"🔍 [SQL GENERATION] State keys after update: {list(state.keys())}")
            print(f"🔍 [SQL GENERATION] sql_validated value: {state.get('sql_validated')}")
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
        TASK: Select the minimal set of columns needed to answer the user's request.

        USER REQUEST:
        "{user_query}"

        AVAILABLE COLUMNS (name, type, description, relevance):
        {chr(10).join(column_details)}

        SELECTION RULES (APPLY ALL):
        - Choose only columns necessary for filtering, grouping, or producing the result.
        - Prefer columns whose descriptions semantically match the request.
        - Pairing rule: If a NAME-style column is selected, you MUST also select its corresponding CODE-style column for the same entity.
          • Prefer CODE columns for filtering/grouping.
          • Use NAME columns for display and NOT NULL checks.
        - How to detect pairs (semantic): two columns clearly referring to the same entity where one is a human-readable label (often ends with "Name" or says "Full name") and the other is an identifier/code (mentions "code", "identifier", or is short alphanumeric).

        AVAILABLE COLUMN NAMES:
        {', '.join(available_columns)}

        OUTPUT (CRITICAL):
        - Return ONLY valid JSON with this exact shape and nothing else:
          {{"needed_columns": ["Column A", "Column B", "..."]}}
        - Every item MUST be taken from AVAILABLE COLUMN NAMES.
        - If a NAME column appears, its CODE counterpart MUST also appear.
        - Example:
          {{"needed_columns": ["Entity Code", "Entity Name", "Occurrence Date"]}}
        """
        
        try:
            response = self.llm.invoke(analysis_prompt)
            analysis_result = response.content.strip()
            
            # Try JSON first
            try:
                import json
                parsed = json.loads(analysis_result)
                selected_columns = parsed.get("needed_columns", []) if isinstance(parsed, dict) else []
            except Exception:
                # Fallback to comma-separated parsing
                selected_columns = [col.strip() for col in analysis_result.split(',') if col.strip() and analysis_result.lower() != "none"]

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
                if result and result.get("success", False):
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
        
        IMPORTANT: Map the user's intent to the most appropriate values according to the available values and the input prompt:
        - For "preventable claims" → use "P" (Preventable) from Preventable Flag
        - For "non-preventable claims" → use "N" (Non-preventable) from Preventable Flag
        - For "current month" → use appropriate date filtering
        - Match the user's specific request to the exact values that would filter the data correctly
        
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
        
        Use SQL Server syntax: wrap column names with spaces in square brackets [Column Name]
        Use table name: PRD.CLAIMS_SUMMARY
        For date filtering, use SQL Server functions like DATEPART, YEAR, MONTH instead of DATE_TRUNC

        When deciding on which date column to use:
        If in the user request, it says something related to "open claims", use the column "Opened Date" for date filtering.
        If in the user request, it says something related to "closed claims", use the column "Close Date" for date filtering.
        If in the user request, there isnt mention of open or closed claims, use the column "Occurrence Date" for date filtering.
        If in the user request, the user mentions a specific date column name, use that column for date filtering by matching it with the column present in the available columns.

        CRITICAL COLUMN PAIR RULE (apply strictly) (this is just one example):
        - If both [Driver Manager] (code) and [Driver Manager Name] (name) exist:
          1) You MUST use [Driver Manager] in WHERE and GROUP BY for all filtering/grouping.
          2) You MUST also SELECT [Driver Manager Name] for readability.
          3) If non-null name is requested, apply IS NOT NULL and TRIM([Driver Manager Name]) <> '' to the NAME column only.
          4) NEVER filter by [Driver Manager Name]. Always filter by [Driver Manager].
        The same should be done for other code and name pairs like the example given above.

        IMPORTANT: Filter out null and empty values for better data quality where necessary:
        - Use WHERE [Column] IS NOT NULL AND TRIM([Column]) <> '' for string columns
        - Use WHERE [Column] IS NOT NULL for numeric/date columns
        - This ensures accurate counts and prevents null values from skewing results when needed.
        
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