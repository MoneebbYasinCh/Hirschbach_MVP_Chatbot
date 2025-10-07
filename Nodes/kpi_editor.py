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
            temperature=0.0
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
        
        # Get user input from the first HumanMessage (proper LangGraph pattern)
        task = ""
        for msg in messages:
            if hasattr(msg, 'content') and hasattr(msg, '__class__'):
                if 'Human' in str(msg.__class__):
                    task = msg.content
                    break
        
        if not task:
            # Fallback: use the last message
            task = messages[-1].content if hasattr(messages[-1], 'content') else str(messages[-1])
        
        print(f" [KPI_EDITOR] Extracted task: '{task}'")
        
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
            # Step 1: Analyze what additional columns are needed
            needed_columns = self._analyze_needed_columns_step1(task, metadata_results, original_sql)
            
            # Step 2: Intelligently decide which columns need entity mapping
            columns_needing_mapping = self._analyze_columns_needing_mapping(task, needed_columns, state)
            
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
                print(f" [KPI_EDITOR] No changes made to SQL")
                print(f" [KPI_EDITOR] Final SQL: {edited_sql}")
                modifications = ["No changes needed"]
            else:
                print(f" [KPI_EDITOR] Successfully modified KPI SQL")
                print(f" [KPI_EDITOR] Modified SQL: {edited_sql}")
                modifications = ["Modified SQL query to better match user requirements"]
            
            return self._set_success_state(state, edited_sql, modifications)
            
        except Exception as e:
            print(f" [KPI_EDITOR] Error: {str(e)}")
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
    
    
    def _analyze_needed_columns_step1(self, task: str, metadata_results: List[Dict[str, Any]], original_sql: str) -> List[str]:
        """Intelligently pick additional columns from metadata results based on task, existing SQL, and column descriptions"""
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
        
        # Smart selection prompt that considers existing SQL and user task
        analysis_prompt = f"""
        Task: "{task}"
        
        Current KPI SQL Query:
        {original_sql}
        
        Available additional columns from metadata retrieval:
        {chr(10).join(column_details)}
        
        ANALYSIS: 
        1. First, analyze what columns are already used in the current SQL query
        2. Then, based on the user task, determine what ADDITIONAL columns are needed for filtering, grouping, or analyzing the data
        3. Only select columns that are NOT already in the SQL query but are needed for the user's request
        
        IMPORTANT SELECTION RULES (APPLY ALL):
        - If both a CODE column and a NAME column exist for the same entity, you MUST include BOTH in the needed columns.
          Example known pair: Driver Manager (code) + Driver Manager Name (name)
        - ALWAYS prefer the CODE column for filtering/grouping and is not null; the NAME column is for display and NOT NULL checks.

        OUTPUT FORMAT (CRITICAL):
        - Return ONLY a comma-separated list of column names from Available column names.
        - If a pair exists, both names MUST appear in the output list.
        - Example valid output: Driver Manager, Driver Manager Name, Occurrence Date
        
        Available column names: {', '.join(available_columns)}

        examples:
        - "show distribution of claims across different claim categories this month" → Create Time (filter for this month)
        
        Return only the exact column names from the list above that are needed as ADDITIONS to the existing SQL, separated by commas. If no additional columns are needed, return "none".
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
                        print(f" [KPI_EDITOR] Selected additional column: {col}")
            
        except Exception as e:
            print(f" [KPI_EDITOR] Error analyzing needed columns: {str(e)}")
        
        if not needed_columns:
            print(" [KPI_EDITOR] No additional columns needed - existing SQL is sufficient")
        
        return needed_columns
    
    def _analyze_columns_needing_mapping(self, task: str, needed_columns: List[str], state: Dict[str, Any] = None) -> List[str]:
        """Intelligently decide which columns actually need entity mapping based on user request"""
        if not needed_columns:
            return []

        # Get column metadata to understand data types and relationships
        column_metadata = {}
        if state:
            for col in needed_columns:
                # Find metadata for this column to understand its data type and description
                for meta in state.get("metadata_rag_results", []):
                    if meta.get("column_name") == col:
                        column_metadata[col] = {
                            "data_type": meta.get("data_type", ""),
                            "description": meta.get("description", "")
                        }
                        break

        # Create intelligent prompt with column context
        column_context = []
        for col, meta in column_metadata.items():
            column_context.append(f"- {col} ({meta['data_type']}): {meta['description']}")

        analysis_prompt = f"""
        User request: "{task}"
        
        Available columns with context:
        {chr(10).join(column_context) if column_context else ', '.join(needed_columns)}
        
        CRITICAL: You MUST identify ANY specific constraints the user mentions, even if the main request seems generic.

        INCIDENT TYPE DETECTION (HIGH PRIORITY):
        - When user mentions specific incident types like "crash", "cargo", "work comp", "Work Comp", look for columns that categorize incidents
        - Common incident type columns: "Accident or Incident Code", "Incident Type", "Claim Type", "Category"
        - If user asks for "crash claims", "cargo claims", "work comp claims" - this requires incident type filtering

        NEED SPECIFIC HANDLING if user mentions ANY of these:
        
        1. TEMPORAL CONSTRAINTS (ALWAYS needs specific handling):
           - "this month", "last week", "today", "yesterday", "this year", "last month"
           - "for this month specifically", "current month", "recent claims"
           - ANY time-based filtering requirement
        
        2. CATEGORICAL VALUES (specific values mentioned):
           - Status: "closed", "open", "pending", "resolved"
           - Incident types: "crash", "cargo", "work comp", "Work Comp", "accident", "incident"
           - Customer codes: specific customer names or codes
           - Locations: "Texas", "California", "North region", specific cities, states
        
        3. NUMERIC FILTERS (value-based constraints):
           - "over $10k", "high-value", "expensive claims", "low-cost"
           - "critical claims", "major incidents", "significant amounts"
        
        4. CONDITIONAL LOGIC (specific conditions):
           - "preventable", "critical", "divided highway", "minor"
           - "warehouse incidents", "roadway crashes", "close quarters"
        
        DON'T NEED SPECIFIC HANDLING only if:
        - Purely generic grouping: "show claims by type" (no specific values)
        - Generic aggregation: "group by status" (no specific status mentioned)
        - Generic counting: "count by customer" (no specific customer)

        CONTEXT-AWARE ANALYSIS:
        - If user mentions "crash claims" → look for incident/accident categorization columns
        - If user mentions "work comp claims" → look for worker compensation categorization
        - If user mentions "cargo claims" → look for cargo/damage categorization
        - Consider column descriptions to understand what each column represents

        REAL EXAMPLES FROM DATA:
        - "this month specifically" → Occurrence Date (temporal constraint)
        - "show closed claims" → Status Flag (categorical: "closed")
        - "crash claims" → Accident or Incident Code (categorical: "Crash")
        - "Work Comp claims" → Accident or Incident Code (categorical: "Work Comp")
        - "claims in Texas" → Claim City (categorical: "Texas")
        - "high-value claims" → Actual Recovered Amount (numeric filter)
        - "critical claims only" → Is Critical Flag (conditional: 1)
        - "show claims by type" → none (generic grouping)
        CODE/NAME PAIRS RULE:
        - If both a CODE and NAME column exist for the same entity, prefer the CODE column for filtering/grouping.
        - Keep the NAME column for display and NOT NULL checks, but do not map name values when a code value exists.


        ANALYSIS FOR: "{task}"
        Look for ANY specific constraints, temporal references, exact values, or conditions mentioned.
        Pay special attention to incident type mentions like "crash", "cargo", "work comp".

        Return column names that need specific handling, separated by commas. If none, return "none".
        """
        
        try:
            print(f" [KPI_EDITOR] Step 2 Input - Task: '{task}'")
            print(f" [KPI_EDITOR] Step 2 Input - Available columns: {needed_columns}")
            print(f" [KPI_EDITOR] Step 2 Prompt Preview: {analysis_prompt[:200]}...")
            
            response = self.llm.invoke(analysis_prompt)
            analysis_result = response.content.strip()
            
            print(f" [KPI_EDITOR] Step 2 LLM Response: '{analysis_result}'")
            
            columns_needing_mapping = []
            if analysis_result.lower() != "none" and analysis_result:
                # Parse the selected column names
                selected_columns = [col.strip() for col in analysis_result.split(',') if col.strip()]
                
                # Only keep columns that exist in needed_columns
                for col in selected_columns:
                    if col in needed_columns and col not in columns_needing_mapping:
                        columns_needing_mapping.append(col)
                        print(f" [KPI_EDITOR] Column needs specific handling: {col}")
            
            if not columns_needing_mapping:
                print(" [KPI_EDITOR] No columns need specific handling - using generic approach")
            
            return columns_needing_mapping
            
        except Exception as e:
            print(f" [KPI_EDITOR] Error analyzing mapping needs: {str(e)}")
            # Fallback: assume all columns need mapping (current behavior)
            return needed_columns
    
    def _map_user_intent_to_values_step2(self, task: str, needed_columns: List[str], entity_mapping_data: str) -> Dict[str, Any]:
        """Step 4: Map user intent to exact values and logic using LLM"""
        if not needed_columns:
            return {}
        
        # Enhanced mapping prompt for temporal, numeric, categorical, and incident type logic
        prompt = f"""
        User request: "{task}"
        Columns: {', '.join(needed_columns)}
        Available values: {entity_mapping_data}

        CRITICAL: Distinguish between FILTERING queries vs AGGREGATION queries:

        === DO NOT MAP (Return "unclear" for these columns) ===
        If the query asks for comparison/aggregation across ALL entities:
        - Comparison keywords: "which", "what", "compare", "versus", "vs"
        - Aggregation keywords: "distribution of", "breakdown by", "for each", "by", "across"
        - Ranking keywords: "top", "lowest", "highest", "rank", "most", "least", "best", "worst"
        - Grouping keywords: "all", "every", "each type of", "group by"

        Examples of when NOT to map:
        - "Which customer has the lowest sum?" → Do NOT map Customer Code (need all customers)
        - "What statuses have the most claims?" → Do NOT map Status Flag (need all statuses)
        - "Compare claims by type" → Do NOT map claim type (need all types)
        - "Distribution of claims across categories" → Do NOT map categories (need all)

        === DO MAP (Return logic_type:value) ===
        Only map when user specifies EXACT entities/values to filter:

        Map user intent to exact values and logic. Handle:
        1. Categorical values: "closed" → categorical:Closed
        2. Temporal logic: "this month", "Q1 of 2024" → temporal:current_month or temporal:2024-Q1
        3. Numeric filters: "over $10k" → numeric:>10000
        4. Conditional logic: "critical" → conditional:1
        5. Incident types: "crash claims" → categorical:Crash, "work comp claims" → categorical:Work Comp

        INCIDENT TYPE MAPPING (IMPORTANT):
        - When user mentions "crash claims" → map to categorical:Crash
        - When user mentions "cargo claims" → map to categorical:Cargo
        - When user mentions "work comp claims" or "Work Comp claims" → map to categorical:Work Comp
        - Look for incident categorization columns: "Accident or Incident Code", "Incident Type", "Claim Type"

        CODE/NAME PAIRS MAPPING RULE:
        - Prefer mapping values for CODE columns when a corresponding NAME column exists.
        - Do not map NAME values if a CODE value is available; use NAME only for display/NOT NULL checks.

        Format: Column1: logic_type:value, Column2: logic_type:value

        Examples of when TO map:
        - Status Flag: categorical:Closed (if user asks for "closed claims")
        - Accident or Incident Code: categorical:Crash (if user asks for "crash claims")
        - Accident or Incident Code: categorical:Work Comp (if user asks for "work comp claims")
        - Preventable Flag: categorical:P (if user asks for "preventable claims")
        - Occurrence Date: temporal:current_month (if user asks for "this month")
        - Occurrence Date: temporal:2024-Q1 (if user asks for "Q1 of 2024")
        - Claim Amount: numeric:>10000 (if user asks for "over $10k")
        - Is Critical Flag: conditional:1 (if user asks for "critical claims")

        Examples of when NOT to map (return "unclear"):
        - Customer Code: unclear (if user asks "which customer has lowest...")
        - Status Flag: unclear (if user asks "distribution by status")
        - Accident or Incident Code: unclear (if user asks "show claims by incident type")

        ANALYSIS FOR: "{task}"
        Does this ask to compare/aggregate across ALL values, or filter to specific values?
        Pay special attention to incident type mentions like "crash", "cargo", "work comp".
        """
        
        try:
            response = self.llm.invoke(prompt)
            mapping_result = response.content.strip()
            
            # Parse the mapping result with logic types
            mapped_values = {}
            for line in mapping_result.split('\n'):
                if ':' in line:
                    column, logic_spec = line.split(':', 1)
                    column = column.strip()
                    logic_spec = logic_spec.strip()
                    
                    if logic_spec != "unclear":
                        # Parse logic_type:value format
                        if ':' in logic_spec:
                            logic_type, value = logic_spec.split(':', 1)
                            mapped_values[column] = {
                                'type': logic_type.strip(),
                                'value': value.strip()
                            }
                            print(f" [KPI_EDITOR] Mapped {column} to: {logic_type.strip()}:{value.strip()}")
                        else:
                            # Fallback for simple values
                            mapped_values[column] = {
                                'type': 'categorical',
                                'value': logic_spec
                            }
                            print(f" [KPI_EDITOR] Mapped {column} to: categorical:{logic_spec}")
                    else:
                        print(f" [KPI_EDITOR] Could not map {column} - unclear intent")
            
            return mapped_values
            
        except Exception as e:
            print(f" [KPI_EDITOR] Error mapping user intent to values: {str(e)}")
            return {}
    
    def _create_sql_generation_prompt_step3(self, task: str, kpi_metric: str, kpi_description: str, original_sql: str, metadata_results: List[Dict[str, Any]], mapped_values: Dict[str, Any]) -> str:
        """Step 3: Create focused SQL generation prompt"""
        
        # Format mapped values with logic types
        values_text = ""
        if mapped_values:
            logic_instructions = []
            for column, logic_info in mapped_values.items():
                logic_type = logic_info.get('type', 'categorical')
                value = logic_info.get('value', '')
                
                if logic_type == 'temporal':
                    if value == 'current_month':
                        logic_instructions.append(f"{column}: Add WHERE MONTH([{column}]) = MONTH(GETDATE()) AND YEAR([{column}]) = YEAR(GETDATE())")
                    elif value == 'current_week':
                        logic_instructions.append(f"{column}: Add WHERE [{column}] >= DATEADD(week, -1, GETDATE())")
                    elif value == 'today':
                        logic_instructions.append(f"{column}: Add WHERE [{column}] = CAST(GETDATE() AS DATE)")
                    elif value.startswith('2024-Q1') or value.startswith('2024-Q2') or value.startswith('2024-Q3') or value.startswith('2024-Q4'):
                        # Handle quarter logic like "Q1 of 2024"
                        year_quarter = value.split('-')
                        if len(year_quarter) == 2:
                            year = year_quarter[0]
                            quarter = year_quarter[1][1:]  # Remove 'Q' prefix
                            if quarter == '1':
                                logic_instructions.append(f"{column}: Add WHERE [{column}] >= '{year}-01-01' AND [{column}] < '{year}-04-01'")
                            elif quarter == '2':
                                logic_instructions.append(f"{column}: Add WHERE [{column}] >= '{year}-04-01' AND [{column}] < '{year}-07-01'")
                            elif quarter == '3':
                                logic_instructions.append(f"{column}: Add WHERE [{column}] >= '{year}-07-01' AND [{column}] < '{year}-10-01'")
                            elif quarter == '4':
                                logic_instructions.append(f"{column}: Add WHERE [{column}] >= '{year}-10-01' AND [{column}] < '{int(year)+1}-01-01'")
                elif logic_type == 'numeric':
                    logic_instructions.append(f"{column}: Add WHERE [{column}] {value}")
                elif logic_type == 'conditional':
                    logic_instructions.append(f"{column}: Add WHERE [{column}] = {value}")
                elif logic_type == 'categorical':
                    logic_instructions.append(f"{column}: Add WHERE [{column}] = '{value}'")
            
            if logic_instructions:
                values_text = f"Apply these specific logic rules:\n" + "\n".join(logic_instructions)
        
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
        TASK: Modify the original SQL query to match the user request: "{task}"
        CRITICAL: Answer with ONLY the SQL query. No explanations, no markdown, no code blocks, no additional text.
        Just the pure SQL statement.
        Original KPI: {kpi_metric}
        Original SQL: {original_sql}
        
        Available columns: {metadata_text}
        {values_text}
        
        INSTRUCTIONS:
        1. Start with the original SQL query
        2. Add necessary WHERE clauses or other modifications
        3. Keep the original SELECT and FROM structure
        4. Only add the minimal changes needed to fulfill the user request
        5. If adding a TOP clause, it must be placed immediately after SELECT (e.g., SELECT TOP 1)
        6. ALWAYS enforce NOT NULL conditions for EVERY column included in GROUP BY:
           - ONLY use TRIM() on string types (varchar, nvarchar, char). Check data_type in metadata.
           - NEVER TRIM: date, datetime, int, decimal, bit, numeric, money columns.
           - String columns: WHERE TRIM([Column]) <> '' AND [Column] IS NOT NULL
           - Non-string columns: WHERE [Column] IS NOT NULL
        
        INCIDENT TYPE FILTERING (IMPORTANT):
        - When user mentions "crash claims", filter by [Accident or Incident Code] = 'Crash'
        - When user mentions "work comp claims" or "Work Comp claims", filter by [Accident or Incident Code] = 'Work Comp'
        - When user mentions "cargo claims", filter by [Accident or Incident Code] = 'Cargo'
        - Apply incident type filters in addition to other WHERE conditions

        When deciding on which date column to use:
        If in the user request, it says something related to "open claims", use the column "Opened Date" for date filtering.
        If in the user request, it says something related to "closed claims", use the column "Close Date" for date filtering.
        If in the user request, there isnt mention of open or closed claims, use the column "Occurrence Date" for date filtering.
        If in the user request, the user mentions a specific date column name, use that column for date filtering by matching it with the column present in the available columns.
        
        ENTITY PAIRS AND FILTERING RULES:
        - If both a CODE column and a NAME column exist for the same entity (e.g., [Driver Manager] code, [Driver Manager Name] name), then:
          1) Prefer using the CODE column in WHERE/GROUP BY for filtering/grouping.
          2) SELECT both columns for readability (code + name).
          3) Apply NOT NULL checks to NAME column. Only TRIM if NAME is string type (check data_type).
        
        CRITICAL GROUP BY RULE (ALWAYS APPLY):
        - ALWAYS use CODE columns in GROUP BY, NEVER NAME columns (unless user explicitly requests grouping by name).
        - Default behavior: GROUP BY [Code Column], then SELECT both [Code Column] and [Name Column] for display.
        - Example: GROUP BY [Customer Code] - then SELECT [Customer Code], [Customer Name]
        - NEVER: GROUP BY [Customer Code], [Customer Name] (this is wrong!)
        - Only exception: If user explicitly says something like "group by customer name" or "group by name"
        CRITICAL SQL SERVER SYNTAX RULES:
        - ALL column names with spaces MUST be wrapped in square brackets: [Column Name]
        - Use proper SQL Server date functions: MONTH(), YEAR(), GETDATE()
        - For date filtering, use: WHERE ["any date column"] >= 'YYYY-MM-DD' AND ["any date column"] < 'YYYY-MM-DD'
        - LIMIT, FETCH FIRST, and similar clauses are NOT valid syntax in SQL Server. Use only:
          - For TOP N: SELECT TOP N ... FROM ... (must be first in SELECT clause)
          - For OFFSET/FETCH: ... ORDER BY ... OFFSET X ROWS FETCH NEXT Y ROWS ONLY (requires ORDER BY)

        DEFAULT SORTING (unless the user specifies otherwise):
        - If the query has an aggregate metric or a date/time column in SELECT, ORDER BY that metric/date DESC.
        
        IMPORTANT SQL SERVER BIT COLUMN HANDLING:
        If any columns are of type 'bit' (e.g., [Preventable Flag], [Is Critical Flag], [Is Divided Highway Flag]), 
        you cannot use aggregate functions like MAX(), MIN(), SUM(), AVG() directly on bit columns in SQL Server.
        Instead, convert bit to int first: MAX(CAST([column_name] AS INT)) or use CASE statements for filtering.
        
        
        """
    
    def _get_entity_mapping_data(self, needed_columns: List[str]) -> str:
        """Get entity mapping data for the specific columns that are needed"""
        entity_data = []
        
        if not needed_columns:
            return "No additional columns needed for filtering"
        
        for column_name in needed_columns:
            try:
                result = self.entity_tool.get_column_values(column_name)
                if result and result.get("success", False):
                    values = result.get("values", [])
                    entity_data.append(f"- {column_name}: {values}")
                    print(f" [KPI_EDITOR] Added entity mapping for {column_name}: {values}")
                else:
                    print(f" [KPI_EDITOR] No values found for column: {column_name}")
            except Exception as e:
                print(f" [KPI_EDITOR] Error getting values for {column_name}: {str(e)}")
        
        if not entity_data:
            return "No exact values available for the needed columns"
        
        return "\n".join(entity_data)
    
