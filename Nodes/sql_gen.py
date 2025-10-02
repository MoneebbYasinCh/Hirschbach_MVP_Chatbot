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
            final_sql = self._generate_final_sql(user_query, needed_columns, metadata_results, mapped_values)
            
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
        You are SQL expert with 10+ years of experience. Your role is to select the minimal set of columns needed to answer the user's request for generating an SQL query.

        ## User Request:
        "{user_query}"
        
        IMPORTANT: Pay special attention to terms like "driver leader" - this refers to the DRIVER'S MANAGER/SUPERVISOR, not the driver themselves. Look for "Driver Manager" columns when you see "leader" in driver context.

        AVAILABLE COLUMNS (name, type, description, relevance):
        {chr(10).join(column_details)}

        SELECTION RULES (APPLY ALL):
        - Choose only columns necessary for filtering, grouping, or producing the result.
        - Prefer columns whose descriptions semantically match the request.
        - Pairing rule: If a NAME-style column is selected, you MUST also select its corresponding CODE-style column for the same entity.
          • Prefer CODE columns for filtering/grouping.
          • Use NAME columns for display and NOT NULL checks.
        - How to detect pairs (semantic): two columns clearly referring to the same entity where one is a human-readable label (often ends with "Name" or says "Full name") and the other is an identifier/code (mentions "code", "identifier", or is short alphanumeric).

        CRITICAL SEMANTIC MAPPING:
        When the user mentions these terms, map them to these specific column patterns:
        - "driver leader", "driver supervisor", "driver manager" → Look for columns containing "Driver Manager" (both Name and Code versions)
        - "leader", "supervisor", "manager" (in driver context) → Look for "Driver Manager" columns
        - "top driver", "best driver", "driver performance" → Look for "Driver" columns (both Name and Code versions)
        - "claims", "accidents", "incidents" → Look for accident/claim related columns
        - "crash", "collision" → Look for "Accident or Incident Code" or similar

        DATE COLUMN PRIORITY RULES:
        When selecting date columns for time-based analysis, prioritize in this order:
        1. "Occurrence Date" - Primary field for when incidents actually happened
        2. "Create Date" - When records were created in system
        3. "Create DateTime" - When records were created (with time)
        4. Other date columns based on context
        
        For claims analysis, ALWAYS prefer "Occurrence Date" over "Create Date/DateTime" unless user specifically asks about when claims were filed/created.

        AVAILABLE COLUMN NAMES:
        {', '.join(available_columns)}

        OUTPUT (CRITICAL):
        - Return ONLY valid JSON with this exact shape and nothing else:
          {{"needed_columns": ["Column A", "Column B", "..."]}}
        - Every item MUST be taken from AVAILABLE COLUMN NAMES.
        - If a NAME column appears, its CODE counterpart MUST also appear.
        - Example:
          {{"needed_columns": ["Entity Code", "Entity Name", "Occurrence Date"]}}
        
        SPECIFIC EXAMPLES:
        - For "driver leader" queries: {{"needed_columns": ["Driver Manager", "Driver Manager Name", "Occurrence Date", "Accident or Incident Code"]}}
        - For time-based claims analysis: {{"needed_columns": ["Driver Code", "Driver Name", "Occurrence Date"]}}
        - For "last 6 months" claims: Use "Occurrence Date", NOT "Create Date" or "Create DateTime"
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
        
        CRITICAL: Distinguish between FILTERING queries vs AGGREGATION queries:
        
        === DO NOT MAP (Return "none" for these columns) ===
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
        
        === DO MAP (Return exact values) ===
        Only map when user specifies EXACT entities/values to filter:
        - Specific entity: "Show me Walmart claims" → Map Customer Code to specific value
        - Specific status: "Show me preventable claims" → Map Preventable Flag to "P"
        - Specific category: "closed claims only" → Map Status Flag to "Closed"
        - Specific time: "current month" → Map to appropriate date filtering
        
        Examples of when TO map:
        - "preventable claims" → Preventable Flag: P
        - "non-preventable claims" → Preventable Flag: N
        - "Walmart customer" → Customer Code: WALMART (if exists in values)
        - "closed claims" → Status Flag: Closed
        
        ANALYSIS FOR: "{user_query}"
        Does this ask to compare/aggregate across ALL values, or filter to specific values?
        
        Map user intent to exact values. Format: Column1: value1, Column2: value2
        If no specific values needed (aggregation/comparison query), return: none
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
    
    def _generate_final_sql(self, user_query: str, needed_columns: List[str], metadata_results: List[Dict], mapped_values: Dict[str, str]) -> str:
        """Generate final SQL with exact values (like KPI editor)"""
        
        # Format metadata for prompt - ONLY use needed columns
        if metadata_results and needed_columns:
            formatted_columns = []
            # Create a lookup dict for faster access
            metadata_lookup = {col.get('column_name', ''): col for col in metadata_results}
            
            # Only format the needed columns
            for col_name in needed_columns:
                if col_name in metadata_lookup:
                    col = metadata_lookup[col_name]
                    col_desc = col.get('description', '')
                    col_type = col.get('data_type', '')
                    formatted_columns.append(f"- {col_name} ({col_type}): {col_desc}")
            
            metadata_text = "\n".join(formatted_columns)
            print(f"🔧 [SQL_GEN] Debug - Filtered columns for SQL generation: {needed_columns}")
            print(f"🔧 [SQL_GEN] Debug - Formatted metadata text: {metadata_text}")
        else:
            metadata_text = "No metadata available"
        
        # Format mapped values
        values_text = ""
        if mapped_values:
            values_text = f"Use these exact values: {mapped_values}"
        
        prompt = f"""
        # Professional SQL Query Generation Prompt

You are an expert SQL query generator specializing in SQL Server syntax. Your task is to convert natural language requests into precise, optimized SQL queries following strict guidelines and best practices.

## CRITICAL REMINDER
- Use ONLY the columns provided in the AVAILABLE COLUMNS section below
- Do NOT substitute similar-sounding column names
- Use the EXACT column names provided - no substitutions or variations

## Your Mission
Analyze the user's request step-by-step and generate a SQL query that accurately fulfills their requirements while adhering to all formatting, filtering, and structural rules defined below.

---

## Step-by-Step Process

### Step 1: Understand the User Request
- Parse the **USER REQUEST**: "{user_query}"
- Identify what data the user wants to retrieve
- Determine any filtering conditions, aggregations, or groupings needed

        ### Step 2: Review Available Schema
        - **AVAILABLE COLUMNS**: {metadata_text}
        - **MAPPED VALUES**: {values_text}
        - **CRITICAL**: You MUST ONLY use the columns listed above in the AVAILABLE COLUMNS section
        - **CRITICAL**: Do NOT substitute similar-sounding columns (e.g., don't use "Entity Code" if "Entity Manager" is provided)
        - **CRITICAL**: Use the EXACT column names provided - no substitutions or variations
        - Match user's intent with appropriate columns from the metadata
        - Verify data types for each column you plan to use

### Step 3: Apply SQL Server Syntax Rules
- **Table Name**: Use `PRD.CLAIMS_SUMMARY`
- **Column Names with Spaces**: Wrap in square brackets: `[Column Name]`
- **Date Functions**: Use SQL Server functions like `DATEPART`, `YEAR`, `MONTH` instead of `DATE_TRUNC`

### Step 3.5: CRITICAL SQL Server Syntax Requirements
**LIMIT CLAUSE REPLACEMENT (CRITICAL):**
- **NEVER use `LIMIT`** - SQL Server does NOT support LIMIT clause
- **Use `TOP` for simple row limiting:**
  ```sql
  SELECT TOP 10 [Column1], [Column2] FROM PRD.CLAIMS_SUMMARY
  ```
- **Use `OFFSET FETCH` for pagination:**
  ```sql
  SELECT [Column1], [Column2] FROM PRD.CLAIMS_SUMMARY
  ORDER BY [Column1]
  OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY
  ```

**SQL Server Specific Syntax:**
- **String Concatenation**: Use `+` instead of `||` or `CONCAT()`
- **Date Formatting**: Use `FORMAT()` or `CONVERT()` instead of `TO_CHAR()`
- **Case Sensitivity**: Use `COLLATE` if needed for case-sensitive operations
- **Boolean Logic**: Use `BIT` type, not `BOOLEAN`

**COMMON SQL SERVER PATTERNS:**
```sql
-- Top N results (replace LIMIT N)
SELECT TOP 5 [Driver Manager], [Driver Manager Name], COUNT(*) AS Claims
FROM PRD.CLAIMS_SUMMARY
GROUP BY [Driver Manager], [Driver Manager Name]
ORDER BY Claims DESC

-- Pagination (replace LIMIT offset, count)
SELECT [Column1], [Column2]
FROM PRD.CLAIMS_SUMMARY
ORDER BY [Column1]
OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY
```

### Step 4: Determine Date Column Logic
Apply this decision tree for date filtering:
- If user mentions **"open claims"** → Use `[Opened Date]`
- If user mentions **"closed claims"** → Use `[Close Date]`
- If user mentions a **specific date column name** → Use that exact column (match with available columns)
- If **no date context** is provided → Use `[Occurrence Date]` as default

### Step 4.5: Apply Incident Type Filtering
- If user mentions specific incident types (crash, accident, etc.) → Look for incident/code columns in available schema and filter accordingly
- Match the user's terminology to the appropriate code column and values

### Step 5: Apply CRITICAL COLUMN PAIR RULE
When CODE and NAME column pairs exist (e.g., `[Entity Manager]` and `[Entity Manager Name]`):

1. **ALWAYS use CODE column** in `GROUP BY` clauses and NEVER the NAME column unless user explicitly requests it.
2. **ALWAYS SELECT both** CODE and NAME columns for readability in SELECT clause.
3. **Apply `IS NOT NULL`** to the NAME column AND in the CODE column if the column is not nullable.
4. **Only use `TRIM()`** if NAME column is string type (check `data_type` in metadata)
5. **NEVER filter by NAME column** - always filter by CODE column

        **Example Pattern:**
        ```
        WHERE [Entity Manager] = 'some_value'  -- Filter by CODE
          AND [Entity Manager Name] IS NOT NULL  -- NULL check on NAME
        GROUP BY [Entity Manager]  -- Group by CODE ONLY (never group by NAME)
        SELECT [Entity Manager], [Entity Manager Name]  -- Display both
        ```

        **WRONG Example (NEVER do this):**
        ```
        GROUP BY [Entity Manager], [Entity Manager Name]  -- ❌ WRONG! Don't group by NAME columns
        ```

Apply this same pattern to ALL code/name column pairs.

### Step 6: Apply CRITICAL GROUP BY RULE
**Default Behavior (ALWAYS follow unless explicitly overridden):**
- **ALWAYS use ONLY CODE columns in GROUP BY**
- **NEVER use NAME columns in GROUP BY** (unless user explicitly requests grouping by name)
- **SELECT both CODE and NAME** for display purposes
- **CRITICAL:** NAME columns are for display only - they should NEVER appear in GROUP BY clauses

        **Correct Pattern:**
        ```
        GROUP BY [Entity Code]
        SELECT [Entity Code], [Entity Name]
        ```

        **WRONG Pattern (NEVER do this):**
        ```
        GROUP BY [Entity Code], [Entity Name]  -- ❌ WRONG! NAME columns don't belong in GROUP BY
        ```

**Exception:** Only group by NAME if user explicitly says "group by [name column]" or "group by name"

**REMEMBER:** CODE columns are unique identifiers for grouping. NAME columns are human-readable labels for display only.

### Step 7: Enforce Data Quality Filters
Apply appropriate null and empty value filters based on data type:

**For String Types** (varchar, nvarchar, char):
```
WHERE TRIM([Column]) <> '' AND [Column] IS NOT NULL
```

**For Non-String Types** (date, datetime, int, decimal, bit, numeric, money):
```
WHERE [Column] IS NOT NULL
```

**CRITICAL:** Check `data_type` in metadata before using `TRIM()` - NEVER TRIM non-string columns!

**ALWAYS enforce `IS NOT NULL` for EVERY column in GROUP BY clause.**

### Step 8: Apply Default Sorting
Unless user specifies otherwise:
- If result contains a **numeric aggregate** (COUNT, SUM, AVG, etc.) → `ORDER BY` that column `DESC`
- If result contains a **date column** → `ORDER BY` that column `DESC`
- Default to descending order for better insights (most recent/highest values first)

---

## Output Format
Return **ONLY** the SQL query. No explanations, no markdown code blocks, no additional text.

---

## Final Checklist Before Generating Query
- [ ] Correct table name used: `PRD.CLAIMS_SUMMARY`
- [ ] **CRITICAL:** ONLY used columns from the AVAILABLE COLUMNS list above
- [ ] Columns with spaces wrapped in `[square brackets]`
- [ ] Correct date column selected based on context
- [ ] CODE columns used in WHERE and GROUP BY (not NAME columns)
- [ ] Both CODE and NAME columns selected for display
- [ ] **CRITICAL:** ONLY CODE columns in GROUP BY clause (NO NAME columns)
- [ ] `IS NOT NULL` applied to all GROUP BY columns
- [ ] `TRIM()` only used on string type columns
- [ ] Appropriate sorting applied
- [ ] **CRITICAL:** NO `LIMIT` clause used - use `TOP` or `OFFSET FETCH` instead
- [ ] **CRITICAL:** SQL Server syntax used throughout (no MySQL/PostgreSQL syntax)
- [ ] String concatenation uses `+` not `||`
- [ ] Date functions use SQL Server equivalents

**Now generate the SQL query for the user's request.**
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