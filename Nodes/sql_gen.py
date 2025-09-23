from typing import Dict, Any, List
import os
import re
from langchain_openai import AzureChatOpenAI
from Search.streaming_bm25_search import StreamingBM25Search
from Tools.entity_mapping_tool import EntityMappingTool

class SQLGenerationNode:
    """Node for generating SQL queries with placeholder values and BM25-based value replacement"""
    
    def __init__(self):
        # Initialize lazily - don't load heavy resources in __init__
        self.llm = None
        self.streaming_bm25 = None
        self.entity_tool = None
        self._initialized = False
    
    def _initialize_if_needed(self):
        """Initialize heavy resources only when needed"""
        if not self._initialized:
            print("[SQL GENERATION] Initializing heavy resources...")
            # Initialize Azure OpenAI
            self.llm = AzureChatOpenAI(
                azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini"),
                azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
                api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-07-18"),
                temperature=0.1
            )
            
            # Initialize streaming BM25 search (keeps index on disk)
            self.streaming_bm25 = StreamingBM25Search()
            
            # Initialize entity mapping tool
            self.entity_tool = EntityMappingTool()
            
            self._initialized = True
            print("[SQL GENERATION] Streaming BM25 search and entity mapping tool initialized!")

    def __call__(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate SQL queries with placeholders and replace them using database search
        
        Args:
            state: Current state containing user query, metadata, and LLM checker results
            
        Returns:
            Updated state with generated SQL
        """
        # Initialize heavy resources only when needed
        self._initialize_if_needed()
        
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
            # Step 1: Extract entities from user query using database search
            extracted_entities = self._extract_entities_from_query(user_query, metadata_results)
            
            # Step 2: Generate SQL with placeholders (using extracted entities as hints)
            sql_with_placeholders = self._generate_sql_with_placeholders(user_query, metadata_results, extracted_entities)
            
            # Step 3: Replace placeholders using database search
            final_sql = self._replace_placeholders_with_database_search(sql_with_placeholders, user_query, metadata_results)
            
            # Update state
            state["sql_generation_status"] = "completed"
            state["generated_sql"] = final_sql
            state["sql_validated"] = True
            state["extracted_entities"] = extracted_entities
            state["sql_generation_result"] = {
                "success": True,
                "final_sql": final_sql,
                "entities_extracted": len(extracted_entities) if extracted_entities else 0,
                "extracted_entities": extracted_entities
            }
            
            print("✅ [SQL GENERATION] SQL generation completed successfully")
            state["sql_validated"] = True  # Mark SQL as validated for Azure retrieval
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
    
    def _extract_entities_from_query(self, user_query: str, metadata_results: List[Dict]) -> List[Dict]:
        """Extract entities from user query using entity mapping tool"""
        print(f"[SQL GENERATION] Extracting entities from query: '{user_query}'")
        
        # Get available column names for filtering
        available_columns = [col.get('column_name', '') for col in metadata_results]
        
        # Use entity mapping tool to get values for relevant columns
        entities = []
        columns_to_check = [
            "Preventable Flag", "Claim Status", "Claim State", "Accident Type"
        ]
        
        for column_name in columns_to_check:
            # Check if this column is in the metadata
            if column_name in available_columns:
                try:
                    result = self.entity_tool.get_column_values(column_name)
                    if result.get("success", False):
                        values = result.get("values", [])
                        entities.append({
                            'column': column_name,
                            'values': values,
                            'confidence': 'high',
                            'source': 'entity_mapping_tool'
                        })
                        print(f"  - {column_name}: {values}")
                except Exception as e:
                    print(f"⚠️ [SQL GENERATION] Error getting values for {column_name}: {str(e)}")
        
        print(f"[SQL GENERATION] Extracted {len(entities)} entity mappings")
        return entities
    
    def _fallback_entity_search(self, user_query: str, available_columns: List[str]) -> List:
        """Fallback entity search when BM25 index is not loaded"""
        # Common values for claims data
        common_values = {
            'state': ['CA', 'TX', 'FL', 'NY', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI', 'California', 'Texas', 'Florida'],
            'claim_status': ['OPEN', 'CLOSED', 'PENDING', 'BILLED', 'PAID', 'open', 'closed', 'pending'],
            'transaction_status': ['OPEN', 'CLOSED', 'PENDING', 'BILLED', 'PAID', 'open', 'closed', 'pending'],
            'accident_type': ['COLLISION', 'COMPREHENSIVE', 'LIABILITY', 'PERSONAL_INJURY'],
            'lastaction': ['CREATE', 'NOTE', 'UPDATE', 'CLOSE', 'BILL', 'create', 'note', 'update']
        }
        
        results = []
        search_lower = user_query.lower()
        
        for column, values in common_values.items():
            if column.lower() in available_columns:
                for value in values:
                    if self._matches_search(value, search_lower):
                        # Create a simple result object
                        class SimpleResult:
                            def __init__(self, column_name, value, score, exact_match):
                                self.column_name = column_name
                                self.value = value
                                self.score = score
                                self.exact_match = exact_match
                        
                        score = self._calculate_fallback_score(value, search_lower)
                        result = SimpleResult(
                            column_name=column,
                            value=value,
                            score=score,
                            exact_match=value.lower() == search_lower
                        )
                        results.append(result)
        
        # Sort by score and return top results
        results.sort(key=lambda x: x.score, reverse=True)
        return results[:20]
    
    def _matches_search(self, value: str, search_term: str) -> bool:
        """Check if value matches search term"""
        value_lower = value.lower()
        return (value_lower == search_term or 
                value_lower.startswith(search_term) or 
                search_term in value_lower)
    
    def _calculate_fallback_score(self, value: str, search_term: str) -> float:
        """Calculate similarity score for fallback search"""
        value_lower = value.lower()
        
        if value_lower == search_term:
            return 1.0
        elif value_lower.startswith(search_term):
            return 0.8
        elif search_term in value_lower:
            return 0.6
        else:
            # Simple fuzzy matching
            import difflib
            similarity = difflib.SequenceMatcher(None, value_lower, search_term).ratio()
            return similarity * 0.5
    
    def _get_database_connection_info(self):
        """Get search method information for debugging"""
        if self.streaming_bm25 and self.streaming_bm25.connection:
            stats = self.streaming_bm25.get_index_stats()
            return f"Using streaming BM25 (index on disk, {stats.get('total_documents', 0)} documents)"
        else:
            return "Using streaming BM25 (no index built yet)"
    
    def _generate_sql_with_placeholders(self, user_query: str, metadata_results: List[Dict], extracted_entities: List[Dict] = None) -> str:
        """Generate SQL query with placeholders for varchar values that need BM25 search"""
        
        # Get available column names for validation
        available_columns = [col.get('column_name', '').lower() for col in metadata_results]
        
        # Format metadata for the prompt
        metadata_text = self._format_metadata_for_sql_generation(metadata_results)
        
        # Format extracted entities for the prompt
        entities_text = ""
        if extracted_entities:
            entities_text = "\n\nEXTRACTED ENTITIES (use these values directly when possible):\n"
            for entity in extracted_entities:
                entities_text += f"- {entity['column']} = '{entity['value']}' (confidence: {entity['confidence']})\n"
        
        prompt = f"""
        Generate a SQL query for claims_summary table based on the user request.

        USER REQUEST: "{user_query}"
        
        AVAILABLE COLUMNS (ONLY use these):
        {metadata_text}{entities_text}
        
        CRITICAL RULES:
        - ONLY use columns from the available list above
        - PREFER using extracted entities when they match your query intent
        - Use AWS SQL syntax: wrap column names with spaces in square brackets [Column Name]
        - Use table name: PRD.CLAIMS_SUMMARY (NO alias, NO 'cs')
        - For TEXT/STRING columns (varchar, nvarchar, char, text) with filtering: use {{PLACEHOLDER:column_name:search_term}}
        - For NUMERIC columns (int, decimal, float) with filtering: write actual values directly
        - For DATE/TIME columns with filtering: write actual values directly (e.g., '2024-01-01')
        - Use AWS SQL syntax throughout
        - DO NOT use any column names not in the available list
        - IMPORTANT: Use realistic values that exist in the data (e.g., 'CA' not 'California', 'C' not 'Closed')
        - NEVER use table aliases like 'cs' or 'PRD' in column references
        - NEVER use 'Claim_Count' - use COUNT(DISTINCT [Claim Number]) AS claim_count
        
        AWS SQL SYNTAX EXAMPLES:
        - Column with spaces: [Accident or Incident Code], [Claim Number], [Claim State]
        - Table reference: PRD.CLAIMS_SUMMARY (NO alias)
        - Full query: SELECT [Accident or Incident Code] AS Type, COUNT(DISTINCT [Claim Number]) AS claim_count FROM PRD.CLAIMS_SUMMARY GROUP BY [Accident or Incident Code]
        
        ENTITY USAGE EXAMPLES:
        - If extracted entity shows "Claim State = 'TX'" and user asks for "Texas claims" → WHERE [Claim State] = 'TX'
        - If extracted entity shows "Accident Type = 'Cargo'" and user asks for "cargo accidents" → WHERE [Accident Type] = 'Cargo'
        
        PLACEHOLDER EXAMPLES (for text columns without extracted entities):
        - "California claims" → WHERE [Claim State] = {{PLACEHOLDER:Claim State:CA}}
        - "closed claims" → WHERE [Status Flag] = {{PLACEHOLDER:Status Flag:C}}
        - "work comp claims" → WHERE [Coverage Major] = {{PLACEHOLDER:Coverage Major:Work Comp}}
        
        EXAMPLE QUERY FOR COVERAGE DISTRIBUTION:
        SELECT [Coverage Major], COUNT(DISTINCT [Claim Number]) AS claim_count 
        FROM PRD.CLAIMS_SUMMARY 
        GROUP BY [Coverage Major]
        
        DIRECT VALUE EXAMPLES (for numeric/date columns):
        - "high value claims" → WHERE [Claim Cost] > 10000
        - "this year" → WHERE [Claim Date] >= '2024-01-01'
        - "disputed claims" → WHERE [Will Dispute] = 1
        
        SQL:
        ```sql
        [Your query here - ONLY use columns from the available list]
        ```
        """
        
        response = self.llm.invoke(prompt)
        response_text = response.content.strip()
        
        # Extract SQL from the response
        sql_query = self._extract_sql_from_response(response_text)
        
        # Validate that only available columns are used
        validated_sql = self._validate_sql_columns(sql_query, available_columns)
        
        return validated_sql
    
    def _validate_sql_columns(self, sql_query: str, available_columns: List[str]) -> str:
        """Validate that SQL only uses columns from the available metadata"""
        import re
        
        # Find all column references in the SQL (case insensitive)
        # Look for column names in square brackets [Column Name] or regular identifiers
        column_pattern = r'\[([^\]]+)\]|\b([a-zA-Z_][a-zA-Z0-9_]*)\b'
        matches = re.findall(column_pattern, sql_query, re.IGNORECASE)
        
        # Extract column names from both square brackets and regular identifiers
        potential_columns = []
        for match in matches:
            if match[0]:  # Column in square brackets
                potential_columns.append(match[0])
            elif match[1]:  # Regular column name
                potential_columns.append(match[1])
        
        # SQL keywords to ignore
        sql_keywords = {
            'select', 'from', 'where', 'group', 'by', 'order', 'having', 'and', 'or', 'not',
            'in', 'like', 'between', 'is', 'null', 'count', 'sum', 'avg', 'min', 'max',
            'distinct', 'as', 'asc', 'desc', 'limit', 'offset', 'case', 'when', 'then',
            'else', 'end', 'if', 'exists', 'union', 'join', 'inner', 'left', 'right',
            'outer', 'on', 'using', 'with', 'as', 'cast', 'convert', 'date', 'time',
            'year', 'month', 'day', 'hour', 'minute', 'second', 'now', 'today',
            'true', 'false', 'null', 'isnull', 'coalesce', 'nvl', 'ifnull'
        }
        
        # Check for invalid columns
        invalid_columns = []
        for col in potential_columns:
            col_lower = col.lower()
            if (col_lower not in sql_keywords and 
                col_lower not in available_columns and 
                not col_lower.isdigit() and
                not col_lower.startswith("'") and
                not col_lower.startswith('"') and
                col_lower not in ['claims_summary']):  # Allow table name
                invalid_columns.append(col)
        
        if invalid_columns:
            print(f"⚠️ [SQL GENERATION] Invalid columns found: {invalid_columns}")
            print(f"[SQL GENERATION] Available columns: {available_columns}")
            
            # Try to fix by removing invalid columns or replacing with valid ones
            fixed_sql = sql_query
            for invalid_col in invalid_columns:
                # Simple fix: comment out lines with invalid columns
                lines = fixed_sql.split('\n')
                fixed_lines = []
                for line in lines:
                    if invalid_col.lower() in line.lower() and not line.strip().startswith('--'):
                        fixed_lines.append(f"-- {line}  -- INVALID COLUMN: {invalid_col}")
                        print(f"[SQL GENERATION] Commented out line with invalid column '{invalid_col}': {line.strip()}")
                    else:
                        fixed_lines.append(line)
                fixed_sql = '\n'.join(fixed_lines)
            
            return fixed_sql
        else:
            print("✅ [SQL GENERATION] All columns are valid")
            return sql_query
    
    def _replace_placeholders_with_database_search(self, sql_with_placeholders: str, user_query: str, metadata_results: List[Dict]) -> str:
        """Replace placeholders in SQL using database search within specific column"""
        final_sql = sql_with_placeholders
        
        # Find all placeholders in the format {PLACEHOLDER:column_name:search_term} or {{PLACEHOLDER:column_name:search_term}}
        placeholder_pattern = r'\{\{?PLACEHOLDER:([^:]+):([^}]+)\}\}?'
        placeholders = re.findall(placeholder_pattern, sql_with_placeholders)
        
        print(f"[SQL GENERATION] Found {len(placeholders)} placeholders to replace")
        print(f"[SQL GENERATION] Search method: {self._get_database_connection_info()}")
        
        for column_name, search_term in placeholders:
            print(f"[SQL GENERATION] Searching for '{search_term}' in column '{column_name}'")
            
            # Search only within the specific column's unique values using streaming BM25
            search_results = self.streaming_bm25.search(search_term, column_filter=column_name, top_k=5)
            print(f"[SQL GENERATION] Streaming BM25 search returned {len(search_results)} results")
            
            if search_results:
                # Always use the best match (highest score) - no threshold
                best_match = search_results[0]
                replacement_value = best_match.value
                match_type = "exact" if best_match.exact_match else "closest"
                print(f"[SQL GENERATION] Found {match_type} match: '{search_term}' → '{replacement_value}' (score: {best_match.score:.3f})")
                
                # Replace the placeholder with the best match value (handle both single and double braces)
                placeholder_text_single = f"{{PLACEHOLDER:{column_name}:{search_term}}}"
                placeholder_text_double = f"{{{{PLACEHOLDER:{column_name}:{search_term}}}}}"
                if placeholder_text_single in final_sql:
                    final_sql = final_sql.replace(placeholder_text_single, f"'{replacement_value}'")
                elif placeholder_text_double in final_sql:
                    final_sql = final_sql.replace(placeholder_text_double, f"'{replacement_value}'")
                print(f"[SQL GENERATION] Replaced placeholder → '{replacement_value}'")
            else:
                print(f"[SQL GENERATION] No streaming BM25 results found for '{search_term}' in column '{column_name}'")
                # Try broader search without column filter
                broader_results = self.streaming_bm25.search(search_term, top_k=3)
                if broader_results:
                    best_match = broader_results[0]
                    replacement_value = best_match.value
                    print(f"[SQL GENERATION] Found broader match: '{search_term}' → '{replacement_value}' (score: {best_match.score:.3f})")
                    
                    # Replace the placeholder (handle both single and double braces)
                    placeholder_text_single = f"{{PLACEHOLDER:{column_name}:{search_term}}}"
                    placeholder_text_double = f"{{{{PLACEHOLDER:{column_name}:{search_term}}}}}"
                    if placeholder_text_single in final_sql:
                        final_sql = final_sql.replace(placeholder_text_single, f"'{replacement_value}'")
                    elif placeholder_text_double in final_sql:
                        final_sql = final_sql.replace(placeholder_text_double, f"'{replacement_value}'")
                    print(f"[SQL GENERATION] Replaced placeholder → '{replacement_value}'")
                else:
                    # No matches found - comment out the line instead of using invalid value
                    print(f"[SQL GENERATION] No matches found for '{search_term}', commenting out line")
                    placeholder_text_single = f"{{PLACEHOLDER:{column_name}:{search_term}}}"
                    placeholder_text_double = f"{{{{PLACEHOLDER:{column_name}:{search_term}}}}}"
                    
                    # Find and comment out the line containing this placeholder
                    lines = final_sql.split('\n')
                    fixed_lines = []
                    for line in lines:
                        if (placeholder_text_single in line or placeholder_text_double in line) and not line.strip().startswith('--'):
                            fixed_lines.append(f"-- {line}  -- NO MATCH FOUND: {search_term}")
                            print(f"[SQL GENERATION] Commented out line with no match: {line.strip()}")
                        else:
                            fixed_lines.append(line)
                    final_sql = '\n'.join(fixed_lines)
        
        # Final validation: ensure no placeholders remain
        remaining_placeholders = re.findall(placeholder_pattern, final_sql)
        if remaining_placeholders:
            print(f"⚠️ [SQL GENERATION] Warning: {len(remaining_placeholders)} placeholders still remain in SQL!")
            for col, term in remaining_placeholders:
                print(f"  - {col}: {term}")
        else:
            print("✅ [SQL GENERATION] All placeholders successfully replaced")
        
        # Validate that all filter values exist in unique values database
        validated_sql = self._validate_filter_values(final_sql, metadata_results)
        
        return validated_sql
    
    def _validate_filter_values(self, sql_query: str, metadata_results: List[Dict]) -> str:
        """Validate that all filter values in SQL exist in the unique values database"""
        import re
        
        # Find all string literals in the SQL (quoted values)
        # This handles both regular quotes and values within square brackets
        string_pattern = r"'([^']+)'"
        string_values = re.findall(string_pattern, sql_query)
        
        if not string_values:
            print("✅ [SQL GENERATION] No filter values to validate")
            return sql_query
        
        print(f"[SQL GENERATION] Validating {len(string_values)} filter values against unique values database")
        
        # Get all unique values from fallback search
        all_unique_values = set()
        # Use fallback values since no database connection
        fallback_values = {
            'ca', 'tx', 'fl', 'ny', 'il', 'pa', 'oh', 'ga', 'nc', 'mi',
            'california', 'texas', 'florida', 'new york', 'illinois',
            'open', 'closed', 'pending', 'billed', 'paid',
            'create', 'note', 'update', 'close', 'bill',
            'collision', 'comprehensive', 'liability', 'personal_injury'
        }
        all_unique_values = fallback_values
        
        print(f"[SQL GENERATION] Unique values database contains {len(all_unique_values)} values")
        
        # Check each string value
        invalid_values = []
        for value in string_values:
            if value.lower() not in all_unique_values:
                invalid_values.append(value)
        
        if invalid_values:
            print(f"⚠️ [SQL GENERATION] Invalid filter values found: {invalid_values}")
            print(f"[SQL GENERATION] These values don't exist in the unique values database")
            
            # Try to find closest matches for invalid values
            fixed_sql = sql_query
            for invalid_value in invalid_values:
                print(f"[SQL GENERATION] Looking for closest match to '{invalid_value}'")
                
                # Search for closest match using streaming BM25
                search_results = self.streaming_bm25.search(invalid_value, top_k=3)
                if search_results:
                    best_match = search_results[0]
                    replacement_value = best_match.value
                    print(f"[SQL GENERATION] Found closest match: '{invalid_value}' → '{replacement_value}' (score: {best_match.score:.3f})")
                    
                    # Replace the invalid value with the closest match
                    fixed_sql = fixed_sql.replace(f"'{invalid_value}'", f"'{replacement_value}'")
                else:
                    print(f"[SQL GENERATION] No match found for '{invalid_value}', removing from query")
                    # Remove lines containing invalid values
                    lines = fixed_sql.split('\n')
                    fixed_lines = []
                    for line in lines:
                        if f"'{invalid_value}'" not in line:
                            fixed_lines.append(line)
                        else:
                            fixed_lines.append(f"-- {line}  -- INVALID VALUE: {invalid_value}")
                            print(f"[SQL GENERATION] Commented out line with invalid value '{invalid_value}': {line.strip()}")
                    fixed_sql = '\n'.join(fixed_lines)
            
            return fixed_sql
        else:
            print("✅ [SQL GENERATION] All filter values are valid")
            return sql_query
    
    def _format_metadata_for_sql_generation(self, metadata_results: List[Dict]) -> str:
        """Format metadata columns for SQL generation prompt"""
        if not metadata_results:
            return "No metadata available"
        
        formatted_columns = []
        for col in metadata_results:
            col_name = col.get('column_name', '')
            col_desc = col.get('description', '')
            col_type = col.get('data_type', '')
            
            # Determine if this column needs placeholders (text columns)
            needs_placeholder = self._needs_placeholder(col_type)
            placeholder_note = " (use PLACEHOLDER for filtering)" if needs_placeholder else " (write values directly)"
            
            # Format: column_name (type): description [placeholder note]
            formatted_columns.append(f"- {col_name} ({col_type}): {col_desc}{placeholder_note}")
        
        return "\n".join(formatted_columns)
    
    def _needs_placeholder(self, data_type: str) -> bool:
        """Determine if a data type needs placeholders for filtering"""
        if not data_type:
            return False
        
        data_type_lower = data_type.lower()
        
        # Text/string types that need placeholders
        text_types = ['varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext', 'string']
        
        # Check if it's a text type
        for text_type in text_types:
            if text_type in data_type_lower:
                return True
        
        return False
    
    def _extract_sql_from_response(self, response_text: str) -> str:
        """Extract SQL query from LLM response"""
        # Look for SQL blocks
        sql_pattern = r'```sql\s*(.*?)\s*```'
        sql_matches = re.findall(sql_pattern, response_text, re.DOTALL | re.IGNORECASE)
        
        if sql_matches:
            return sql_matches[0].strip()
        
        # Look for SELECT statements
        select_pattern = r'(SELECT\s+.*?(?:;|$))'
        select_matches = re.findall(select_pattern, response_text, re.DOTALL | re.IGNORECASE)
        
        if select_matches:
            return select_matches[0].strip().rstrip(';')
        
        # Fallback: return the entire response
        return response_text.strip()
    
