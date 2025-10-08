from typing import Dict, Any, List
import os
from langchain_openai import AzureChatOpenAI
import re

class SQLModifierNode:
    """Node for intelligently modifying previous SQL queries based on user follow-up requests"""
    
    def __init__(self):
        self.llm = AzureChatOpenAI(
            azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-07-18"),
            temperature=0.0
        )
    
    def __call__(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Modify previous SQL query based on user's follow-up request
        
        Args:
            state: Current state containing SQL modification request
            
        Returns:
            Updated state with modified SQL
        """
        print("[SQL_MODIFIER] Processing SQL modification request...")
        
        # Debug: Check what's in the state
        print(f"[SQL_MODIFIER DEBUG] State keys: {list(state.keys())}")
        print(f"[SQL_MODIFIER DEBUG] Orchestration: {state.get('orchestration', {})}")
        
        # Get modification request from orchestrator
        modification_request = state.get("sql_modification_request", {})
        print(f"[SQL_MODIFIER DEBUG] Modification request: {modification_request}")
        
        # Also check orchestration as backup
        orchestration = state.get("orchestration", {})
        if not modification_request or not modification_request.get("should_modify"):
            # Try to reconstruct from orchestration and SQL history
            if orchestration.get("decision") == "sql_modification":
                print("[SQL_MODIFIER] Attempting to reconstruct modification request from orchestration")
                sql_history = state.get("sql_query_history", [])
                user_input = orchestration.get("user_input", "")
                
                if sql_history and user_input:
                    # Reconstruct the modification request
                    modification_request = self._reconstruct_modification_request(user_input, sql_history)
                    if modification_request.get("should_modify"):
                        print("[SQL_MODIFIER] Successfully reconstructed modification request")
                    else:
                        print("[SQL_MODIFIER] Failed to reconstruct modification request")
                        state["sql_modification_completed"] = False
                        state["sql_modification_error"] = "Could not reconstruct modification request"
                        return state
                else:
                    print("[SQL_MODIFIER] No SQL history or user input available for reconstruction")
                    state["sql_modification_completed"] = False
                    state["sql_modification_error"] = "No valid modification request found in state"
                    return state
            else:
                print("[SQL_MODIFIER] No valid modification request found")
                state["sql_modification_completed"] = False
                state["sql_modification_error"] = "No valid modification request found in state"
                return state
        
        base_sql = modification_request.get("base_sql", "")
        target_period = modification_request.get("target_period", "")
        new_question = modification_request.get("new_question", "")
        base_question = modification_request.get("base_question", "")
        
        print(f"[SQL_MODIFIER] Modifying SQL for period: {target_period}")
        print(f"[SQL_MODIFIER] Base question: {base_question}")
        print(f"[SQL_MODIFIER] New question: {new_question}")
        
        try:
            # Modify the SQL query
            modified_sql = self._modify_sql_for_period(base_sql, target_period, new_question, state)
            
            if modified_sql:
                # Store the modified SQL in state
                state["generated_sql"] = modified_sql
                state["sql_validated"] = True
                state["sql_modification_completed"] = True
                
                # CRITICAL: Also update the top_kpi with modified SQL so Azure Retrieval uses it
                if "top_kpi" in state and isinstance(state["top_kpi"], dict):
                    state["top_kpi"]["sql_query"] = modified_sql
                    print(f"[SQL_MODIFIER DEBUG] Updated top_kpi with modified SQL")
                
                # Store in SQL history
                self._store_sql_in_history(state, new_question, modified_sql, "sql_modification")
                
                print(f"[SQL_MODIFIER] Successfully modified SQL")
                print(f"[SQL_MODIFIER] Modified SQL: {modified_sql[:100]}...")
                print(f"[SQL_MODIFIER DEBUG] Set sql_modification_completed = True")
                print(f"[SQL_MODIFIER DEBUG] Set generated_sql = {modified_sql[:50]}...")
                print(f"[SQL_MODIFIER DEBUG] Set sql_validated = True")
            else:
                print("[SQL_MODIFIER] Failed to modify SQL")
                state["sql_modification_completed"] = False
        
        except Exception as e:
            print(f"[SQL_MODIFIER] Error: {str(e)}")
            state["sql_modification_completed"] = False
        
        return state
    
    def _modify_sql_for_period(self, base_sql: str, target_period: str, new_question: str, state: Dict[str, Any] = None) -> str:
        """Intelligently modify SQL query to target a different time period using LLM"""
        
        print(f"[SQL_MODIFIER] Using intelligent LLM-based SQL modification for: '{target_period}'")
        
        # Check if we should use conversation history in prompts
        use_history = False
        conversation_context = ""
        
        if state:
            orchestration = state.get("orchestration", {})
            use_history = orchestration.get("use_history_in_prompts", False)
            
            if use_history:
                print(f"[SQL_MODIFIER] Including conversation history in prompt for context")
                # Get conversation history from orchestrator if available
                try:
                    from Graph_Flow.main_graph import get_global_orchestrator
                    orchestrator = get_global_orchestrator()
                    conversation_context = orchestrator._get_conversation_context(new_question)
                except Exception as e:
                    print(f"[SQL_MODIFIER] Could not get conversation context: {e}")
                    conversation_context = ""
            else:
                print(f"[SQL_MODIFIER] Not using conversation history - treating as new unrelated query")
        
        # Build context section for prompt
        context_section = ""
        if use_history and conversation_context:
            context_section = f"""
        CONVERSATION CONTEXT:
        {conversation_context}
        
        This modification request is a follow-up to previous queries. Use this context to understand the user's intent and maintain consistency with previous analysis.
        """
        
        prompt = f"""
        You are an expert SQL modifier with deep understanding of temporal expressions. Your task is to intelligently modify an existing SQL query to target a different time period based on natural language.
        
        ORIGINAL SQL QUERY:
        {base_sql}
        
        NEW USER REQUEST: "{new_question}"
        TARGET TEMPORAL REFERENCE: "{target_period}"
        {context_section}
        
        INTELLIGENT MODIFICATION INSTRUCTIONS:
        
        1. ANALYZE the original SQL query to understand:
           - What date/time columns are being filtered
           - What time period the original query targets
           - The current date filtering logic
        
        2. INTERPRET the target temporal reference intelligently:
           - "this_week" = current week (Monday to Sunday)
           - "last_week" = previous week  
           - "this_month" = current calendar month
           - "last_month" = previous calendar month
           - "this_quarter" = current quarter (Q1, Q2, Q3, Q4)
           - "last_quarter" = previous quarter
           - "this_year" = current calendar year
           - "last_year" = previous calendar year
           - "today" = current date only
           - "yesterday" = previous date only
           - Handle variations like "current week", "past month", "earlier this quarter", etc.
        
        3. GENERATE appropriate SQL Server date functions:
           - Use DATEADD, DATEDIFF, DATEPART functions
           - Create proper date ranges with >= and < comparisons
           - Handle edge cases (month boundaries, year boundaries, etc.)
        
        4. PRESERVE the complete original query structure:
           - Keep all SELECT columns exactly the same
           - Maintain FROM clause and JOINs
           - Preserve WHERE conditions (except date filters)
           - Keep GROUP BY clause identical
           - Include complete ORDER BY clause
        
        EXAMPLES of intelligent temporal mapping:
        
        Original: WHERE [Date] >= DATEADD(QUARTER, -1, GETDATE())
        Target: "this quarter" 
        Result: WHERE [Date] >= DATEADD(QUARTER, DATEDIFF(QUARTER, 0, GETDATE()), 0) AND [Date] < DATEADD(QUARTER, DATEDIFF(QUARTER, 0, GETDATE()) + 1, 0)
        
        Original: WHERE MONTH([Date]) = MONTH(GETDATE())
        Target: "last month"
        Result: WHERE [Date] >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 1, 0) AND [Date] < DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0)
        
        CRITICAL REQUIREMENTS:
        - Return a COMPLETE, executable SQL query
        - MUST include the full ORDER BY clause from the original query
        - Only modify date/time filtering conditions  
        - Use proper SQL Server syntax
        - Handle any temporal expression intelligently
        - Ensure syntactic correctness
        - DO NOT truncate or abbreviate any part of the query
        - The query MUST end with a complete ORDER BY clause (e.g., "ORDER BY [Column] DESC")
        
        VALIDATION CHECK: Your response must contain a complete ORDER BY clause that ends with DESC or ASC.
        
        Return ONLY the complete modified SQL query, no explanations or markdown. Ensure the ORDER BY clause is complete.
        """
        
        try:
            print(f"[SQL_MODIFIER] Sending prompt to LLM for SQL modification...")
            response = self.llm.invoke(prompt)
            modified_sql = response.content.strip()
            
            print(f"[SQL_MODIFIER] LLM Response length: {len(modified_sql)} characters")
            print(f"[SQL_MODIFIER] LLM Response preview: {modified_sql[:200]}...")
            
            # Check if response looks truncated
            if not modified_sql.strip().upper().endswith(('DESC', 'ASC', ';')):
                print(f"[SQL_MODIFIER] WARNING: Response might be truncated - doesn't end with ORDER BY clause")
            
            # Clean up any markdown formatting
            original_length = len(modified_sql)
            if modified_sql.startswith("```sql"):
                modified_sql = modified_sql[6:]
                print(f"[SQL_MODIFIER] Removed ```sql prefix")
            elif modified_sql.startswith("```"):
                modified_sql = modified_sql[3:]
                print(f"[SQL_MODIFIER] Removed ``` prefix")
            if modified_sql.endswith("```"):
                modified_sql = modified_sql[:-3]
                print(f"[SQL_MODIFIER] Removed ``` suffix")
            
            cleaned_sql = modified_sql.strip()
            print(f"[SQL_MODIFIER] Cleaned SQL length: {len(cleaned_sql)} characters (was {original_length})")
            
            # Enhanced ORDER BY detection and recovery
            cleaned_upper = cleaned_sql.upper()
            has_complete_order_by = 'ORDER BY' in cleaned_upper and (
                cleaned_upper.endswith(' DESC') or 
                cleaned_upper.endswith(' ASC') or
                cleaned_upper.endswith('DESC') or
                cleaned_upper.endswith('ASC')
            )
            
            # Check for incomplete ORDER BY (like "ORDER ..." or just "ORDER")
            has_incomplete_order = (
                cleaned_upper.endswith('ORDER') or 
                cleaned_upper.endswith('ORDER ...') or
                cleaned_upper.endswith('ORDER BY') or
                'ORDER ...' in cleaned_upper
            )
            
            if not has_complete_order_by or has_incomplete_order:
                print(f"[SQL_MODIFIER] ERROR: Modified SQL has incomplete/missing ORDER BY clause!")
                print(f"[SQL_MODIFIER] Current SQL ends with: '{cleaned_sql[-50:]}'")
                print(f"[SQL_MODIFIER] Attempting to recover ORDER BY from original query...")
                
                # Remove incomplete ORDER BY if present
                if has_incomplete_order:
                    # Remove trailing incomplete ORDER BY parts
                    for incomplete in ['ORDER ...', 'ORDER BY', 'ORDER']:
                        if cleaned_sql.upper().endswith(incomplete):
                            cleaned_sql = cleaned_sql[:-len(incomplete)].rstrip()
                            print(f"[SQL_MODIFIER] Removed incomplete '{incomplete}' from end")
                            break
                
                # Extract ORDER BY clause from original query
                base_sql_upper = base_sql.upper()
                if 'ORDER BY' in base_sql_upper:
                    order_by_start = base_sql_upper.find('ORDER BY')
                    original_order_by = base_sql[order_by_start:].strip()
                    
                    # Append the complete ORDER BY clause
                    cleaned_sql = cleaned_sql.rstrip() + ' ' + original_order_by
                    print(f"[SQL_MODIFIER] Recovered ORDER BY clause: {original_order_by}")
                    print(f"[SQL_MODIFIER] Final SQL length: {len(cleaned_sql)} characters")
                else:
                    print(f"[SQL_MODIFIER] WARNING: No ORDER BY clause found in original query either!")
            else:
                print(f"[SQL_MODIFIER] ORDER BY clause appears complete")
            
            if not cleaned_sql:
                print(f"[SQL_MODIFIER] WARNING: Cleaned SQL is empty!")
                return ""
            
            # Final validation - ensure the query is complete and executable
            final_validation = self._validate_sql_completeness(cleaned_sql, base_sql)
            if not final_validation["is_complete"]:
                print(f"[SQL_MODIFIER] FINAL VALIDATION FAILED: {final_validation['reason']}")
                print(f"[SQL_MODIFIER] Attempting final recovery...")
                cleaned_sql = final_validation.get("recovered_sql", cleaned_sql)
            
            print(f"[SQL_MODIFIER] Final SQL query length: {len(cleaned_sql)} characters")
            print(f"[SQL_MODIFIER] Final SQL preview: {cleaned_sql[:100]}...{cleaned_sql[-50:]}")
            
            return cleaned_sql
            
        except Exception as e:
            print(f"[SQL_MODIFIER] Error modifying SQL: {str(e)}")
            return ""
    
    def _validate_sql_completeness(self, sql: str, original_sql: str) -> Dict[str, Any]:
        """Validate that the SQL query is complete and executable"""
        
        sql_upper = sql.upper()
        
        # Check for required SQL components
        has_select = 'SELECT' in sql_upper
        has_from = 'FROM' in sql_upper
        has_order_by = 'ORDER BY' in sql_upper
        ends_properly = sql_upper.endswith((' DESC', ' ASC', 'DESC', 'ASC'))
        
        if not has_select:
            return {"is_complete": False, "reason": "Missing SELECT clause"}
        
        if not has_from:
            return {"is_complete": False, "reason": "Missing FROM clause"}
        
        if not has_order_by:
            # Try to recover ORDER BY from original
            original_upper = original_sql.upper()
            if 'ORDER BY' in original_upper:
                order_by_start = original_upper.find('ORDER BY')
                original_order_by = original_sql[order_by_start:].strip()
                recovered_sql = sql.rstrip() + ' ' + original_order_by
                return {
                    "is_complete": False, 
                    "reason": "Missing ORDER BY clause",
                    "recovered_sql": recovered_sql
                }
            else:
                return {"is_complete": False, "reason": "Missing ORDER BY clause and none in original"}
        
        if not ends_properly:
            return {"is_complete": False, "reason": "ORDER BY clause incomplete - doesn't end with DESC/ASC"}
        
        return {"is_complete": True, "reason": "SQL appears complete"}
    
    def _store_sql_in_history(self, state: Dict[str, Any], user_query: str, sql_query: str, source: str) -> None:
        """Store modified SQL query in conversation history"""
        if "sql_query_history" not in state:
            state["sql_query_history"] = []
        
        sql_entry = {
            "user_question": user_query,
            "generated_sql": sql_query,
            "source": source,
            "timestamp": self._get_current_timestamp()
        }
        
        state["sql_query_history"].append(sql_entry)
        
        # Keep only last 10 queries
        if len(state["sql_query_history"]) > 10:
            state["sql_query_history"] = state["sql_query_history"][-10:]
        
        print(f"[SQL_MODIFIER] Stored modified SQL in history: {user_query[:50]}...")
    
    def _reconstruct_modification_request(self, user_input: str, sql_history: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Reconstruct modification request from user input and SQL history"""
        if not sql_history:
            return {"should_modify": False}
        
        # Get the most recent SQL query
        last_sql_entry = sql_history[-1]
        last_question = last_sql_entry.get("user_question", "").lower()
        last_sql = last_sql_entry.get("generated_sql", "")
        current_input = user_input.lower()
        
        # Define temporal modification patterns (same as orchestrator)
        temporal_patterns = {
            "last_week": ["last week", "previous week", "week before", "past week"],
            "last_month": ["last month", "previous month", "month before", "past month"],
            "last_quarter": ["last quarter", "previous quarter", "quarter before"],
            "last_year": ["last year", "previous year", "year before"],
            "today": ["today", "this day"],
            "yesterday": ["yesterday", "day before"],
            "this_month": ["this month", "current month"],
            "this_quarter": ["this quarter", "current quarter"],
            "this_year": ["this year", "current year"]
        }
        
        # Check if current input is a temporal follow-up
        detected_period = None
        for period, patterns in temporal_patterns.items():
            if any(pattern in current_input for pattern in patterns):
                detected_period = period
                break
        
        # Check if it's a simple follow-up pattern
        follow_up_patterns = ["what about", "how about", "show me for", "and for", "also for"]
        is_follow_up = any(pattern in current_input for pattern in follow_up_patterns)
        
        if detected_period and (is_follow_up or len(current_input.split()) <= 5):
            # Check if the previous query is compatible
            time_indicators = ["this week", "current week", "this month", "current month", 
                             "this quarter", "this year", "today", "week", "month", "quarter", "year"]
            
            has_time_context = any(indicator in last_question for indicator in time_indicators)
            has_date_in_sql = any(date_func in last_sql.upper() for date_func in 
                                ["DATEPART", "GETDATE", "DATEADD", "YEAR", "MONTH", "DAY"])
            
            if has_time_context or has_date_in_sql:
                return {
                    "should_modify": True,
                    "modification_type": "temporal_change",
                    "target_period": detected_period,
                    "base_sql": last_sql,
                    "base_question": last_sql_entry.get("user_question", ""),
                    "new_question": user_input
                }
        
        return {"should_modify": False}
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
