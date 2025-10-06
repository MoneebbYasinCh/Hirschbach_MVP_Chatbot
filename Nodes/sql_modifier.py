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
            modified_sql = self._modify_sql_for_period(base_sql, target_period, new_question)
            
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
    
    def _modify_sql_for_period(self, base_sql: str, target_period: str, new_question: str) -> str:
        """Modify SQL query to target a different time period"""
        
        # Define period modification mappings
        period_mappings = {
            "last_week": {
                "description": "previous week",
                "date_logic": "DATEADD(WEEK, -1, GETDATE())",
                "range_start": "DATEADD(WEEK, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0))",
                "range_end": "DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)"
            },
            "last_month": {
                "description": "previous month", 
                "date_logic": "DATEADD(MONTH, -1, GETDATE())",
                "range_start": "DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 1, 0)",
                "range_end": "DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0)"
            },
            "this_month": {
                "description": "current month",
                "date_logic": "GETDATE()",
                "range_start": "DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0)",
                "range_end": "DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) + 1, 0)"
            },
            "yesterday": {
                "description": "previous day",
                "date_logic": "DATEADD(DAY, -1, GETDATE())",
                "range_start": "CAST(DATEADD(DAY, -1, GETDATE()) AS DATE)",
                "range_end": "CAST(GETDATE() AS DATE)"
            },
            "today": {
                "description": "current day",
                "date_logic": "GETDATE()",
                "range_start": "CAST(GETDATE() AS DATE)",
                "range_end": "CAST(DATEADD(DAY, 1, GETDATE()) AS DATE)"
            }
        }
        
        if target_period not in period_mappings:
            return ""
        
        period_info = period_mappings[target_period]
        
        prompt = f"""
        You are an expert SQL modifier. Your task is to modify an existing SQL query to target a different time period.
        
        ORIGINAL SQL QUERY:
        {base_sql}
        
        TARGET PERIOD: {period_info['description']}
        NEW USER QUESTION: "{new_question}"
        
        MODIFICATION RULES:
        1. Keep the same SELECT, FROM, GROUP BY, and ORDER BY structure
        2. Only modify the date/time filtering conditions in the WHERE clause
        3. Use SQL Server syntax and functions
        4. For {target_period}, use these date ranges:
           - Range start: {period_info['range_start']}
           - Range end: {period_info['range_end']}
        
        COMMON PATTERNS TO MODIFY:
        
        For "this week" → "last week":
        - Replace: DATEPART(WEEK, [date_column]) = DATEPART(WEEK, GETDATE())
        - With: [date_column] >= {period_info['range_start']} AND [date_column] < {period_info['range_end']}
        
        For "this month" → "last month":
        - Replace: MONTH([date_column]) = MONTH(GETDATE()) AND YEAR([date_column]) = YEAR(GETDATE())
        - With: [date_column] >= {period_info['range_start']} AND [date_column] < {period_info['range_end']}
        
        For any DATEADD/DATEPART patterns:
        - Identify the date column being filtered
        - Replace the time logic with the appropriate range for {target_period}
        
        CRITICAL REQUIREMENTS:
        - Preserve all column names, table names, and aliases exactly
        - Keep the same aggregation logic (COUNT, SUM, etc.)
        - Only modify date filtering conditions
        - Use proper SQL Server date functions
        - Ensure the modified query is syntactically correct
        
        Return ONLY the modified SQL query, no explanations or markdown.
        """
        
        try:
            response = self.llm.invoke(prompt)
            modified_sql = response.content.strip()
            
            # Clean up any markdown formatting
            if modified_sql.startswith("```sql"):
                modified_sql = modified_sql[6:]
            elif modified_sql.startswith("```"):
                modified_sql = modified_sql[3:]
            if modified_sql.endswith("```"):
                modified_sql = modified_sql[:-3]
            
            return modified_sql.strip()
            
        except Exception as e:
            print(f"[SQL_MODIFIER] Error modifying SQL: {str(e)}")
            return ""
    
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
