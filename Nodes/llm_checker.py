from typing import Dict, Any, List
import os
from langchain_openai import AzureChatOpenAI

class LLMCheckerNode:
    """Node for intelligently deciding what to do with retrieved KPI results"""
    
    def __init__(self):
        # Initialize Azure OpenAI
        self.llm = AzureChatOpenAI(
            azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-07-18"),
            temperature=0.0  # Remove randomness for consistent results
        )
    
    def __call__(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Intelligently decide what to do with the retrieved KPI based on how well it matches the user's task.
        Returns a decision type that determines the next path in the workflow.
        """
        
        # Get user input - prioritize user_query from state over messages
        task = state.get("user_query", "")
        if not task:
            # Fallback: Find the first HumanMessage in messages
            messages = state.get("messages", [])
            if messages:
                # Look for the first human message (user's actual query)
                for msg in messages:
                    if hasattr(msg, 'content') and hasattr(msg, '__class__'):
                        if 'Human' in str(msg.__class__):
                            task = msg.content
                            break
                if not task:
                    # Last resort: use the last message
                    task = messages[-1].content if hasattr(messages[-1], 'content') else str(messages[-1])
            else:
                task = state.get("task", "")

        # Confidence-based scope check - permissive by default
        try:
            metadata_results = state.get("metadata_rag_results", []) or []
            # Build comprehensive list of available columns (NO LIMIT - show all)
            available_cols = []
            for col in metadata_results:  # Show ALL columns
                name = col.get("column_name", "").strip()
                desc = col.get("description", "").strip()
                if name:
                    available_cols.append(f"- {name}: {desc}")

            # Only run scope check if we have metadata and task
            if available_cols and task:
                scope_prompt = f"""
Analyze if this request can be answered with the available columns.

USER REQUEST: "{task}"

AVAILABLE COLUMNS (name: description):
{chr(10).join(available_cols)}

CRITICAL: Only mark as OUT_OF_SCOPE if you are HIGHLY CONFIDENT (60%+) that the request requires data that is clearly NOT represented in any of these columns.

If there's ANY possibility the request could be answered with these columns, mark it as IN_SCOPE.

Examples of CLEAR OUT_OF_SCOPE (reject these):
- "Show me real-time GPS locations" (requires live telematics data)
- "What are driver speeding events?" (requires telematics/speed data)
- "Show me vehicle maintenance schedules" (requires maintenance system data)
- "What's the weather during accidents?" (requires weather API data)

Examples of IN_SCOPE (accept these even if indirect):
- "Which customers have the most claims?" → Customer Code, Claim Number (IN_SCOPE)
- "Show me accident trends" → Occurrence Date, Accident Code (IN_SCOPE)
- "What are high-risk drivers?" → Driver info, Preventable Flag (IN_SCOPE)
- "Claims by location" → Claim City, Claim State (IN_SCOPE)

Respond in JSON:
{{
    "decision": "IN_SCOPE" or "OUT_OF_SCOPE",
    "confidence": "HIGH" or "MEDIUM" or "LOW",
    "reasoning": "Brief explanation"
}}

DEFAULT: If unsure, choose IN_SCOPE.
"""

                scope_resp = self.llm.invoke(scope_prompt)
                scope_content = str(getattr(scope_resp, "content", "")).strip()
                
                # Clean up the response - remove markdown code blocks if present
                if scope_content.startswith("```json"):
                    scope_content = scope_content[7:]
                elif scope_content.startswith("```"):
                    scope_content = scope_content[3:]
                if scope_content.endswith("```"):
                    scope_content = scope_content[:-3]
                scope_content = scope_content.strip()
                
                # Parse JSON response
                try:
                    import json
                    scope_result = json.loads(scope_content)
                    decision = scope_result.get("decision", "IN_SCOPE").upper()
                    confidence = scope_result.get("confidence", "LOW").upper()
                    reasoning = scope_result.get("reasoning", "")
                    
                    # Only block if BOTH out of scope AND high confidence
                    if decision == "OUT_OF_SCOPE" and confidence == "HIGH":
                        print(f"🚫 [LLM_CHECKER] OUT_OF_SCOPE (High Confidence): {reasoning}")
                        state["llm_check_result"] = {
                            "decision_type": "out_of_scope",
                            "reasoning": reasoning,
                            "confidence": confidence
                        }
                        state["final_response"] = (
                            f"I cannot answer this request because it requires data that is not available in my current dataset.\n\n"
                            f"**Reason:** {reasoning}\n\n"
                            f"I can help you analyze claims data including customer information, claim types, dates, "
                            f"locations, financial metrics, and risk factors. Would you like to ask a related question?"
                        )
                        state["workflow_status"] = "complete"
                        state["next_node"] = "end"
                        return state
                    else:
                        # IN_SCOPE or not confident enough - let it proceed
                        print(f"✅ [LLM_CHECKER] IN_SCOPE - Proceeding with request")
                        print(f"🔍 [LLM_CHECKER] Decision: IN_SCOPE")
                        
                except json.JSONDecodeError as e:
                    # If JSON parsing fails, default to IN_SCOPE
                    print(f"⚠️ [LLM_CHECKER] Could not parse scope response - defaulting to IN_SCOPE")
                    print(f"⚠️ [LLM_CHECKER] Raw response: {scope_content[:200]}...")
                    pass
                    
        except Exception as e:
            # Always fail open - let the request proceed
            print(f"⚠️ [LLM_CHECKER] Scope check error: {str(e)} - defaulting to IN_SCOPE")
            pass
        
        # Get KPI results from the state
        top_kpi = state.get("top_kpi")
        
        if not top_kpi:
            # No KPIs found, proceed to SQL generation
            fallback_result = {
                "llm_check_result": {
                    "decision_type": "not_relevant",
                    "reasoning": "No KPIs found in retrieval"
                },
                "next_node": "sql_generation"
            }
            state.update(fallback_result)
            return state
            
        kpi_metric = top_kpi.get("metric_name", "")
        kpi_description = top_kpi.get("description", "")
        kpi_sql = top_kpi.get("sql_query", "")
        
        # Simple, clean prompt - let the LLM decide naturally
        prompt = f"""
        USER REQUEST: "{task}"
        
        KPI NAME: "{kpi_metric}"
        KPI DESCRIPTION: {kpi_description}
        KPI SQL: {kpi_sql}

        Can this KPI completely answer the user's request?

        - If the KPI can answer the request exactly as-is, without any modifications: return "perfect_match"
        - If the KPI is relevant but needs ONLY minor modifications (adding a filter, changing date range): return "needs_minor_edit"  
        - If the KPI needs major changes (different grouping, different aggregation, different columns): return "not_relevant"
        - If the KPI answers a completely different question: return "not_relevant"

        Return only one word: perfect_match, needs_minor_edit, or not_relevant

        For example (this is just one dummy example, there can be many other examples with variations):
        
        if the KPI is: "Claims by Type (Work Comp, Cargo, Crash)"
        and the KPI description is: "Shows the distribution of claims across different claim categories (e.g., Work Compensation, Cargo, Crash). Helps identify which types of claims occur most frequently."
        and the KPI SQL is: "select [Accident or Incident Code] AS Type, COUNT(DISTINCT [Claim Number]) from PRD.CLAIMS_SUMMARY cs 
        group by [Accident or Incident Code]"
        and the user request is: "Show the distribution of claims across different claim categories"
        then the response should be "perfect_match"

        if the KPI is: "Claims by Type (Work Comp, Cargo, Crash)"
        and the KPI description is: "Shows the distribution of claims across different claim categories (e.g., Work Compensation, Cargo, Crash). Helps identify which types of claims occur most frequently."
        and the KPI SQL is: "select [Accident or Incident Code] AS Type, COUNT(DISTINCT [Claim Number]) from PRD.CLAIMS_SUMMARY cs 
        group by [Accident or Incident Code]"
        and the user request is: "Show the distribution of claims across different claim categories this month"
        then the response should be "needs_minor_edit"

        if the KPI is: "Claims by Type (Work Comp, Cargo, Crash)"
        and the KPI description is: "Shows the distribution of claims across different claim categories (e.g., Work Compensation, Cargo, Crash). Helps identify which types of claims occur most frequently."
        and the KPI SQL is: "select [Accident or Incident Code] AS Type, COUNT(DISTINCT [Claim Number]) from PRD.CLAIMS_SUMMARY cs 
        group by [Accident or Incident Code]"
        and the user request is: "Can you please provide me the number of preventable claims for the current month?"
        then the response should be "not_relevant"
        
        if the KPI is: "Total Open Claims this Week"
        and the KPI description is: "Provides the total number of new open claims reported in the current calendar week"
        and the KPI SQL is: "SELECT COUNT(DISTINCT [Claim Number]) AS [Claims Count] FROM PRD.CLAIMS_SUMMARY cs WHERE [Occurrence Date] >= DATEADD(WEEK, DATEDIFF(WEEK, 0, GETUTCDATE()), 0)"
        and the user request is: "What is the customer code for which maximum number of claims are present?"
        then the response should be "not_relevant" (because it needs different grouping and different time scope)
        """
        
        try:
            response = self.llm.invoke(prompt)
            response_text = response.content.strip()
            
            # Simple word detection - no complex parsing needed
            decision_word = response_text.lower().strip()
            
            # Validate and clean the response
            if decision_word == "perfect_match":
                decision_type = "perfect_match"
                next_node = "azure_retrieval"
            elif decision_word == "needs_minor_edit":
                decision_type = "needs_minor_edit"
                next_node = "kpi_editor"
            elif decision_word == "not_relevant":
                decision_type = "not_relevant"
                next_node = "sql_generation"
            else:
                # Fallback if LLM didn't follow instructions
                print(f"⚠️ [LLM_CHECKER] Unexpected response: '{response_text}' - defaulting to not_relevant")
                decision_type = "not_relevant"
                next_node = "sql_generation"
            
            llm_check_result = {
                "decision_type": decision_type,
                "reasoning": f"LLM decision based on KPI-request match",
                "confidence": "HIGH",
                "kpi_metric": kpi_metric,
                "kpi_sql": kpi_sql,
            }
            
            print(f"🧠 [LLM_CHECKER] Decision: {decision_type.upper()} → {next_node}")
            print(f"  Task: {task}")
            print(f"  KPI: {kpi_metric}")
            print(f"🔍 [LLM_CHECKER] Decision: {decision_type}")
            
            # If perfect match, also show the SQL that will be executed
            if decision_type == "perfect_match" and kpi_sql:
                print(f"📝 [LLM_CHECKER] SQL to execute: {kpi_sql}")
            
            # Update state and return it
            state["llm_check_result"] = llm_check_result
            state["next_node"] = next_node
            
            return state
            
        except Exception as e:
            # Fallback to conservative decision
            fallback_result = {
                "decision_type": "not_relevant",
                "reasoning": f"Error in decision making: {str(e)}. Defaulting to SQL generation.",
                "confidence": "LOW",
                "kpi_metric": kpi_metric,
                "kpi_sql": kpi_sql,
            }
            
            print(f"❌ [LLM_CHECKER] Error: {str(e)} - Using fallback decision")
            
            # Update state and return it
            state["llm_check_result"] = fallback_result
            state["next_node"] = "sql_generation"
            
            return state