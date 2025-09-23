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
            temperature=0.1
        )
    
    def __call__(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Intelligently decide what to do with the retrieved KPI based on how well it matches the user's task.
        Returns a decision type that determines the next path in the workflow.
        """
        # Get user input from the latest message
        messages = state.get("messages", [])
        if messages:
            task = messages[-1].content if hasattr(messages[-1], 'content') else str(messages[-1])
        else:
            task = state.get("task", "")
        
        # Get KPI results from the state
        top_kpi = state.get("top_kpi")
        
        if not top_kpi:
            # No KPIs found, proceed to SQL generation
            return {
                "llm_check_result": {
                    "decision_type": "not_relevant",
                    "reasoning": "No KPIs found in retrieval"
                },
                "next_node": "sql_gen"
            }
        kpi_metric = top_kpi.get("metric_name", "")
        kpi_description = top_kpi.get("description", "")
        kpi_sql = top_kpi.get("sql_query", "")
        kpi_table_columns = top_kpi.get("table_columns", "")
        
        # Create comprehensive prompt for intelligent decision making
        prompt = f"""
        You are an expert at analyzing how well retrieved KPIs match user requests and deciding the best course of action.

        USER REQUEST: "{task}"
        
        RETRIEVED KPI: "{kpi_metric}"
        KPI DESCRIPTION: {kpi_description}
        KPI TABLE COLUMNS: {kpi_table_columns}

        DECISION CRITERIA - Choose the most appropriate action:

        1. **PERFECT_MATCH** → Use KPI directly
           - KPI exactly matches what user is asking for
           - No modifications needed
           - High confidence the KPI will answer the user's question

        2. **NEEDS_MINOR_EDIT** → Edit the KPI
           - KPI is very close but needs small adjustments
           - Same core concept but different filtering/grouping/metrics
           - Can be improved with minor SQL modifications

        3. **NOT_RELEVANT** → Use metadata retrieval
           - KPI doesn't match the user's request
           - Different concept entirely
           - Better to generate new SQL from scratch

        EXAMPLES (for claims_summary table):

        PERFECT_MATCH:
        - User: "show me closed claims by state" → KPI: "Closed Claims by State" → PERFECT_MATCH
        - User: "total incurred amount by adjuster" → KPI: "Total Incurred by Adjuster" → PERFECT_MATCH

        NEEDS_MINOR_EDIT:
        - User: "show me closed claims this year" → KPI: "Closed Claims by State" → NEEDS_MINOR_EDIT (needs date filter)
        - User: "claims by department last month" → KPI: "Claims by Department" → NEEDS_MINOR_EDIT (needs date filter)
        - User: "high value claims" → KPI: "Claims by Amount" → NEEDS_MINOR_EDIT (needs amount threshold)

        NOT_RELEVANT:
        - User: "patient demographics" → KPI: "Claims by State" → NOT_RELEVANT
        - User: "provider schedule" → KPI: "Claims by Adjuster" → NOT_RELEVANT
        - User: "appointment data" → KPI: "Claims Metrics" → NOT_RELEVANT

        ANALYSIS:
        - User wants: {task}
        - KPI provides: {kpi_metric}
        
        Consider:
        1. How well does the KPI concept match the user's intent?
        2. Are the metrics, grouping, and filtering appropriate?
        3. Would minor adjustments make it perfect?
        4. Is it worth editing or better to start fresh?

        DECISION: [PERFECT_MATCH/NEEDS_MINOR_EDIT/NOT_RELEVANT]
        REASONING: [Brief explanation of why you chose this decision]
        CONFIDENCE: [HIGH/MEDIUM/LOW]
        """
        
        try:
            response = self.llm.invoke(prompt)
            response_text = response.content.strip()
            
            # Parse the response
            decision_type, reasoning, confidence = self._parse_decision_response(response_text)
            
            # Determine next node based on decision
            if decision_type == "perfect_match":
                next_node = "aws_retrieval"
            elif decision_type == "needs_minor_edit":
                next_node = "kpi_editor"
            else:  # not_relevant
                next_node = "sql_gen"
            
            llm_check_result = {
                "decision_type": decision_type,
                "reasoning": reasoning,
                "confidence": confidence,
                "kpi_metric": kpi_metric,
                "kpi_sql": kpi_sql,
                "kpi_table_columns": kpi_table_columns,
            }
            
            print(f"🧠 [LLM_CHECKER] Decision: {decision_type.upper()} → {next_node}")
            print(f"  Task: {task}")
            print(f"  KPI: {kpi_metric}")
            print(f"  Confidence: {confidence}")
            print(f"  Reasoning: {reasoning}")
            
            return {
                "llm_check_result": llm_check_result,
                "next_node": next_node
            }
            
        except Exception as e:
            # Fallback to conservative decision
            fallback_result = {
                "decision_type": "not_relevant",
                "reasoning": f"Error in decision making: {str(e)}. Defaulting to SQL generation.",
                "confidence": "LOW",
                "kpi_metric": kpi_metric,
                "kpi_sql": kpi_sql,
                "kpi_table_columns": kpi_table_columns,
            }
            
            print(f"❌ [LLM_CHECKER] Error: {str(e)} - Using fallback decision")
            return {
                "llm_check_result": fallback_result,
                "next_node": "sql_gen"
            }
    
    def _parse_decision_response(self, response_text: str) -> tuple[str, str, str]:
        """Parse the LLM response to extract decision type, reasoning, and confidence"""
        lines = response_text.split('\n')
        decision_type = "not_relevant"  # Default fallback
        reasoning = "Unable to parse response"
        confidence = "LOW"
        
        for line in lines:
            line = line.strip()
            if line.startswith("DECISION:"):
                decision = line.replace("DECISION:", "").strip().upper()
                if decision in ["PERFECT_MATCH", "NEEDS_MINOR_EDIT", "NOT_RELEVANT"]:
                    decision_type = decision.lower()
            elif line.startswith("REASONING:"):
                reasoning = line.replace("REASONING:", "").strip()
            elif line.startswith("CONFIDENCE:"):
                confidence = line.replace("CONFIDENCE:", "").strip().upper()
        
        return decision_type, reasoning, confidence