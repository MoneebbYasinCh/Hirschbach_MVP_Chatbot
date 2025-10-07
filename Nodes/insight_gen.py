from typing import Dict, Any
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
from langchain_openai import AzureChatOpenAI
import os
import json
import logging

class InsightGenerationNode:
    """Node for generating insights from Azure retrieval results"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # Initialize Azure OpenAI for intelligent insight generation
        self.llm = AzureChatOpenAI(
            azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-07-18"),
            temperature=0.3  # Slightly higher for more creative insights
        )
    
    def __call__(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate insights from Azure retrieval results
        
        Args:
            state: Current state containing Azure retrieval results
            
        Returns:
            Updated state with generated insights
        """
        self.logger.info("[INSIGHT GENERATION] Processing insights from Azure data...")
        print("[INSIGHT GENERATION] Processing insights from Azure data...")
        
        # Get Azure retrieval results
        azure_retrieval_completed = state.get("azure_retrieval_completed", False)
        azure_data = state.get("azure_data", {})
        kpi_processed = state.get("kpi_processed", False)
        
        # Debug prints to see what we're getting
        print(f"[INSIGHT GENERATION] Debug - azure_retrieval_completed: {azure_retrieval_completed}")
        print(f"[INSIGHT GENERATION] Debug - azure_data keys: {list(azure_data.keys()) if azure_data else 'None'}")
        print(f"[INSIGHT GENERATION] Debug - azure_data success: {azure_data.get('success') if azure_data else 'None'}")
        print(f"[INSIGHT GENERATION] Debug - azure_data rows: {azure_data.get('rows_returned') if azure_data else 'None'}")
        
        # Check multiple conditions to ensure we process available data
        has_success_flag = azure_data.get("success", False)
        has_data = azure_data.get("data", []) and len(azure_data.get("data", [])) > 0
        has_rows = azure_data.get("rows_returned", 0) > 0
        
        should_process = azure_data and (has_success_flag or has_data or has_rows)
        
        print(f"[INSIGHT GENERATION] Debug - should_process: {should_process} (success: {has_success_flag}, has_data: {has_data}, has_rows: {has_rows})")
        
        if should_process:
            self.logger.info(f"[INSIGHT GENERATION] Generating insights from {azure_data.get('rows_returned', 0)} rows of data")
            print(f"[INSIGHT GENERATION] Generating insights from {azure_data.get('rows_returned', 0)} rows of data")
            
            # Analyze the data to generate insights
            data = azure_data.get("data", [])
            columns = azure_data.get("columns", [])
            
            # Get user query from state for context
            user_query = state.get("user_query", "")
            if not user_query:
                messages = state.get("messages", [])
                if messages:
                    user_query = messages[-1].content if hasattr(messages[-1], 'content') else str(messages[-1])
            
            # Add user query to azure_data for insight generation
            azure_data_with_context = azure_data.copy()
            azure_data_with_context["user_query"] = user_query
            
            # Generate insights based on the data
            insights = self._generate_data_insights(data, columns, azure_data_with_context)
            
            state["insights_generated"] = True
            state["generated_insights"] = insights
            self.logger.info(f"[INSIGHT GENERATION] Generated insights: {len(insights.get('key_findings', []))} findings")
            print(f"[INSIGHT GENERATION] Generated insights: {len(insights.get('key_findings', []))} findings")
        else:
            self.logger.warning("[INSIGHT GENERATION] No Azure data available for insight generation")
            print("[INSIGHT GENERATION] No Azure data available for insight generation")
            state["insights_generated"] = False
            state["generated_insights"] = {
                "data_summary": "No data available for analysis",
                "key_findings": [],
                "recommendations": ["Ensure data retrieval is successful"],
                "execution_time": "0.0s"
            }
        
        if kpi_processed:
            print("[INSIGHT GENERATION] Processing KPI insights")
            # TODO: Implement KPI-specific insight generation
            state["kpi_insights_generated"] = True
        else:
            print("[INSIGHT GENERATION] No KPI data to process")
            state["kpi_insights_generated"] = False
        
        return state
    
    def _generate_data_insights(self, data: list, columns: list, azure_data: dict) -> dict:
        """Generate intelligent insights from the retrieved data using LLM"""
        
        if not data:
            return {
                "data_summary": "No data returned from query",
                "key_findings": ["No data returned from query"],
                "recommendations": ["Check query syntax and database connectivity"],
                "execution_time": azure_data.get("execution_time", "0.0s"),
                "data_preview": []
            }
        
        # Get user query from state for context
        user_query = azure_data.get("user_query", "data analysis")
        sql_query = azure_data.get("query_executed", "")
        
        # Prepare data for LLM analysis (limit to first 10 rows for token efficiency)
        data_sample = data[:10] if len(data) > 10 else data
        
        # Create comprehensive prompt for insight generation
        insight_prompt = f"""
        Analyze the following data and provide intelligent business insights for a transportation/logistics company. Keep the tone really polite, calm and professional. You dont need to be too straight forward as you need to speak politely just like humans.
        
        CONTEXT:
        - User Query: "{user_query}"
        - SQL Query: {sql_query}
        - Total Rows: {len(data)}
        - Columns: {columns}
        - Execution Time: {azure_data.get('execution_time', '0.0s')}
        
        DATA SAMPLE:
        {json.dumps(data_sample, indent=2, default=str)}
        
        ANALYSIS REQUIREMENTS:
        
        1. **Data Summary**: Describe what the data represents and key metrics
        
        2. **Key Findings**: Identify 3-5 most important insights from the data
        
        3. **Risk Assessment**: Highlight any risk factors or concerning patterns
        
        4. **Business Recommendations**: Provide 3-4 actionable recommendations
        
        5. **Trends & Patterns**: Note any significant patterns or anomalies

        6. **SQL Query Reasoning**: Explain how and why this SQL query was constructed to answer the user's question
           - What tables/columns were selected and why
           - What filters/conditions were applied and their purpose
           - What aggregations/groupings were used and why
           - Whether the query is doing filtering vs aggregation vs both
           - Any specific values that were mapped and why
        
        FOCUS AREAS:
        - Safety and risk management
        - Operational efficiency
        - Cost optimization
        - Compliance and regulatory issues
        - Performance trends
        
        IMPORTANT: Return ONLY the JSON object below, no markdown formatting, no code blocks, no extra text:
        
        {{
            "sql_query_reasoning": "Detailed explanation of how the SQL query was designed to answer the user's question, including column selection, filtering logic, aggregation strategy, and any value mappings applied",
            "data_summary": "Brief description of the data and key metrics",
            "key_findings": ["Finding 1", "Finding 2", "Finding 3", ...],
            "risk_assessment": "Assessment of risk factors and concerns",
            "recommendations": ["Recommendation 1", "Recommendation 2", "Recommendation 3", ...],
            "trends_patterns": "Description of notable trends or patterns",
            "business_impact": "Potential business impact and implications"
        }}
        """
        
        try:
            print(" [INSIGHT GENERATION] Generating AI-powered insights...")
            response = self.llm.invoke(insight_prompt)
            llm_insights = response.content.strip()
            
            # Clean up the response - remove any markdown code blocks if present
            if llm_insights.startswith("```json"):
                llm_insights = llm_insights[7:]  # Remove ```json
            elif llm_insights.startswith("```"):
                llm_insights = llm_insights[3:]   # Remove ```
            if llm_insights.endswith("```"):
                llm_insights = llm_insights[:-3]  # Remove trailing ```
            llm_insights = llm_insights.strip()
            
            print(f" [INSIGHT GENERATION] Cleaned response: {llm_insights[:200]}...")
            
            # Try to parse JSON response
            try:
                parsed_insights = json.loads(llm_insights)
                
                # Structure the final insights
                insights = {
                    "sql_query_reasoning": parsed_insights.get("sql_query_reasoning", "SQL query reasoning not provided"),
                    "data_summary": parsed_insights.get("data_summary", f"Retrieved {len(data)} rows in {azure_data.get('execution_time', '0.0s')}"),
                    "key_findings": parsed_insights.get("key_findings", []),
                    "risk_assessment": parsed_insights.get("risk_assessment", "No specific risks identified"),
                    "recommendations": parsed_insights.get("recommendations", []),
                    "trends_patterns": parsed_insights.get("trends_patterns", "No notable patterns identified"),
                    "business_impact": parsed_insights.get("business_impact", "Impact assessment pending"),
                    "execution_time": azure_data.get("execution_time", "0.0s"),
                    "data_preview": data_sample,
                    "total_rows": len(data)
                }
                
                print(f" [INSIGHT GENERATION] Generated {len(insights.get('key_findings', []))} key findings")
                return insights
                
            except json.JSONDecodeError:
                # Fallback: parse as plain text
                print(" [INSIGHT GENERATION] LLM response not in JSON format, parsing as text")
                return self._parse_text_insights(llm_insights, data, columns, azure_data)
                
        except Exception as e:
            print(f" [INSIGHT GENERATION] Error generating LLM insights: {str(e)}")
            # Fallback to basic analysis
            return self._generate_basic_insights(data, columns, azure_data)
    
    def _parse_text_insights(self, text_insights: str, data: list, columns: list, azure_data: dict) -> dict:
        """Parse LLM text response into structured insights"""
        lines = text_insights.split('\n')
        
        return {
            "data_summary": f"Retrieved {len(data)} rows with {len(columns)} columns in {azure_data.get('execution_time', '0.0s')}",
            "key_findings": [line.strip() for line in lines if line.strip() and not line.startswith('#')],
            "recommendations": ["Review data patterns for optimization opportunities"],
            "execution_time": azure_data.get("execution_time", "0.0s"),
            "data_preview": data[:5],
            "total_rows": len(data),
            "ai_analysis": text_insights
        }
    
    def _generate_basic_insights(self, data: list, columns: list, azure_data: dict) -> dict:
        """Fallback basic insights when LLM fails"""
        insights = {
            "data_summary": f"Retrieved {len(data)} rows with {len(columns)} columns in {azure_data.get('execution_time', '0.0s')}",
            "key_findings": [
                f"Dataset contains {len(data)} records",
                f"Data structure includes: {', '.join(columns)}"
            ],
            "recommendations": [
                "Review data for patterns and trends",
                "Consider implementing automated monitoring"
            ],
            "execution_time": azure_data.get("execution_time", "0.0s"),
            "data_preview": data[:5],
            "total_rows": len(data)
        }
        
        # Basic pattern detection
        if data and isinstance(data[0], dict):
            for col in columns:
                if any(isinstance(row.get(col), (int, float)) for row in data):
                    values = [row.get(col, 0) for row in data if isinstance(row.get(col), (int, float))]
                    if values:
                        total = sum(values)
                        avg = total / len(values)
                        insights["key_findings"].append(f"{col}: Total = {total:,.0f}, Average = {avg:.1f}")
        
        return insights
