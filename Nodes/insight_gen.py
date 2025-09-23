from typing import Dict, Any
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
import logging

class InsightGenerationNode:
    """Node for generating insights from Azure retrieval results"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
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
        
        if azure_retrieval_completed and azure_data and azure_data.get("success"):
            self.logger.info(f"[INSIGHT GENERATION] Generating insights from {azure_data.get('rows_returned', 0)} rows of data")
            print(f"[INSIGHT GENERATION] Generating insights from {azure_data.get('rows_returned', 0)} rows of data")
            
            # Analyze the data to generate insights
            data = azure_data.get("data", [])
            columns = azure_data.get("columns", [])
            
            # Generate insights based on the data
            insights = self._generate_data_insights(data, columns, azure_data)
            
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
        """Generate insights from the retrieved data"""
        insights = {
            "data_summary": f"Retrieved {azure_data.get('rows_returned', 0)} rows in {azure_data.get('execution_time', '0.0s')}",
            "key_findings": [],
            "recommendations": [],
            "execution_time": azure_data.get("execution_time", "0.0s"),
            "data_preview": data[:5] if data else []  # Show first 5 rows
        }
        
        if not data:
            insights["key_findings"].append("No data returned from query")
            insights["recommendations"].append("Check query syntax and database connectivity")
            return insights
        
        # Analyze the data structure
        insights["key_findings"].append(f"Data contains {len(columns)} columns: {', '.join(columns)}")
        insights["key_findings"].append(f"Total records analyzed: {len(data)}")
        
        # Look for patterns in the data
        if "Type" in columns and "ClaimCount" in columns:
            # This looks like accident/incident data
            total_claims = sum(row.get("ClaimCount", 0) for row in data if isinstance(row.get("ClaimCount"), (int, float)))
            insights["key_findings"].append(f"Total claims across all types: {total_claims:,}")
            
            # Find the highest risk type
            if data:
                highest_risk = max(data, key=lambda x: x.get("ClaimCount", 0))
                insights["key_findings"].append(f"Highest risk type: {highest_risk.get('Type', 'Unknown')} with {highest_risk.get('ClaimCount', 0):,} claims")
                insights["recommendations"].append(f"Focus safety efforts on {highest_risk.get('Type', 'Unknown')} incidents")
        
        # General recommendations
        insights["recommendations"].extend([
            "Monitor trends over time for early warning signs",
            "Implement targeted safety interventions based on data patterns",
            "Regular data review recommended for continuous improvement"
        ])
        
        return insights
