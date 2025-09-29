from typing import Dict, Any, List, TypedDict, Annotated
from langchain_core.messages import BaseMessage
import operator

class HirschbachGraphState(TypedDict):
    """State for Hirschbach Risk Intelligence workflow"""
    messages: Annotated[List[BaseMessage], operator.add]
    user_query: str  # Store the original user query
    
    # Main orchestration
    orchestration: Dict[str, Any]
    
    # Database results
    azure_data: Dict[str, Any]  # Azure SQL Database results
    
    # RAG results
    kpi_rag_results: List[Dict[str, Any]]
    metadata_rag_results: List[Dict[str, Any]]
    metadata_lookup: Dict[str, Any]  # Quick lookup for metadata
    top_kpi: Dict[str, Any]  # Top KPI from retrieval
    
    # Node statuses
    kpi_retrieval_status: str
    metadata_retrieval_status: str
    sql_generation_status: str
    azure_retrieval_status: str
    insight_generation_status: str
    kpi_editor_status: str  # KPI editor status
    
    # Generated content
    generated_sql: str
    generated_insights: Dict[str, Any]
    
    # Error handling
    error_message: str
    
    # Multi-task data aggregation
    aggregated_data: List[Dict[str, Any]]  # Store data from multiple tasks
    
    # Final response
    final_response: str
    
    # Workflow status
    workflow_status: str  # "active", "complete", "error"