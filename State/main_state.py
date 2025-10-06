from typing import Dict, Any, List, TypedDict, Annotated
from langchain_core.messages import BaseMessage
import operator

class HirschbachGraphState(TypedDict):
    """State for Hirschbach Risk Intelligence workflow"""
    messages: List[BaseMessage]  # Remove operator.add to prevent accumulation
    user_query: str  # Store the original user query
    
    # SQL Query History for context preservation
    sql_query_history: List[Dict[str, Any]]  # Store previous SQL queries and their context
    
    # Main orchestration
    orchestration: Dict[str, Any]
    
    # Main task queue from orchestrator
    main_task_queue: List[Dict[str, Any]]
    
    # Subgraph-specific queues
    nl_to_sql_queue: List[Dict[str, Any]]
    
    # Subgraph internal states
    nl_to_sql_state: Dict[str, Any]  # Internal state for NL-to-SQL subgraph
    
    # Completed tasks
    completed_tasks: List[Dict[str, Any]]
    
    # Database results
    snowflake_results: Dict[str, Any]
    redshift_results: Dict[str, Any]  # Redshift query results
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
    task_results: Dict[str, Any]  # Store results from each completed task
    
    # Insights
    insights: Dict[str, Any]
    
    # Final response
    final_response: str
    
    # Workflow status
    workflow_status: str  # "active", "complete", "error"