from typing import Dict, Any, List
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
import logging
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

try:
    from utils.logger import HirschbachLogger, log_node_initialization, log_node_execution, log_node_completion, log_error
except ImportError:
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('hirschbach_graph.log')
            ]
        )

from State.main_state import HirschbachGraphState

class StartNode:
    """Start node that initializes the conversation"""

    def __init__(self):
        try:
            self.logger = HirschbachLogger(__name__)
        except NameError:
            # Fallback to basic logging
            self.logger = logging.getLogger(__name__)

    def __call__(self, state: HirschbachGraphState) -> HirschbachGraphState:
        """
        Initialize the conversation state

        Args:
            state: Current state

        Returns:
            Updated state with initialization
        """
        self.logger.node_initialized("Initializing Hirschbach Risk Intelligence conversation")

        # Ensure essential fields exist
        if "messages" not in state:
            state["messages"] = []
        if "workflow_status" not in state:
            state["workflow_status"] = "active"

        self.logger.node_success("State initialized successfully")
        return state

class EndNode:
    """End node that finalizes the conversation"""

    def __init__(self):
        try:
            self.logger = HirschbachLogger(__name__)
        except NameError:
            # Fallback to basic logging
            self.logger = logging.getLogger(__name__)

    def __call__(self, state: HirschbachGraphState) -> HirschbachGraphState:
        """
        Finalize the conversation and prepare final response

        Args:
            state: Current state

        Returns:
            Updated state with finalization
        """
        self.logger.node_started("Finalizing Hirschbach Risk Intelligence conversation")

        # Set workflow status to complete
        state["workflow_status"] = "complete"

        # Generate final response if not already present
        if not state.get("final_response"):
            if state.get("error_message"):
                state["final_response"] = f"I encountered an error: {state['error_message']}"
            elif state.get("aggregated_data"):
                # Generate summary from aggregated data
                state["final_response"] = self._generate_risk_summary(state["aggregated_data"])
            else:
                state["final_response"] = "I've completed processing your risk intelligence request."

        self.logger.node_processing("Generating final response", state['final_response'][:100] + "...")

        # Clear processing state while preserving essential data for UI
        self._clear_processing_state(state)
        self.logger.node_processing("Cleared processing state for next query")

        self.logger.node_completed("Conversation finalized successfully")
        return state
    
    def _clear_processing_state(self, state: HirschbachGraphState) -> None:
        """
        Clear processing state while preserving essential data for UI display
        
        Args:
            state: Current state to clean
        """
        # Fields to preserve for UI display AND CONTEXT PRESERVATION
        preserve_fields = {
            "messages",           # Chat history - CRITICAL for context preservation
            "final_response",     # Final response for UI
            "azure_data",         # Data results for UI tables
            "generated_insights", # Insights for UI display
            "workflow_status",    # Completion status
            "user_query"          # Keep user query for context
        }
        
        # Fields to clear (processing state)
        clear_fields = [
            # Query processing (keep user_query for context)
            "task", "orchestrator_decision",
            
            # KPI processing
            "kpi_retrieval_completed", "top_kpi", "kpi_rag_results",
            "kpi_editor_status", "kpi_editor_result", "kpi_editor_error",
            "edited_kpi", "kpi_validated",
            
            # Metadata processing
            "metadata_retrieval_completed", "metadata_rag_results",
            
            # LLM checker
            "llm_check_result", "next_node",
            
            # SQL generation
            "sql_generation_status", "sql_generation_result", "sql_generation_error",
            "generated_sql", "sql_validated",
            
            # Azure retrieval processing flags
            "azure_retrieval_completed", "insights_triggered", "kpi_processed",
            
            # Insight generation flags
            "insights_generated", "kpi_insights_generated",
            
            # Error handling
            "error_message", "aggregated_data"
        ]
        
        # Clear processing fields
        for field in clear_fields:
            if field in state:
                del state[field]

        self.logger.node_debug(f"Cleared {len(clear_fields)} processing fields", f"Preserved {len(preserve_fields)} UI fields")
    
    def _generate_risk_summary(self, aggregated_data: List[Dict[str, Any]]) -> str:
        """
        Generate a risk intelligence summary from aggregated data
        
        Args:
            aggregated_data: List of data from completed tasks
            
        Returns:
            Summary string focused on risk insights
        """
        if not aggregated_data:
            return "No risk data available to summarize."
        
        summary_parts = []
        summary_parts.append("Here's a summary of the risk intelligence insights I found:")
        
        for i, data in enumerate(aggregated_data, 1):
            if isinstance(data, dict) and "summary" in data:
                summary_parts.append(f"{i}. {data['summary']}")
            elif isinstance(data, dict) and "result" in data:
                summary_parts.append(f"{i}. {data['result']}")
            else:
                summary_parts.append(f"{i}. {str(data)}")
        
        return "\n".join(summary_parts)

# Factory function to create the main graph
def create_main_graph():
    """
    Factory function to create the main Hirschbach Risk Intelligence graph
    
    Returns:
        Compiled LangGraph StateGraph
    """
    # Create the graph
    workflow = StateGraph(HirschbachGraphState)
    
    # Add nodes
    start_node = StartNode()
    end_node = EndNode()
    
    # Simple node creation - no logging overhead
    def get_orchestrator():
        from Nodes.orchestrator import HirschbachOrchestrator
        return HirschbachOrchestrator()
    
    def get_kpi_retrieval():
        from Nodes.kpi_retrieval import KPIRetrievalNode
        return KPIRetrievalNode()
    
    def get_metadata_retrieval():
        from Nodes.metadata_retrieval import MetadataRetrievalNode
        return MetadataRetrievalNode()
    
    def get_llm_checker():
        from Nodes.llm_checker import LLMCheckerNode
        return LLMCheckerNode()
    
    def get_kpi_editor():
        from Nodes.kpi_editor import KPIEditorNode
        return KPIEditorNode()
    
    def get_sql_generation():
        from Nodes.sql_gen import SQLGenerationNode
        return SQLGenerationNode()
    
    def get_azure_retrieval():
        from Nodes.azure_retrieval import AzureRetrievalNode
        return AzureRetrievalNode()
    
    def get_insight_generation():
        from Nodes.insight_gen import InsightGenerationNode
        return InsightGenerationNode()
    
    # Add all nodes
    workflow.add_node("start", start_node)
    workflow.add_node("orchestrator", get_orchestrator())
    workflow.add_node("kpi_retrieval", get_kpi_retrieval())
    workflow.add_node("metadata_retrieval", get_metadata_retrieval())
    workflow.add_node("llm_checker", get_llm_checker())
    workflow.add_node("kpi_editor", get_kpi_editor())
    workflow.add_node("sql_generation", get_sql_generation())
    workflow.add_node("azure_retrieval", get_azure_retrieval())
    workflow.add_node("insight_generation", get_insight_generation())
    workflow.add_node("end", end_node)
    
    # Define the flow with conditional routing
    workflow.set_entry_point("start")
    
    # Add edges
    workflow.add_edge("start", "orchestrator")
    
    # Conditional routing from orchestrator
    def route_after_orchestrator(state):
        """Route based on orchestrator decision"""
        workflow_status = state.get("workflow_status", "active")
        if workflow_status == "complete":
            return "end"
        else:
            return "kpi_retrieval"  # Start with KPI retrieval
    
    workflow.add_conditional_edges(
        "orchestrator",
        route_after_orchestrator,
        {
            "kpi_retrieval": "kpi_retrieval",
            "end": "end"
        }
    )
    
    # KPI retrieval flows to metadata retrieval
    workflow.add_edge("kpi_retrieval", "metadata_retrieval")
    
    # Metadata retrieval flows to LLM checker
    workflow.add_edge("metadata_retrieval", "llm_checker")
    
    # LLM checker conditional routing
    def route_after_llm_checker(state):
        """Route based on LLM checker decision"""
        # Respect explicit end routing first
        next_node = state.get("next_node")
        if next_node == "end":
            return "end"
        llm_check_result = state.get("llm_check_result", {})
        decision_type = llm_check_result.get("decision_type", "not_relevant")
        
        if decision_type == "perfect_match":
            return "azure_retrieval"
        elif decision_type == "needs_minor_edit":
            return "kpi_editor"
        else:  # not_relevant
            return "sql_generation"
    
    workflow.add_conditional_edges(
        "llm_checker",
        route_after_llm_checker,
        {
            "azure_retrieval": "azure_retrieval",
            "kpi_editor": "kpi_editor",
            "sql_generation": "sql_generation",
            "end": "end"
        }
    )
    
    # KPI editor flows to Azure retrieval
    workflow.add_edge("kpi_editor", "azure_retrieval")
    
    # SQL generation flows to Azure retrieval
    workflow.add_edge("sql_generation", "azure_retrieval")
    
    # Azure retrieval flows to insight generation
    workflow.add_edge("azure_retrieval", "insight_generation")
    
    # Insight generation flows to end
    workflow.add_edge("insight_generation", "end")
    workflow.add_edge("end", END)
    
    # Compile the graph
    memory = MemorySaver()
    compiled_graph = workflow.compile(checkpointer=memory)
    return compiled_graph


