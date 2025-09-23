from typing import Dict, Any, List, TypedDict, Annotated
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
import operator
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

# Import our custom modules
from Nodes.orchestrator import HirschbachOrchestrator
from Nodes.kpi_retrieval import KPIRetrievalNode
from Nodes.metadata_retrieval import MetadataRetrievalNode
from Nodes.llm_checker import LLMCheckerNode
from Nodes.kpi_editor import KPIEditorNode
from Nodes.sql_gen import SQLGenerationNode
from Nodes.azure_retrieval import AzureRetrievalNode
from Nodes.insight_gen import InsightGenerationNode
from State.main_state import RCMState

class RCMGraphState(TypedDict):
    """State for the main RCM graph with Azure retrieval workflow"""
    messages: Annotated[List[BaseMessage], operator.add]
    
    # Main orchestration
    orchestration: Dict[str, Any]
    
    # Main task queue from orchestrator
    main_task_queue: List[Dict[str, Any]]
    
    # Subgraph-specific queues
    nl_to_sql_queue: List[Dict[str, Any]]
    
    # Subgraph internal states
    nl_to_sql_state: Dict[str, Any]
    
    # Completed tasks
    completed_tasks: List[Dict[str, Any]]
    
    # Database results
    snowflake_results: Dict[str, Any]
    redshift_results: Dict[str, Any]
    azure_data: Dict[str, Any]
    
    # KPI and metadata retrieval results
    kpi_rag_results: List[Dict[str, Any]]
    metadata_rag_results: List[Dict[str, Any]]
    
    # LLM checker results
    llm_check_result: Dict[str, Any]
    
    # KPI editor results
    edited_kpi: Dict[str, Any]
    kpi_validated: bool
    
    # SQL generation results
    generated_sql: str
    sql_validated: bool
    sql_generation_status: str
    sql_generation_result: Dict[str, Any]
    
    # Azure retrieval results
    azure_retrieval_completed: bool
    kpi_processed: bool
    
    # Insight generation results
    insights: Dict[str, Any]
    
    # Error handling
    error_message: str
    
    # Multi-task data aggregation
    aggregated_data: List[Dict[str, Any]]
    task_results: Dict[str, Any]
    
    # Final response
    final_response: str
    
    # Workflow status
    workflow_status: str  # "active", "complete", "error"

class StartNode:
    """Start node that initializes the conversation"""
    
    def __call__(self, state: RCMGraphState) -> RCMGraphState:
        """
        Initialize the conversation state
        
        Args:
            state: Current state
            
        Returns:
            Updated state with initialization
        """
        print("[START] Initializing conversation...")
        
        # Ensure messages list exists
        if "messages" not in state:
            state["messages"] = []
        
        # Initialize other required fields if not present
        if "orchestration" not in state:
            state["orchestration"] = {}
        if "main_task_queue" not in state:
            state["main_task_queue"] = []
        if "nl_to_sql_queue" not in state:
            state["nl_to_sql_queue"] = []
        if "nl_to_sql_state" not in state:
            state["nl_to_sql_state"] = {}
        if "completed_tasks" not in state:
            state["completed_tasks"] = []
        if "snowflake_results" not in state:
            state["snowflake_results"] = {}
        if "redshift_results" not in state:
            state["redshift_results"] = {}
        if "azure_data" not in state:
            state["azure_data"] = {}
        if "kpi_rag_results" not in state:
            state["kpi_rag_results"] = []
        if "metadata_rag_results" not in state:
            state["metadata_rag_results"] = []
        if "llm_check_result" not in state:
            state["llm_check_result"] = {}
        if "edited_kpi" not in state:
            state["edited_kpi"] = {}
        if "kpi_validated" not in state:
            state["kpi_validated"] = False
        if "generated_sql" not in state:
            state["generated_sql"] = ""
        if "sql_validated" not in state:
            state["sql_validated"] = False
        if "sql_generation_status" not in state:
            state["sql_generation_status"] = ""
        if "sql_generation_result" not in state:
            state["sql_generation_result"] = {}
        if "azure_retrieval_completed" not in state:
            state["azure_retrieval_completed"] = False
        if "kpi_processed" not in state:
            state["kpi_processed"] = False
        if "error_message" not in state:
            state["error_message"] = ""
        if "aggregated_data" not in state:
            state["aggregated_data"] = []
        if "task_results" not in state:
            state["task_results"] = {}
        if "insights" not in state:
            state["insights"] = {}
        if "final_response" not in state:
            state["final_response"] = ""
        if "workflow_status" not in state:
            state["workflow_status"] = "active"
        
        print("[START] State initialized successfully")
        return state

class StopNode:
    """Stop node that finalizes the conversation"""
    
    def __call__(self, state: RCMGraphState) -> RCMGraphState:
        """
        Finalize the conversation and prepare final response
        
        Args:
            state: Current state
            
        Returns:
            Updated state with finalization
        """
        print("[STOP] Finalizing conversation...")
        
        # Set workflow status to complete
        state["workflow_status"] = "complete"
        
        # Generate final response if not already present
        if not state.get("final_response"):
            if state.get("error_message"):
                state["final_response"] = f"I encountered an error: {state['error_message']}"
            elif state.get("aggregated_data"):
                # Generate summary from aggregated data
                state["final_response"] = self._generate_data_summary(state["aggregated_data"])
            else:
                state["final_response"] = "I've completed processing your request."
        
        print(f"[STOP] Final response: {state['final_response'][:100]}...")
        return state
    
    def _generate_data_summary(self, aggregated_data: List[Dict[str, Any]]) -> str:
        """
        Generate a summary from aggregated data
        
        Args:
            aggregated_data: List of data from completed tasks
            
        Returns:
            Summary string
        """
        if not aggregated_data:
            return "No data available to summarize."
        
        summary_parts = []
        summary_parts.append("Here's a summary of the data I found:")
        
        for i, data in enumerate(aggregated_data, 1):
            if isinstance(data, dict) and "summary" in data:
                summary_parts.append(f"{i}. {data['summary']}")
            elif isinstance(data, dict) and "result" in data:
                summary_parts.append(f"{i}. {data['result']}")
            else:
                summary_parts.append(f"{i}. {str(data)}")
        
        return "\n".join(summary_parts)

class RCMGraph:
    """Main RCM graph with Azure retrieval workflow"""
    
    def __init__(self):
        self.orchestrator = HirschbachOrchestrator()
        self.kpi_retrieval = KPIRetrievalNode()
        self.metadata_retrieval = MetadataRetrievalNode()
        self.llm_checker = LLMCheckerNode()
        self.kpi_editor = KPIEditorNode()
        self.sql_generation = SQLGenerationNode()
        self.azure_retrieval = AzureRetrievalNode()
        self.insight_generation = InsightGenerationNode()
        self.start_node = StartNode()
        self.stop_node = StopNode()
        self.graph = self._build_graph()
    
    def _build_graph(self) -> StateGraph:
        """
        Build the main RCM graph with Azure retrieval workflow
        
        Returns:
            Configured StateGraph
        """
        # Create the graph
        workflow = StateGraph(RCMGraphState)
        
        # Add nodes
        workflow.add_node("start", self.start_node)
        workflow.add_node("orchestrator", self.orchestrator)
        workflow.add_node("kpi_retrieval", self.kpi_retrieval)
        workflow.add_node("metadata_retrieval", self.metadata_retrieval)
        workflow.add_node("llm_checker", self.llm_checker)
        workflow.add_node("kpi_editor", self.kpi_editor)
        workflow.add_node("sql_generation", self.sql_generation)
        workflow.add_node("azure_retrieval", self.azure_retrieval)
        workflow.add_node("insight_generation", self.insight_generation)
        workflow.add_node("stop", self.stop_node)
        
        # Define the flow
        workflow.set_entry_point("start")
        
        # Add edges for data analysis workflow
        workflow.add_edge("start", "orchestrator")
        
        # From orchestrator, route to either stop (direct reply) or data analysis
        workflow.add_conditional_edges(
            "orchestrator",
            self._route_after_orchestrator,
            {
                "direct_reply": "stop",
                "data_analysis": "kpi_retrieval"
            }
        )
        
        # Data analysis workflow
        workflow.add_edge("kpi_retrieval", "llm_checker")
        workflow.add_edge("metadata_retrieval", "llm_checker")
        workflow.add_edge("llm_checker", "kpi_editor")
        workflow.add_edge("llm_checker", "sql_generation")
        workflow.add_edge("kpi_editor", "azure_retrieval")
        workflow.add_edge("sql_generation", "azure_retrieval")
        workflow.add_edge("azure_retrieval", "insight_generation")
        workflow.add_edge("insight_generation", "stop")
        workflow.add_edge("stop", END)
        
        # Compile the graph
        memory = MemorySaver()
        return workflow.compile(checkpointer=memory)
    
    def _route_after_orchestrator(self, state: RCMGraphState) -> str:
        """
        Route after orchestrator based on workflow status
        
        Args:
            state: Current state
            
        Returns:
            Next node name
        """
        if state.get("workflow_status") == "complete":
            return "stop"
        else:
            return "data_analysis"
    
    def process_message(self, message: str, thread_id: str = "default") -> Dict[str, Any]:
        """
        Process a single message through the graph
        
        Args:
            message: User message to process
            thread_id: Thread ID for conversation tracking
            
        Returns:
            Dictionary with the final state
        """
        # Create initial state
        initial_state = {
            "messages": [HumanMessage(content=message)],
            "orchestration": {},
            "main_task_queue": [],
            "nl_to_sql_queue": [],
            "nl_to_sql_state": {},
            "completed_tasks": [],
            "snowflake_results": {},
            "redshift_results": {},
            "azure_data": {},
            "kpi_rag_results": [],
            "metadata_rag_results": [],
            "llm_check_result": {},
            "edited_kpi": {},
            "kpi_validated": False,
            "generated_sql": "",
            "sql_validated": False,
            "sql_generation_status": "",
            "sql_generation_result": {},
            "azure_retrieval_completed": False,
            "kpi_processed": False,
            "error_message": "",
            "aggregated_data": [],
            "task_results": {},
            "insights": {},
            "final_response": "",
            "workflow_status": "active"
        }
        
        # Process through the graph
        config = {"configurable": {"thread_id": thread_id}}
        final_state = self.graph.invoke(initial_state, config=config)
        
        return final_state
    
    def process_conversation(self, messages: List[BaseMessage], thread_id: str = "default") -> Dict[str, Any]:
        """
        Process a conversation with multiple messages
        
        Args:
            messages: List of conversation messages
            thread_id: Thread ID for conversation tracking
            
        Returns:
            Dictionary with the final state
        """
        # Create initial state with all messages
        initial_state = {
            "messages": messages,
            "orchestration": {},
            "main_task_queue": [],
            "nl_to_sql_queue": [],
            "nl_to_sql_state": {},
            "completed_tasks": [],
            "snowflake_results": {},
            "redshift_results": {},
            "azure_data": {},
            "kpi_rag_results": [],
            "metadata_rag_results": [],
            "llm_check_result": {},
            "edited_kpi": {},
            "kpi_validated": False,
            "generated_sql": "",
            "sql_validated": False,
            "sql_generation_status": "",
            "sql_generation_result": {},
            "azure_retrieval_completed": False,
            "kpi_processed": False,
            "error_message": "",
            "aggregated_data": [],
            "task_results": {},
            "insights": {},
            "final_response": "",
            "workflow_status": "active"
        }
        
        # Process through the graph
        config = {"configurable": {"thread_id": thread_id}}
        final_state = self.graph.invoke(initial_state, config=config)
        
        return final_state
    
    def get_conversation_history(self, thread_id: str = "default") -> List[BaseMessage]:
        """
        Get conversation history for a thread
        
        Args:
            thread_id: Thread ID to get history for
            
        Returns:
            List of messages in the conversation
        """
        config = {"configurable": {"thread_id": thread_id}}
        state = self.graph.get_state(config)
        
        if state and state.values:
            return state.values.get("messages", [])
        return []
    
    def clear_conversation(self, thread_id: str = "default"):
        """
        Clear conversation history for a thread
        
        Args:
            thread_id: Thread ID to clear
        """
        config = {"configurable": {"thread_id": thread_id}}
        # This would clear the conversation, but the exact method depends on the checkpointer implementation
        pass

# Factory function to create the main graph
def create_main_graph() -> RCMGraph:
    """
    Factory function to create the main Hirschbach graph
    
    Returns:
        RCMGraph instance
    """
    return RCMGraph()

# Example usage
if __name__ == "__main__":
    # Create the graph
    graph = create_main_graph()
    
    # Process a sample message
    sample_message = "How are we performing this month?"
    result = graph.process_message(sample_message)
    
    print("Final response:", result.get("final_response", "No response generated"))
    print("Workflow status:", result.get("workflow_status", "Unknown"))
