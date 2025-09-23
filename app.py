import streamlit as st
import os
from dotenv import load_dotenv
from typing import Dict, Any, List
from langchain_core.messages import HumanMessage, AIMessage
import json
import time

# Enable caching for better performance
@st.cache_resource
def load_graph():
    """Load the graph with caching to improve performance"""
    try:
        from Graph_Flow.main_graph import create_main_graph
        return create_main_graph()
    except Exception as e:
        st.error(f"Error loading graph: {str(e)}")
        return None

# Load environment variables
load_dotenv()

# Import our custom modules
from Graph_Flow.main_graph import create_main_graph
from State.main_state import RCMState

# Page configuration
st.set_page_config(
    page_title="Hirschbach AI Risk Intelligence Platform",
    page_icon="🚛",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better UI
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .chat-message {
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
        border-left: 4px solid #1f77b4;
        color: #000000;
    }
    .user-message {
        background-color: #e3f2fd;
        border-left-color: #2196f3;
        color: #000000;
    }
    .assistant-message {
        background-color: #f3e5f5;
        border-left-color: #9c27b0;
        color: #000000;
    }
    .status-indicator {
        padding: 0.25rem 0.5rem;
        border-radius: 0.25rem;
        font-size: 0.8rem;
        font-weight: bold;
    }
    .status-active {
        background-color: #4caf50;
        color: white;
    }
    .status-complete {
        background-color: #2196f3;
        color: white;
    }
    .status-error {
        background-color: #f44336;
        color: white;
    }
    .orchestration-info {
        background-color: #f5f5f5;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
        color: #000000;
    }
    .stChatMessage {
        color: #000000 !important;
    }
    .stChatMessage p {
        color: #000000 !important;
    }
    .stChatMessage strong {
        color: #000000 !important;
    }
</style>
""", unsafe_allow_html=True)

class HirschbachChatInterface:
    """Streamlit interface for Hirschbach AI Risk Intelligence Platform"""
    
    def __init__(self):
        self.graph = None
        self.initialize_session_state()
    
    def get_graph(self):
        """Lazy load the graph to avoid import errors during app startup"""
        if self.graph is None:
            with st.spinner("Initializing AI system..."):
                self.graph = load_graph()
                if self.graph is None:
                    st.stop()
        return self.graph
    
    def initialize_session_state(self):
        """Initialize Streamlit session state"""
        if "messages" not in st.session_state:
            st.session_state.messages = []
        if "state" not in st.session_state:
            st.session_state.state = self.create_initial_state()
        if "orchestration_history" not in st.session_state:
            st.session_state.orchestration_history = []
    
    def create_initial_state(self) -> RCMState:
        """Create initial state for the RCM system"""
        return RCMState(
            messages=[],
            orchestration={},
            main_task_queue=[],
            nl_to_sql_queue=[],
            nl_to_sql_state={},
            completed_tasks=[],
            snowflake_results={},
            redshift_results={},
            error_message="",
            aggregated_data=[],
            task_results={},
            insights={},
            final_response="",
            workflow_status="inactive"
        )
    
    def display_header(self):
        """Display the main header"""
        st.markdown('<div class="main-header">🚛 Hirschbach AI Risk Intelligence MVP</div>', unsafe_allow_html=True)
        
        # Status indicator
        status = st.session_state.state.get("workflow_status", "inactive")
        status_class = f"status-{status}" if status in ["active", "complete", "error"] else "status-inactive"
        status_text = status.upper() if status != "inactive" else "READY"
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.markdown(f'<div class="status-indicator {status_class}">Status: {status_text}</div>', 
                       unsafe_allow_html=True)
    
    def display_sidebar(self):
        """Display sidebar with system information"""
        with st.sidebar:
            st.header("🔧 System Information")
            
            # Environment status
            st.subheader("Environment Status")
            azure_openai_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
            azure_openai_key = os.getenv("AZURE_OPENAI_API_KEY")
            azure_search_endpoint = os.getenv("AZURE_SEARCH_SERVICE_ENDPOINT")
            azure_search_key = os.getenv("AZURE_SEARCH_API_KEY")
            
            st.write(f"Azure OpenAI: {'✅ Connected' if azure_openai_endpoint and azure_openai_key else '❌ Not configured'}")
            st.write(f"Azure AI Search: {'✅ Connected' if azure_search_endpoint and azure_search_key else '❌ Not configured'}")
            
            # Current state info
            st.subheader("Graph State")
            state = st.session_state.state
            
            if state.get("orchestration"):
                orchestration = state["orchestration"]
                st.write(f"**Tasks:** {len(orchestration.get('tasks', []))}")
                st.write(f"**Routed to:** {orchestration.get('routed_to', 'N/A')}")
                st.write(f"**Status:** {state.get('workflow_status', 'N/A')}")
            
            # Show queue status
            st.subheader("Queue Status")
            st.write(f"**NL-to-SQL:** {len(state.get('nl_to_sql_queue', []))}")
            st.write(f"**Risk Analyzer:** {len(state.get('risk_analyzer_queue', []))}")
            st.write(f"**Compliance Monitor:** {len(state.get('compliance_monitor_queue', []))}")
            st.write(f"**Intervention Engine:** {len(state.get('intervention_engine_queue', []))}")
            
            # Clear conversation button
            if st.button("🗑️ Clear Conversation", type="secondary"):
                st.session_state.messages = []
                st.session_state.state = self.create_initial_state()
                st.session_state.orchestration_history = []
                st.rerun()
    
    def display_orchestration_info(self):
        """Display orchestration information if available"""
        state = st.session_state.state
        
        if state.get("orchestration"):
            orchestration = state["orchestration"]
            
            with st.expander("📋 Orchestration Details", expanded=False):
                st.markdown(f"""
                <div class="orchestration-info">
                    <strong>Original Input:</strong> {orchestration.get('original_input', 'N/A')}<br>
                    <strong>Routed to:</strong> {orchestration.get('routed_to', 'N/A')}<br>
                    <strong>Task Count:</strong> {len(orchestration.get('tasks', []))}
                </div>
                """, unsafe_allow_html=True)
                
                # Display tasks
                if orchestration.get('tasks'):
                    st.subheader("Tasks")
                    for i, task in enumerate(orchestration['tasks'], 1):
                        st.write(f"{i}. {task.get('description', 'N/A')}")
    
    def display_azure_data_and_insights(self):
        """Display Azure data and insights if available"""
        state = st.session_state.state
        
        # Display Azure data
        if state.get("azure_data") and state.get("azure_retrieval_completed"):
            azure_data = state["azure_data"]
            
            with st.expander("📊 Azure Data Results", expanded=True):
                if azure_data.get("success"):
                    st.success(f"✅ Successfully retrieved {azure_data.get('rows_returned', 0)} rows in {azure_data.get('execution_time', '0.0s')}")
                    
                    # Display data preview
                    if azure_data.get("data"):
                        st.subheader("Data Preview")
                        # Limit data display for performance
                        display_data = azure_data["data"][:100]  # Show max 100 rows
                        data_df = st.dataframe(display_data, width='stretch')
                        if len(azure_data["data"]) > 100:
                            st.info(f"Showing first 100 rows of {len(azure_data['data'])} total rows")
                        
                        # Display query executed
                        st.subheader("Query Executed")
                        st.code(azure_data.get("query_executed", "N/A"), language="sql")
                else:
                    st.error(f"❌ Data retrieval failed: {azure_data.get('error', 'Unknown error')}")
        
        # Display insights
        if state.get("insights_generated") and state.get("generated_insights"):
            insights = state["generated_insights"]
            
            with st.expander("🔍 Generated Insights", expanded=True):
                st.markdown(f"**Data Summary:** {insights.get('data_summary', 'N/A')}")
                
                # Key findings
                if insights.get("key_findings"):
                    st.subheader("Key Findings")
                    for finding in insights["key_findings"]:
                        st.markdown(f"• {finding}")
                
                # Recommendations
                if insights.get("recommendations"):
                    st.subheader("Recommendations")
                    for rec in insights["recommendations"]:
                        st.markdown(f"💡 {rec}")
                
                # Data preview in insights
                if insights.get("data_preview"):
                    st.subheader("Data Sample")
                    st.dataframe(insights["data_preview"], width='stretch')
    
    def display_suggested_queries(self):
        """Display suggested queries for risk intelligence"""
        st.markdown("### Try These Out")
        
        # Risk intelligence queries based on the reference images
        suggested_queries = [
            "Which drivers present the highest risk of a preventable accident based on recent crash history and come away from home?",
            "Where are preventable crash claims exceeding our safety targets, and what's driving the increase?",
            "Which regions and/or business segments are most exposed to future accidents in the next quarter?",
            "Who are the top drivers we should proactively reach out to this week based on elevated accident risk?",
            "How do accident trends compare between company drivers and non-company drivers over the last 6 months?",
            "Which drivers have had the highest concentration of high-risk drivers under their supervision?"
        ]
        
        # Create columns for the queries
        cols = st.columns(2)
        for i, query in enumerate(suggested_queries):
            with cols[i % 2]:
                if st.button(f"🔍 {query[:60]}...", key=f"suggested_{i}", help=query):
                    # Process the suggested query
                    self.process_user_input(query)
                    st.rerun()
    
    def display_messages(self):
        """Display chat messages"""
        for message in st.session_state.messages:
            if isinstance(message, HumanMessage):
                st.markdown(f"""
                <div class="chat-message user-message">
                    <strong>You:</strong> {message.content}
                </div>
                """, unsafe_allow_html=True)
            elif isinstance(message, AIMessage):
                st.markdown(f"""
                <div class="chat-message assistant-message">
                    <strong>Assistant:</strong> {message.content}
                </div>
                """, unsafe_allow_html=True)
    
    def process_user_input(self, user_input: str):
        """Process user input through the graph"""
        try:
            # Add user message to session state
            user_message = HumanMessage(content=user_input)
            st.session_state.messages.append(user_message)
            
            # Process through the graph
            with st.spinner("Processing your risk intelligence request..."):
                graph = self.get_graph()
                result = graph.process_message(user_input, thread_id="main")
                
                # Update session state with graph result
                st.session_state.state = result
                
                # Add AI response if available
                if result.get("final_response"):
                    ai_message = AIMessage(content=result["final_response"])
                    st.session_state.messages.append(ai_message)
                
                # Store orchestration info
                if result.get("orchestration"):
                    st.session_state.orchestration_history.append(result["orchestration"])
            
        except Exception as e:
            st.error(f"Error processing request: {str(e)}")
            error_message = AIMessage(content=f"I encountered an error: {str(e)}")
            st.session_state.messages.append(error_message)
    
    def run(self):
        """Main run method for the Streamlit app"""
        self.display_header()
        self.display_sidebar()
        
        # Main chat interface
        st.subheader("💬 Ask Anything. Get Insights")
        st.markdown("*Natural language queries powered by AI. Simply ask a question and get actionable insights instantly.*")
        
        # Show loading state if graph not initialized
        if self.graph is None:
            st.info("🚀 Click 'Send' to initialize the AI system and start your first query!")
        
        # Display orchestration info
        self.display_orchestration_info()
        
        # Display Azure data and insights
        self.display_azure_data_and_insights()
        
        # Suggested queries section
        self.display_suggested_queries()
        
        # Display messages
        self.display_messages()
        
        # Chat input
        user_input = st.chat_input("Tell me about your fleet risk, driver performance or safety metrics...")
        
        if user_input:
            self.process_user_input(user_input)
            st.rerun()
        
        # Instructions
        with st.expander("ℹ️ How to use this Risk Intelligence Platform", expanded=False):
            st.markdown("""
            **This Hirschbach AI Risk Intelligence Platform transforms fleet risk from reactive reports to predictive insights:**
            
            🎯 **Instant Insights:**
            - Ask in plain English, get immediate answers - no delays, smarter decisions
            - Natural language queries powered by AI for actionable insights instantly
            
            🔮 **Predictive & Proactive:**
            - Spot high-risk drivers early and prevent avoidable crashes
            - Identify regions and business segments most exposed to future accidents
            - Anticipate issues before they become problems
            
            🤖 **Automated Guidance:**
            - Delivers tailored coaching and recommendations for each driver
            - Ranks interventions by ROI, impact & cost
            - Builds a confident, data-driven roadmap for safety improvements
            
            **Key Capabilities:**
            - Driver risk prioritization and root cause analysis
            - Predictive accident risk assessment
            - Safety target monitoring and trend analysis
            - Proactive intervention recommendations
            - Fleet-wide risk intelligence and reporting
            """)

def main():
    """Main function to run the Streamlit app"""
    # Check environment variables
    if not os.getenv("AZURE_OPENAI_ENDPOINT") or not os.getenv("AZURE_OPENAI_API_KEY"):
        st.error("❌ Azure OpenAI configuration not found. Please set AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_API_KEY in your .env file.")
        st.stop()
    
    if not os.getenv("AZURE_SEARCH_SERVICE_ENDPOINT") or not os.getenv("AZURE_SEARCH_API_KEY"):
        st.warning("⚠️ Azure AI Search configuration not found. RAG features may not work properly.")
    
    # Create and run the interface
    interface = HirschbachChatInterface()
    interface.run()

if __name__ == "__main__":
    main()
