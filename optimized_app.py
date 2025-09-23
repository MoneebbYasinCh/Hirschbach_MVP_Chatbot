import streamlit as st
import os
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, AIMessage
import pandas as pd
import logging

# Load environment variables
load_dotenv()

# Reduce logging verbosity (suppress noisy network logs and SDK internals)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("langchain").setLevel(logging.WARNING)
logging.getLogger("langgraph").setLevel(logging.WARNING)
logging.getLogger("Graph_Flow").setLevel(logging.WARNING)
logging.getLogger("Nodes").setLevel(logging.WARNING)
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

# Simple create-and-run approach - no caching, no complexity
def create_and_run_graph(user_input):
    """Create graph and run it directly - fast and simple"""
    from Graph_Flow.main_graph import create_main_graph
    
    # Create graph fresh each time
    graph = create_main_graph()
    
    # Run it immediately
    config = {"configurable": {"thread_id": "user_session"}}
    inputs = {
        "messages": [HumanMessage(content=user_input)],
        "user_query": user_input  # Store the original user query
    }
    
    return graph.invoke(inputs, config)

# Page configuration
st.set_page_config(
    page_title="Hirschbach AI Risk Intelligence Platform",
    page_icon="🚛",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .chat-scroll {
        max-height: 60vh;
        overflow-y: auto;
        padding-right: 10px;
        margin-bottom: 1rem;
    }
    .user-message {
        background-color: #007bff;
        color: white;
        padding: 12px 16px;
        border-radius: 18px 18px 4px 18px;
        margin: 8px 0 8px auto;
        max-width: 70%;
        text-align: right;
        word-break: break-word;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        line-height: 1.4;
    }
    .bot-message {
        background-color: #f8f9fa;
        color: #212529;
        padding: 12px 16px;
        border-radius: 18px 18px 18px 4px;
        margin: 8px auto 8px 0;
        max-width: 70%;
        text-align: left;
        word-break: break-word;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        line-height: 1.5;
        white-space: pre-wrap;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    }
    .data-section {
        background-color: #e8f5e8;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .insights-section {
        background-color: #fff3e0;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)


def initialize_session_state():
    """Initialize Streamlit session state"""
    if 'messages' not in st.session_state:
        st.session_state.messages = []


def main():
    """Main Streamlit app using LangGraph's built-in orchestration"""
    initialize_session_state()
    
    # Header
    st.markdown('<h1 class="main-header">🚛 Hirschbach AI Risk Intelligence Platform</h1>', unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.header("🔧 System Status")
        
        # Check environment variables
        env_vars = {
            "Azure OpenAI": (os.getenv("AZURE_OPENAI_ENDPOINT"), os.getenv("AZURE_OPENAI_API_KEY")),
            "Azure Search": (os.getenv("AZURE_SEARCH_SERVICE_ENDPOINT"), os.getenv("AZURE_SEARCH_API_KEY")),
            "Azure SQL": (os.getenv("SQL_CONNECTION_STRING"), None)
        }
        
        for service, (endpoint, key) in env_vars.items():
            if service == "Azure SQL":
                if endpoint:
                    st.success(f"✅ {service} configured")
                else:
                    st.error(f"❌ {service} not found")
            else:
                if endpoint and key:
                    st.success(f"✅ {service} configured")
                else:
                    st.warning(f"⚠️ {service} incomplete")
        
        # System status
        st.success("✅ System ready")
        st.info("🚀 Fast create-and-run approach")
        
        # Show current workflow status
        if hasattr(st.session_state, 'last_result') and st.session_state.last_result:
            result = st.session_state.last_result
            st.subheader("📊 Last Execution")
            
            # Show which nodes were executed
            if result.get("sql_generation_status"):
                status = result.get("sql_generation_status")
                color = "🟢" if status == "completed" else "🔴"
                st.write(f"{color} SQL Generation: {status}")
            
            if result.get("kpi_editor_status"):
                status = result.get("kpi_editor_status")
                color = "🟢" if status == "completed" else "🔴"
                st.write(f"{color} KPI Editor: {status}")
            
            if result.get("azure_retrieval_completed"):
                st.write("🟢 Azure Retrieval: completed")
            
            if result.get("insights_generated"):
                st.write("🟢 Insights: generated")
        
        # Clear conversation button
        if st.button("🗑️ Clear Conversation"):
            st.session_state.messages = []
            if hasattr(st.session_state, 'last_result'):
                del st.session_state.last_result
            st.rerun()
    
    # Chat container
    chat_container = st.container()
    with chat_container:
        st.markdown('<div class="chat-scroll">', unsafe_allow_html=True)
        
        # Display chat messages
        for msg_idx, message in enumerate(st.session_state.messages):
            if message["role"] == "user":
                st.markdown(f'<div class="user-message">{message["content"]}</div>', unsafe_allow_html=True)
            else:
                # Display bot message
                st.markdown(f'<div class="bot-message">{message["content"]}</div>', unsafe_allow_html=True)
                
                # Display data if available
                if message.get("has_data") and message.get("data") is not None:
                    st.markdown('<div class="data-section">', unsafe_allow_html=True)
                    st.subheader("📊 Retrieved Data")
                    
                    # Show execution details
                    if message.get("execution_details"):
                        details = message["execution_details"]
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Rows", f"{details.get('rows_returned', 0):,}")
                        with col2:
                            st.metric("Execution Time", details.get('execution_time', '0.0s'))
                        with col3:
                            st.metric("Status", "✅ Success" if details.get('success') else "❌ Failed")
                    
                    # Display the data
                    st.dataframe(message["data"], width='stretch')
                    
                    # Show row count
                    row_count = len(message['data'])
                    st.info(f"Total rows: {row_count:,}")
                    
                    # Show SQL query if available
                    if message.get("sql_query"):
                        with st.expander("🔍 SQL Query Executed", expanded=False):
                            st.code(message["sql_query"], language="sql")
                else:
                    # Show SQL query even if no data
                    if message.get("sql_query"):
                        st.markdown('<div class="data-section">', unsafe_allow_html=True)
                        st.subheader("🔍 Generated SQL Query")
                        st.code(message["sql_query"], language="sql")
                        st.info("ℹ️ This query was generated but not executed (no data returned)")
                        st.markdown('</div>', unsafe_allow_html=True)
                
                # Display insights if available
                if message.get("has_insights") and message.get("insights"):
                    insights = message["insights"]
                    st.markdown('<div class="insights-section">', unsafe_allow_html=True)
                    st.subheader("🧠 AI Generated Insights")
                    
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
                    
                    st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Suggested queries
    st.subheader("💡 Try These Sample Queries")
    col1, col2 = st.columns(2)
    
    sample_queries = [
        "Show me accident trends by state for the last 6 months",
        "Which drivers have the highest risk of preventable accidents?",
        "What are the most common types of cargo claims?",
        "Show me claims data filtered by status and coverage type",
        "Which regions have the highest claim frequency?",
        "Analyze accident patterns by driver experience level"
    ]
    
    for i, query in enumerate(sample_queries):
        with col1 if i % 2 == 0 else col2:
            if st.button(f"🔍 {query[:40]}...", key=f"sample_{i}", help=query):
                # Add the query to chat input
                st.session_state.suggested_query = query
                st.rerun()
    
    # Handle suggested query
    if hasattr(st.session_state, 'suggested_query'):
        user_input = st.session_state.suggested_query
        del st.session_state.suggested_query
    else:
        user_input = st.chat_input("Ask about accident trends, driver risk, or safety insights...")
    
    if user_input:
        # Add user message
        st.session_state.messages.append({"role": "user", "content": user_input})
        
        # Process through LangGraph - simple create-and-run
        with st.spinner("Processing your request..."):
            try:
                # Create and run graph directly - no caching, no complexity
                result = create_and_run_graph(user_input)
                
                # Store result for sidebar display
                st.session_state.last_result = result
                
                # Extract response and data
                final_response = result.get("final_response", "I've processed your request.")
                azure_data = result.get("azure_data", {})
                insights = result.get("generated_insights", {})
                
                # Prepare message content
                message_content = final_response
                
                # Check if we have data
                has_data = False
                data_df = None
                execution_details = None
                sql_query = None
                
                if azure_data.get("success") and azure_data.get("data"):
                    has_data = True
                    data_df = pd.DataFrame(azure_data["data"])
                    execution_details = {
                        "rows_returned": azure_data.get('rows_returned', 0),
                        "execution_time": azure_data.get('execution_time', '0.0s'),
                        "success": azure_data.get('success', False)
                    }
                    sql_query = azure_data.get('query_executed', '')
                    message_content += f"\n\n**Data Summary:** Retrieved {azure_data.get('rows_returned', 0)} rows in {azure_data.get('execution_time', '0.0s')}"
                elif azure_data.get("error"):
                    message_content += f"\n\n**Data Error:** {azure_data.get('error')}"
                    sql_query = azure_data.get('query_executed', '')
                else:
                    # Check if we have SQL query even without data
                    sql_query = result.get("generated_sql", "")
                    if not sql_query and result.get("top_kpi"):
                        sql_query = result.get("top_kpi", {}).get("sql_query", "")
                
                # Check if we have insights
                has_insights = bool(insights and insights.get("key_findings"))
                
                # Add AI response to chat
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": message_content,
                    "has_data": has_data,
                    "data": data_df,
                    "execution_details": execution_details,
                    "sql_query": sql_query,
                    "has_insights": has_insights,
                    "insights": insights
                })
                
            except Exception as e:
                error_message = f"I encountered an error: {str(e)}"
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": error_message,
                    "has_data": False,
                    "has_insights": False
                })
        
        st.rerun()
    
    # Instructions
    with st.expander("ℹ️ How to use this platform"):
        st.markdown("""
        **Hirschbach AI Risk Intelligence Platform using LangGraph orchestration:**
        
        1. **Ask Questions**: Type your questions about risk intelligence in natural language
        2. **AI Processing**: LangGraph automatically routes through:
           - **Orchestrator**: Determines if you need existing KPI or new SQL generation
           - **KPI Editor**: Modifies existing KPIs to match your request
           - **SQL Generation**: Creates new SQL queries from scratch
           - **Azure Retrieval**: Executes SQL and retrieves data from your database
           - **Insight Generation**: Analyzes data and provides recommendations
        3. **Get Results**: See data tables, SQL queries, and AI-generated insights
        4. **Full Workflow**: Input → Orchestrator → (KPI Editor OR SQL Generation) → Azure Retrieval → Insight Generation → Output
        
        **Features:**
        - ✅ **Smart Routing**: Automatically chooses between KPI editing and SQL generation
        - ✅ **Real-time Data**: Direct connection to your Azure SQL database
        - ✅ **AI Insights**: Automated analysis and recommendations
        - ✅ **Query Transparency**: See exactly what SQL was executed
        - ✅ **Performance Tracking**: Monitor execution times and row counts
        
        **This version uses LangGraph's built-in orchestration for optimal performance!**
        """)

if __name__ == "__main__":
    main()
