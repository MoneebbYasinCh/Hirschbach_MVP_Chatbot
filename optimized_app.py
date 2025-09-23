import streamlit as st
import os
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, AIMessage
import pandas as pd

# Load environment variables
load_dotenv()

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
        
        # Check environment
        connection_string = os.getenv("SQL_CONNECTION_STRING")
        if connection_string:
            st.success("✅ Azure SQL configured")
        else:
            st.error("❌ SQL_CONNECTION_STRING not found")
        
        # System status
        st.success("✅ System ready")
        st.info("🚀 Fast create-and-run approach")
        
        # Clear conversation button
        if st.button("🗑️ Clear Conversation"):
            st.session_state.messages = []
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
                    st.dataframe(message["data"], width='stretch')
                    
                    # Show row count
                    row_count = len(message['data'])
                    st.info(f"Total rows: {row_count:,}")
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
    
    # Chat input
    user_input = st.chat_input("Ask about accident trends, driver risk, or safety insights...")
    
    if user_input:
        # Add user message
        st.session_state.messages.append({"role": "user", "content": user_input})
        
        # Process through LangGraph - simple create-and-run
        with st.spinner("Processing your request..."):
            try:
                print(f"[APP] Processing: {user_input}")
                # Create and run graph directly - no caching, no complexity
                print("[APP] Creating graph...")
                result = create_and_run_graph(user_input)
                print("[APP] Graph execution completed!")
                
                # Extract response and data
                final_response = result.get("final_response", "I've processed your request.")
                azure_data = result.get("azure_data", {})
                insights = result.get("generated_insights", {})
                
                # Prepare message content
                message_content = final_response
                
                # Check if we have data
                has_data = False
                data_df = None
                if azure_data.get("success") and azure_data.get("data"):
                    has_data = True
                    data_df = pd.DataFrame(azure_data["data"])
                    message_content += f"\n\n**Data Summary:** Retrieved {azure_data.get('rows_returned', 0)} rows in {azure_data.get('execution_time', '0.0s')}"
                
                # Check if we have insights
                has_insights = bool(insights and insights.get("key_findings"))
                
                # Add AI response to chat
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": message_content,
                    "has_data": has_data,
                    "data": data_df,
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
        
        1. **Ask Questions**: Type your questions about risk intelligence
        2. **AI Processing**: LangGraph automatically routes and processes your request
        3. **Get Results**: See data, insights, and AI-generated recommendations
        4. **Full Workflow**: Input → Orchestrator → Azure Retrieval → Insight Generation → Output
        
        **This version uses LangGraph's built-in orchestration for optimal performance!**
        """)

if __name__ == "__main__":
    main()
