import streamlit as st
import os
from dotenv import load_dotenv
from typing import Dict, Any, List
from langchain_core.messages import HumanMessage, AIMessage
import time

# Load environment variables
load_dotenv()

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
    .response-section {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
        border-left: 4px solid #1f77b4;
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

# Lazy load the actual graph
@st.cache_resource
def load_graph():
    """Load the actual graph from Graph_Flow"""
    try:
        from Graph_Flow.main_graph import create_main_graph
        return create_main_graph()
    except Exception as e:
        st.error(f"Error loading graph: {str(e)}")
        return None

def main():
    """Main Streamlit app"""
    st.markdown('<div class="main-header">🚛 Hirschbach AI Risk Intelligence Platform</div>', unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.header("🔧 System Status")
        
        # Check environment
        connection_string = os.getenv("SQL_CONNECTION_STRING")
        if connection_string:
            st.success("✅ Azure SQL configured")
        else:
            st.error("❌ SQL_CONNECTION_STRING not found")
        
        # Initialize graph button
        if st.button("🚀 Initialize AI System"):
            with st.spinner("Loading AI system..."):
                st.session_state.graph = load_graph()
                if st.session_state.graph:
                    st.success("✅ AI System ready!")
                else:
                    st.error("❌ Failed to load AI system")
    
    # Main content
    st.subheader("💬 Ask Questions About Risk Intelligence")
    
    # Show initialization status
    if "graph" not in st.session_state:
        st.info("🚀 Click 'Initialize AI System' in the sidebar to start")
        return
    
    # Chat interface
    user_input = st.chat_input("Ask about accident trends, driver risk, or safety insights...")
    
    if user_input:
        # Add user message to chat
        st.session_state.messages = st.session_state.get("messages", [])
        st.session_state.messages.append({"role": "user", "content": user_input})
        
        # Process through the actual graph
        with st.spinner("Processing your request through AI system..."):
            result = st.session_state.graph.process_message(user_input, thread_id="main")
        
        # Add AI response to chat
        ai_response = result.get("final_response", "No response generated")
        st.session_state.messages.append({"role": "assistant", "content": ai_response})
        
        # Store results for display
        st.session_state.last_result = result
        
        # Rerun to show results
        st.rerun()
    
    # Display chat history
    if "messages" in st.session_state:
        for message in st.session_state.messages:
            if message["role"] == "user":
                st.markdown(f"**You:** {message['content']}")
            else:
                st.markdown(f"**AI:** {message['content']}")
    
    # Display detailed results from last query
    if "last_result" in st.session_state:
        result = st.session_state.last_result
        
        # Show Azure data if available
        if result.get("azure_data") and result.get("azure_retrieval_completed"):
            azure_data = result["azure_data"]
            
            if azure_data.get("success"):
                st.markdown('<div class="data-section">', unsafe_allow_html=True)
                st.subheader("📊 Retrieved Data")
                st.success(f"✅ Retrieved {azure_data.get('rows_returned', 0)} rows in {azure_data.get('execution_time', '0.0s')}")
                
                # Show data preview
                if azure_data.get("data"):
                    import pandas as pd
                    df = pd.DataFrame(azure_data["data"])
                    st.dataframe(df, width='stretch')
                
                # Show query
                st.subheader("🔍 SQL Query Executed")
                st.code(azure_data.get("query_executed", "N/A"), language="sql")
                st.markdown('</div>', unsafe_allow_html=True)
        
        # Show insights if available
        if result.get("insights_generated") and result.get("generated_insights"):
            insights = result["generated_insights"]
            
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
    
    # Instructions
    with st.expander("ℹ️ How to use this platform"):
        st.markdown("""
        **Streamlined Hirschbach AI Risk Intelligence Platform:**
        
        1. **Initialize**: Click "Initialize AI System" in the sidebar
        2. **Ask Questions**: Use the chat input to ask about risk intelligence
        3. **Get Results**: See data, insights, and AI-generated recommendations
        4. **Full Workflow**: Input → Orchestrator → Azure Retrieval → Insight Generation → Output
        
        **This version uses the actual graph workflow but optimized for performance!**
        """)

if __name__ == "__main__":
    main()
