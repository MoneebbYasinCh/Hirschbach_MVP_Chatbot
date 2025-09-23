import streamlit as st
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

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
    .data-section {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

def test_azure_connection():
    """Test Azure SQL connection and return sample data"""
    try:
        import pyodbc
        
        # Get connection string from environment
        connection_string = os.getenv("SQL_CONNECTION_STRING")
        if not connection_string:
            return {"error": "SQL_CONNECTION_STRING not found in environment"}
        
        # Connect and execute sample query
        with pyodbc.connect(connection_string) as conn:
            with conn.cursor() as cursor:
                # Execute the sample query
                query = "SELECT [Accident or Incident Code] AS Type, COUNT(DISTINCT [Claim Number]) as ClaimCount FROM PRD.CLAIMS_SUMMARY cs GROUP BY [Accident or Incident Code]"
                cursor.execute(query)
                
                # Get column names
                columns = [column[0] for column in cursor.description]
                
                # Fetch data
                rows = cursor.fetchall()
                data = []
                for row in rows:
                    row_dict = {}
                    for i, value in enumerate(row):
                        row_dict[columns[i]] = value
                    data.append(row_dict)
                
                return {
                    "success": True,
                    "data": data,
                    "columns": columns,
                    "row_count": len(data),
                    "query": query
                }
                
    except Exception as e:
        return {"error": str(e)}

def generate_insights(data):
    """Generate simple insights from the data"""
    if not data or not data.get("success"):
        return {"error": "No data available for analysis"}
    
    rows = data["data"]
    if not rows:
        return {"error": "No rows returned"}
    
    # Calculate insights
    total_claims = sum(row.get("ClaimCount", 0) for row in rows if isinstance(row.get("ClaimCount"), (int, float)))
    
    # Find highest risk type
    highest_risk = max(rows, key=lambda x: x.get("ClaimCount", 0))
    
    insights = {
        "total_claims": total_claims,
        "highest_risk_type": highest_risk.get("Type", "Unknown"),
        "highest_risk_count": highest_risk.get("ClaimCount", 0),
        "total_types": len(rows),
        "recommendations": [
            f"Focus safety efforts on {highest_risk.get('Type', 'Unknown')} incidents",
            "Monitor trends over time for early warning signs",
            "Implement targeted safety interventions"
        ]
    }
    
    return insights

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
        
        # Test connection button
        if st.button("🔍 Test Database Connection"):
            with st.spinner("Testing connection..."):
                result = test_azure_connection()
                if result.get("success"):
                    st.success("✅ Connection successful!")
                    st.session_state.connection_test = result
                else:
                    st.error(f"❌ Connection failed: {result.get('error')}")
    
    # Main content
    st.subheader("💬 Risk Intelligence Dashboard")
    
    # Show connection test results
    if "connection_test" in st.session_state:
        test_result = st.session_state.connection_test
        
        if test_result.get("success"):
            st.markdown('<div class="data-section">', unsafe_allow_html=True)
            st.success(f"✅ Retrieved {test_result['row_count']} rows from database")
            
            # Display data
            st.subheader("📊 Accident/Incident Data")
            df = pd.DataFrame(test_result["data"])
            st.dataframe(df, width='stretch')
            
            # Show query
            st.subheader("🔍 Query Executed")
            st.code(test_result["query"], language="sql")
            
            # Generate and display insights
            st.subheader("🧠 Generated Insights")
            insights = generate_insights(test_result)
            
            if not insights.get("error"):
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Claims", f"{insights['total_claims']:,}")
                with col2:
                    st.metric("Highest Risk Type", insights['highest_risk_type'])
                with col3:
                    st.metric("Claims for Highest Risk", f"{insights['highest_risk_count']:,}")
                
                st.subheader("💡 Recommendations")
                for rec in insights['recommendations']:
                    st.markdown(f"• {rec}")
            else:
                st.error(f"Error generating insights: {insights['error']}")
            
            st.markdown('</div>', unsafe_allow_html=True)
    
    # Chat input
    st.subheader("💬 Ask Questions About Your Data")
    user_input = st.chat_input("Ask about accident trends, risk patterns, or safety insights...")
    
    if user_input:
        st.write(f"**You asked:** {user_input}")
        
        # Simple response based on the data
        if "connection_test" in st.session_state and st.session_state.connection_test.get("success"):
            data = st.session_state.connection_test
            insights = generate_insights(data)
            
            st.write("**AI Response:**")
            if not insights.get("error"):
                st.write(f"Based on your data, I found {insights['total_types']} different accident/incident types with a total of {insights['total_claims']:,} claims.")
                st.write(f"The highest risk type is **{insights['highest_risk_type']}** with {insights['highest_risk_count']:,} claims.")
                st.write("**Recommendations:**")
                for rec in insights['recommendations']:
                    st.write(f"• {rec}")
            else:
                st.write("I need to see the data first. Please click 'Test Database Connection' in the sidebar.")
        else:
            st.write("Please first test the database connection using the button in the sidebar to load the data.")
    
    # Instructions
    with st.expander("ℹ️ How to use this platform"):
        st.markdown("""
        **This is a simplified version of the Hirschbach AI Risk Intelligence Platform:**
        
        1. **Test Connection**: Click the "Test Database Connection" button in the sidebar
        2. **Ask Questions**: Use the chat input below to ask about your data
        3. **View Data**: See real accident/incident data from your Azure SQL Database
        4. **Get Insights**: Automatic analysis of the data with recommendations
        
        **Current Data**: Shows accident/incident types and claim counts from your database.
        """)

if __name__ == "__main__":
    main()
