import os
from typing import Dict, Any, List
from openai import AzureOpenAI
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class KPIRetrievalNode:
    """Node for retrieving from KPI RAG using Azure AI Search"""
    
    def __init__(self):
        # Initialize Azure OpenAI client for embeddings
        api_version = os.getenv("AZURE_OPENAI_API_VERSION")
        
        if api_version:
            self.openai_client = AzureOpenAI(
                azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
                api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                api_version=api_version
            )
        else:
            self.openai_client = AzureOpenAI(
                azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
                api_key=os.getenv("AZURE_OPENAI_API_KEY")
            )
        
        # Initialize Azure AI Search
        self.service_endpoint = os.getenv("AZURE_SEARCH_SERVICE_ENDPOINT")
        self.api_key = os.getenv("AZURE_SEARCH_API_KEY")
        self.index_name = os.getenv("AZURE_SEARCH_INDEX_NAME", "kpis-hml-mvp")
        
        if not self.service_endpoint or not self.api_key:
            raise ValueError("Azure Search service endpoint and API key must be provided")
        
        self.credential = AzureKeyCredential(self.api_key)
        self.search_client = SearchClient(
            endpoint=self.service_endpoint,
            index_name=self.index_name,
            credential=self.credential
        )
    
    def _create_embedding(self, text: str) -> List[float]:
        """Create embedding for text using Azure OpenAI"""
        embeddings_deployment = os.getenv("AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT", "text-embedding-3-small")
        response = self.openai_client.embeddings.create(
            input=text,
            model=embeddings_deployment
        )
        return response.data[0].embedding
    
    def _retrieve_kpis(self, query: str, top_k: int = 3) -> List[Dict[str, Any]]:
        """Retrieve top K relevant KPIs from Azure AI Search"""
        # Create embedding for the query
        query_embedding = self._create_embedding(query)
        
        # Search Azure AI Search index
        results = self.search_client.search(
            search_text=None,  # Pure vector search
            vector_queries=[{
                "kind": "vector",
                "vector": query_embedding,
                "k": top_k,
                "fields": "content_vector"
            }],
            select=["id", "metric_name", "table_columns", "sql_query", "description"],
            top=top_k
        )
        
        # Process results
        kpi_results = []
        for result in results:
            score = getattr(result, '@search.score', 0.0)
            
            kpi_result = {
                "id": result.get("id", ""),
                "score": score,
                "metric_name": result.get("metric_name", ""),
                "table_columns": result.get("table_columns", ""),
                "sql_query": result.get("sql_query", ""),
                "description": result.get("description", "")
            }
            kpi_results.append(kpi_result)
        
        return kpi_results
    
    def __call__(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Retrieve relevant KPIs from KPI RAG for claims_summary analysis"""
        # Get user query from state (preferred) or fallback to last message
        user_query = state.get("user_query", "")
        if not user_query:
            # Fallback to last message if user_query not in state
            messages = state.get("messages", [])
            if messages:
                latest_message = messages[-1]
                user_query = latest_message.content if hasattr(latest_message, 'content') else str(latest_message)
        
        if not user_query:
            print("No user query found for KPI retrieval")
            return state
        
        print(f"[KPI RETRIEVAL] Processing query: {user_query}")
        
        # Retrieve top 3 KPIs from Azure AI Search
        kpi_results = self._retrieve_kpis(user_query, top_k=3)
        
        if kpi_results:
            print(f"[KPI RETRIEVAL] Found {len(kpi_results)} relevant KPIs")
        else:
            print("[KPI RETRIEVAL] No relevant KPIs found")
        
        # Update state with KPI results - only the top KPI
        state["kpi_retrieval_status"] = "completed"
        
        if kpi_results:
            selected_kpi = kpi_results[0]
            print(f"📊 [KPI RETRIEVAL] Selected KPI: {selected_kpi.get('metric_name', 'Unknown')}")
            print(f"📝 [KPI RETRIEVAL] Description: {selected_kpi.get('description', 'No description')}")
            print(f"🔍 [KPI RETRIEVAL] SQL Query: {selected_kpi.get('sql_query', 'No SQL')[:100]}...")
            
            state["top_kpi"] = {
                "metric_name": selected_kpi.get('metric_name', ''),
                "description": selected_kpi.get('description', ''),
                "sql_query": selected_kpi.get('sql_query', ''),
                "table_columns": selected_kpi.get('table_columns', '')
            }
        else:
            state["top_kpi"] = None
        
        return state
        