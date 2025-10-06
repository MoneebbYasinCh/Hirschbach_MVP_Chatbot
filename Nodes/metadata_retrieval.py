import os
from typing import Dict, Any, List
from langchain_openai import AzureChatOpenAI
from openai import AzureOpenAI
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import time

# Load environment variables
load_dotenv()

class MetadataRetrievalNode:
    """Node for iterative LLM-driven metadata retrieval using Azure AI Search"""
    
    def __init__(self):
        # Initialize Azure OpenAI client
        api_version = os.getenv("AZURE_OPENAI_API_VERSION")
        deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
        
        if api_version:
            self.llm = AzureChatOpenAI(
                azure_deployment=deployment_name,
                azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
                api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                api_version=api_version,
                temperature=0.0
            )
        else:
            self.llm = AzureChatOpenAI(
                azure_deployment=deployment_name,
                azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
                api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                temperature=0.0
            )
        
        # Initialize Azure OpenAI client for embeddings
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
        self.index_name = os.getenv("AZURE_SEARCH_INDEX_NAME_2", "metadata-hml-mvp")
        
        if not self.service_endpoint or not self.api_key:
            raise ValueError("Azure Search service endpoint and API key must be provided")
        
        self.credential = AzureKeyCredential(self.api_key)
        self.search_client = SearchClient(
            endpoint=self.service_endpoint,
            index_name=self.index_name,
            credential=self.credential
        )
        
        
        # Determine optimal worker counts for parallel execution
        self.max_llm_workers = min(5, os.cpu_count() or 4)  # Limit LLM workers
        self.max_search_workers = min(10, (os.cpu_count() or 4) * 2)  # More workers for I/O bound operations
    
    def _create_embedding(self, text: str) -> List[float]:
        """Create embedding for text using Azure OpenAI"""
        embeddings_deployment = os.getenv("AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT", "text-embedding-3-large")
        response = self.openai_client.embeddings.create(
            input=text,
            model=embeddings_deployment
        )
        return response.data[0].embedding
    
    def _retrieve_metadata(self, query: str, top_k: int = 2, max_retries: int = 3) -> List[Dict[str, Any]]:
        """Retrieve top K metadata entries from Azure AI Search with retry logic"""
        for attempt in range(max_retries):
            try:
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
                    select=["id", "content", "column_name", "description", "data_type", "table_name", "primary_key", "foreign_key"],
                    top=top_k
                )
                
                # Process results
                metadata_results = []
                for result in results:
                    # Extract score from Azure AI Search results
                    score = 0.0
                    if isinstance(result, dict) and '@search.score' in result:
                        score = result['@search.score']
                    elif hasattr(result, '@search.score'):
                        score = getattr(result, '@search.score', 0.0)
                    
                    metadata_result = {
                        "id": result.get("id", ""),
                        "score": score,
                        "column_name": result.get("column_name", ""),
                        "description": result.get("description", ""),
                        "data_type": result.get("data_type", ""),
                        "table_name": result.get("table_name", ""),
                        "primary_key": result.get("primary_key", ""),
                        "foreign_key": result.get("foreign_key", "")
                    }
                    metadata_results.append(metadata_result)
                
                return metadata_results
                
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Attempt {attempt + 1} failed for query '{query}': {e}. Retrying...")
                    time.sleep(0.5 * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    print(f"All {max_retries} attempts failed for query '{query}': {e}")
                    return []
        
        return []
    


    def _analyze_query_requirements(self, query: str) -> Dict[str, Any]:
        """Analyze the query to understand what data elements are needed"""
        prompt = f"""
        Analyze this query to understand what data elements are needed for analysis.
        
        Query: "{query}"
        
        What does this query need to answer the question?
        
        Return a simple JSON object with:
        {{
            "needs_counting": true/false,
            "needs_grouping": true/false, 
            "needs_filtering": true/false,
            "needs_amounts": true/false,
            "needs_dates": true/false,
            "needs_locations": true/false,
            "needs_status": true/false,
            "needs_people": true/false,
            "needs_categories": true/false
        }}
        """
        
        response = self.llm.invoke(prompt)
        try:
            import json
            json_start = response.content.find('{')
            json_end = response.content.rfind('}') + 1
            if json_start != -1 and json_end > json_start:
                json_str = response.content[json_start:json_end]
                return json.loads(json_str)
        except:
            pass
        
        # Simple fallback
        return {
            "needs_counting": True,
            "needs_grouping": False,
            "needs_filtering": False,
            "needs_amounts": False,
            "needs_dates": False,
            "needs_locations": False,
            "needs_status": False,
            "needs_people": False,
            "needs_categories": False
        }
    
    def _create_targeted_search_descriptions(self, query: str, requirements: Dict[str, Any]) -> List[str]:
        """Generate semantic search descriptions based on query analysis"""
        
        prompt = f"""
        Based on this query analysis, generate comprehensive generic column descriptions for semantic search. The goal is to find all relevant database columns that could be used to generate SQL for this query.
        
        Query: "{query}"
        
        Analysis shows the query needs:
        - Counting: {requirements.get('needs_counting', False)}
        - Grouping: {requirements.get('needs_grouping', False)}
        - Filtering: {requirements.get('needs_filtering', False)}
        - Amounts: {requirements.get('needs_amounts', False)}
        - Dates: {requirements.get('needs_dates', False)}
        - Locations: {requirements.get('needs_locations', False)}
        - Status: {requirements.get('needs_status', False)}
        - People: {requirements.get('needs_people', False)}
        - Categories: {requirements.get('needs_categories', False)}
        
        Generate detailed column descriptions that would help find the right data columns.
        Only generate descriptions for columns that are actually needed for this specific query.
        Consider ALL possible column types that might be relevant, not just the ones marked as True.
        Think about additional columns that might be needed for filtering or context.
        Make each description comprehensive and specific to improve semantic search accuracy.
        
        Examples of detailed descriptions:
        - "column for counting and identifying individual records with unique identifiers and primary keys"
        - "column for geographic location data including states, cities, addresses, and regional information"
        - "column for status information including open/closed states, flags, and operational status"
        - "column for monetary amounts and financial data including costs, totals, incurred amounts, and payments"
        - "column for date and time information including occurrence dates, creation dates, and timestamps"
        - "column for person names including assignees, drivers, managers, and responsible parties"
        - "column for category and type information including codes, classifications, and groupings"
        - "column for measurement and aggregation data including counts, sums, averages, and statistics"
        - "column for relationship data including foreign keys and references to other tables"
        - "column for descriptive text including comments, notes, and detailed explanations"
        
        Generate as many relevant descriptions as needed to comprehensively cover the query requirements.
        Return only the descriptions, one per line:
        """
        
        response = self.llm.invoke(prompt)
        descriptions = [line.strip() for line in response.content.split('\n') if line.strip()]
        return descriptions


    
    
    def _deduplicate_columns(self, columns: List[Dict]) -> List[Dict]:
        """Remove duplicate columns, keeping the one with highest score"""
        unique_columns = {}
        for col in columns:
            col_name = col.get('column_name', '')
            if col_name not in unique_columns or col.get('score', 0) > unique_columns[col_name].get('score', 0):
                unique_columns[col_name] = col
        
        return list(unique_columns.values())
    
    def _iterative_metadata_retrieval(self, task: str) -> List[Dict]:
        """Iteratively retrieve metadata until LLM thinks it's complete - runs all iterations in parallel"""
        
        print(f"Starting parallel iterative metadata retrieval for: {task}")
        start_time = time.time()
        
        # Step 1: Analyze the query to understand what's needed
        requirements = self._analyze_query_requirements(task)
        
        # Step 2: Generate semantic search descriptions
        targeted_descriptions = self._create_targeted_search_descriptions(task, requirements)
        
        # Use only the targeted descriptions
        all_search_descriptions = targeted_descriptions
        
        if not all_search_descriptions:
            print(" No search descriptions generated, falling back to basic metadata retrieval")
            # Fallback: get some basic columns
            basic_columns = self._retrieve_metadata(task, 10)
            return basic_columns
        
        # Run all vector searches in parallel using ThreadPoolExecutor
        all_columns = []
        
        with ThreadPoolExecutor(max_workers=self.max_search_workers) as executor:
            # Submit all search tasks for all descriptions
            future_to_description = {
                executor.submit(self._retrieve_metadata, description, 4): description 
                for description in all_search_descriptions
            }
            
            # Collect results as they complete with timeout
            completed_searches = 0
            
            for future in as_completed(future_to_description):
                description = future_to_description[future]
                try:
                    columns = future.result(timeout=45)  # 45 second timeout per search
                    all_columns.extend(columns)
                    completed_searches += 1
                    
                except TimeoutError:
                    print(f"⏰ Timeout retrieving metadata for '{description}'")
                except Exception as e:
                    print(f" Error retrieving metadata for '{description}': {e}")
                    # Continue with other results even if one fails
        
        print(f" Completed {completed_searches}/{len(all_search_descriptions)} searches successfully")
        
        # Deduplicate columns, keeping the one with highest score
        all_columns = self._deduplicate_columns(all_columns)
        
        print(f" Retrieved {len(all_columns)} unique columns")
        
        # Ensure Occurrence Date is always available (from actual metadata CSV)
        occurrence_date_exists = any(col.get('column_name') == 'Occurrence Date' for col in all_columns)
        if not occurrence_date_exists:
            occurrence_date_entry = {
                "id": "guaranteed_occurrence_date",
                "column_name": "Occurrence Date",
                "description": "Date when the accident or incident actually occurred. Primary field for time-based analysis.",
                "data_type": "date",
                "table_name": "PRD.CLAIMS_SUMMARY",
                "primary_key": "",
                "foreign_key": ""
            }
            all_columns.append(occurrence_date_entry)
            print(" [METADATA RETRIEVAL] Added guaranteed Occurrence Date entry")
        
        return all_columns
    
    
    def __call__(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Semantic metadata retrieval node for data analysis"""
        # Get user query from state (preferred) or fallback to last message
        task = state.get("user_query", "")
        if not task:
            # Fallback to last message if user_query not in state
            messages = state.get("messages", [])
            if messages:
                latest_message = messages[-1]
                task = latest_message.content if hasattr(latest_message, 'content') else str(latest_message)
        
        if not task:
            print("No user query found for metadata retrieval")
            return state

        print(f"[METADATA RETRIEVAL] Processing query: {task}")

        # Get columns using semantic search
        retrieved_columns = self._iterative_metadata_retrieval(task)

        print(f"[METADATA RETRIEVAL] Retrieved {len(retrieved_columns)} columns")
        
        # Print the retrieved columns
        if retrieved_columns:
            print(" [METADATA RETRIEVAL] Retrieved columns:")
            for i, col in enumerate(retrieved_columns, 1):
                col_name = col.get('column_name', 'Unknown')
                col_desc = col.get('description', 'No description')
                col_type = col.get('data_type', 'Unknown type')
                col_score = col.get('score', 0)
                print(f"  {i}. {col_name} ({col_type}) - {col_desc} (score: {col_score:.2f})")
        else:
            print(" [METADATA RETRIEVAL] No columns retrieved")
        
        # Update state with results
        state["metadata_rag_results"] = retrieved_columns
        state["metadata_retrieval_status"] = "completed"
        
        # Create a lookup dictionary for easy access by column name
        state["metadata_lookup"] = {
            col.get('column_name', ''): {
                "description": col.get('description', ''),
                "data_type": col.get('data_type', ''),
                "score": col.get('score', 0),
                "primary_key": col.get('primary_key', ''),
                "foreign_key": col.get('foreign_key', '')
            }
            for col in retrieved_columns
        }
        
        return state
