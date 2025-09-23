import os
import pandas as pd
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SearchIndex,
    SearchField,
    SearchFieldDataType,
    VectorSearch,
    VectorSearchProfile,
    HnswAlgorithmConfiguration,
    HnswParameters
)
# Document operations are now direct methods on SearchClient
from openai import AzureOpenAI
from dotenv import load_dotenv
import json
from typing import List, Dict, Any

# Load environment variables
load_dotenv()

class KPIRAGPusher:
    """Standalone script to push KPI data to Azure AI Search vector database"""
    
    def __init__(self):
        # Initialize Azure OpenAI client
        api_version = os.getenv("AZURE_OPENAI_API_VERSION")
        if api_version:
            self.openai_client = AzureOpenAI(
                azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
                api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                api_version=api_version
            )
        else:
            # Let Azure OpenAI use its default version
            self.openai_client = AzureOpenAI(
                azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
                api_key=os.getenv("AZURE_OPENAI_API_KEY")
            )
        
        # Azure AI Search configuration
        self.service_endpoint = os.getenv("AZURE_SEARCH_SERVICE_ENDPOINT")
        self.api_key = os.getenv("AZURE_SEARCH_API_KEY")
        self.index_name = os.getenv("AZURE_SEARCH_INDEX_NAME", "kpis-hml-mvp")
        
        if not self.service_endpoint or not self.api_key:
            raise ValueError("Azure Search service endpoint and API key must be provided in environment variables")
        
        # Initialize Azure AI Search clients
        self.credential = AzureKeyCredential(self.api_key)
        self.search_client = SearchClient(
            endpoint=self.service_endpoint,
            index_name=self.index_name,
            credential=self.credential
        )
        self.index_client = SearchIndexClient(
            endpoint=self.service_endpoint,
            credential=self.credential
        )
        
        # Create or get index
        self._create_or_get_index()
        
    def _create_or_get_index(self):
        """Create or get existing Azure AI Search index"""
        try:
            # Check if index exists
            existing_indexes = [index.name for index in self.index_client.list_indexes()]
            
            if self.index_name not in existing_indexes:
                print(f"Creating new Azure AI Search index: {self.index_name}")
                self._create_index()
            else:
                print(f"Using existing Azure AI Search index: {self.index_name}")
                
        except Exception as e:
            print(f"Error checking/creating index: {e}")
            raise
    
    def _create_index(self):
        """Create Azure AI Search index with vector search capabilities"""
        # Define fields for the index - simple and clean
        fields = [
            SearchField(name="id", type=SearchFieldDataType.String, key=True),
            SearchField(name="content", type=SearchFieldDataType.String, searchable=True),
            SearchField(name="metric_name", type=SearchFieldDataType.String, searchable=True),
            SearchField(name="table_columns", type=SearchFieldDataType.String, searchable=True),
            SearchField(name="sql_query", type=SearchFieldDataType.String, searchable=True),
            SearchField(name="description", type=SearchFieldDataType.String, searchable=True),
            SearchField(
                name="content_vector",
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                vector_search_dimensions=1536,  # OpenAI text-embedding-3-small dimension
                vector_search_profile_name="kpi-vector-profile"
            )
        ]
        
        # Define vector search configuration with default profile
        vector_search = VectorSearch(
            profiles=[
                VectorSearchProfile(
                    name="kpi-vector-profile",
                    algorithm_configuration_name="kpi-hnsw-config"
                )
            ],
            algorithms=[
                HnswAlgorithmConfiguration(
                    name="kpi-hnsw-config",
                    parameters=HnswParameters(
                        m=4,
                        ef_construction=400,
                        ef_search=500,
                        metric="cosine"
                    )
                )
            ]
        )
        
        # Create the index with vector search configuration
        index = SearchIndex(
            name=self.index_name,
            fields=fields,
            vector_search=vector_search
        )
        
        # Create the index
        self.index_client.create_index(index)
        print(f"Successfully created index: {self.index_name}")
    
    def _create_embedding(self, text: str) -> List[float]:
        """Create embedding for text using OpenAI"""
        embeddings_deployment = os.getenv("AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT", "text-embedding-3-small")
        response = self.openai_client.embeddings.create(
            input=text,
            model=embeddings_deployment
        )
        return response.data[0].embedding
    
    def _create_kpi_text(self, row: pd.Series) -> str:
        """Create a comprehensive text representation of the KPI row with every detail"""
        # Get all column values, preserving exact formatting and content
        metric_name = str(row['Metric Name']) if pd.notna(row['Metric Name']) else ""
        table_columns = str(row['Table : Columns']) if pd.notna(row['Table : Columns']) else ""
        sql_query = str(row['SQL Query']) if pd.notna(row['SQL Query']) else ""
        description = str(row['Description']) if pd.notna(row['Description']) else ""
        
        # Parse table and columns information for better searchability
        parsed_tables = self._parse_table_columns(table_columns)
        
        # Create comprehensive text with every detail preserved for maximum searchability
        text_parts = [
            f"METRIC NAME: {metric_name}",
            f"TABLE AND COLUMNS: {table_columns}",
            f"PARSED TABLE INFO: {parsed_tables}",
            f"SQL QUERY: {sql_query}",
            f"DESCRIPTION: {description}",
            f"RAW METRIC NAME: {metric_name}",
            f"RAW TABLE COLUMNS: {table_columns}",
            f"RAW SQL QUERY: {sql_query}",
            f"RAW DESCRIPTION: {description}",
            f"FULL KPI DETAILS: Metric Name: {metric_name} | Table Columns: {table_columns} | SQL Query: {sql_query} | Description: {description}",
            f"SEARCHABLE CONTENT: {metric_name} {table_columns} {sql_query} {description}",
            f"COMPLETE RECORD: {metric_name} | {table_columns} | {sql_query} | {description}"
        ]
        return "\n\n".join(text_parts)
    
    def _parse_table_columns(self, table_columns_str: str) -> str:
        """Parse table and columns string into a structured format"""
        if not table_columns_str or pd.isna(table_columns_str):
            return "No table information available"
        
        # Split by semicolon to get multiple tables
        tables = table_columns_str.split(';')
        parsed_tables = []
        
        for table in tables:
            table = table.strip()
            if ':' in table:
                # Split by colon to separate table name and columns
                parts = table.split(':', 1)
                if len(parts) == 2:
                    table_name = parts[0].strip()
                    columns = parts[1].strip()
                    
                    # Clean up column names (remove extra spaces, split by comma)
                    column_list = [col.strip() for col in columns.split(',')]
                    formatted_columns = ', '.join(column_list)
                    
                    parsed_tables.append(f"Table '{table_name}' with columns: {formatted_columns}")
                else:
                    parsed_tables.append(table)
            else:
                parsed_tables.append(table)
        
        return "; ".join(parsed_tables)
    
    def _create_metadata(self, row: pd.Series) -> Dict[str, Any]:
        """Create simple metadata for the KPI"""
        return {
            "metric_name": str(row['Metric Name']) if pd.notna(row['Metric Name']) else "",
            "table_columns": str(row['Table : Columns']) if pd.notna(row['Table : Columns']) else "",
            "sql_query": str(row['SQL Query']) if pd.notna(row['SQL Query']) else "",
            "description": str(row['Description']) if pd.notna(row['Description']) else ""
        }
    
    def process_csv_and_push(self, csv_path: str):
        """Read CSV file and push each row to Azure AI Search"""
        print(f"Reading CSV file: {csv_path}")
        
        # Read CSV file
        df = pd.read_csv(csv_path)
        print(f"Found {len(df)} KPI records")
        
        # Process each row and create documents
        documents = []
        
        for index, row in df.iterrows():
            try:
                # Create text representation
                kpi_text = self._create_kpi_text(row)
                
                # Create embedding
                embedding = self._create_embedding(kpi_text)
                
                # Create metadata
                metadata = self._create_metadata(row)
                
                # Create document for Azure AI Search - simple and clean
                document = {
                    "id": f"kpi_{index}",
                    "content": kpi_text,
                    "content_vector": embedding,
                    "metric_name": metadata["metric_name"],
                    "table_columns": metadata["table_columns"],
                    "sql_query": metadata["sql_query"],
                    "description": metadata["description"]
                }
                
                documents.append(document)
                print(f"Processed KPI {index + 1}/{len(df)}: {row['Metric Name']}")
                
            except Exception as e:
                print(f"Error processing row {index}: {e}")
                continue
        
        # Upload documents to Azure AI Search in batches
        if documents:
            print(f"Uploading {len(documents)} documents to Azure AI Search...")
            self._upload_documents_in_batches(documents)
            print("Successfully pushed all KPI documents to Azure AI Search!")
        else:
            print("No documents to upload")
    
    def _upload_documents_in_batches(self, documents: List[Dict[str, Any]], batch_size: int = 100):
        """Upload documents to Azure AI Search in batches"""
        for i in range(0, len(documents), batch_size):
            batch_docs = documents[i:i + batch_size]
            
            try:
                # Upload batch using the new syntax
                result = self.search_client.upload_documents(documents=batch_docs)
                print(f"Uploaded batch {i//batch_size + 1}/{(len(documents) + batch_size - 1)//batch_size}")
                print(f"Batch {i//batch_size + 1} uploaded successfully")
                    
            except Exception as e:
                print(f"Error uploading batch {i//batch_size + 1}: {e}")
                continue
    
    def get_index_stats(self):
        """Get statistics about the Azure AI Search index"""
        try:
            # Get index statistics
            stats = self.index_client.get_index_statistics(self.index_name)
            print(f"Azure AI Search Index Statistics:")
            
            # Support both dict and SDK object shapes
            if isinstance(stats, dict):
                doc_count = stats.get("documentCount") or stats.get("document_count")
                storage_size = stats.get("storageSize") or stats.get("storage_size")
            else:
                doc_count = getattr(stats, "document_count", None) or getattr(stats, "documentCount", None)
                storage_size = getattr(stats, "storage_size", None) or getattr(stats, "storageSize", None)
            
            print(f"Document count: {doc_count}")
            print(f"Storage size: {storage_size} bytes")
            print(f"Index name: {self.index_name}")
            return stats
        except Exception as e:
            print(f"Error getting index statistics: {e}")
            return None

def main():
    """Main function to run the KPI RAG pusher"""
    try:
        # Initialize pusher
        pusher = KPIRAGPusher()
        
        # Process CSV file - use correct relative path from RAG directory
        csv_path = "../Data/KPIs_HML_MVP.csv"
        pusher.process_csv_and_push(csv_path)
        
        # Get index statistics
        pusher.get_index_stats()
        
        print("KPI RAG push completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
