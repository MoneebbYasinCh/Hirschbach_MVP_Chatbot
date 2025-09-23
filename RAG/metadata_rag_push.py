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

class MetadataRAGPusher:
    """Standalone script to push metadata to Azure AI Search vector database"""
    
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
        self.index_name = os.getenv("AZURE_SEARCH_INDEX_NAME_2", "metadata-hml-mvp")
        
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
        """Create Azure AI Search index with vector search capabilities for metadata"""
        # Define fields for the index - simple and clean
        fields = [
            SearchField(name="id", type=SearchFieldDataType.String, key=True),
            SearchField(name="content", type=SearchFieldDataType.String, searchable=True),
            SearchField(name="table_id", type=SearchFieldDataType.String, searchable=True),
            SearchField(name="schema_name", type=SearchFieldDataType.String, searchable=True),
            SearchField(name="table_description", type=SearchFieldDataType.String, searchable=True),
            SearchField(name="table_name", type=SearchFieldDataType.String, searchable=True),
            SearchField(name="column_name", type=SearchFieldDataType.String, searchable=True),
            SearchField(name="data_type", type=SearchFieldDataType.String, searchable=True),
            SearchField(name="primary_key", type=SearchFieldDataType.String, searchable=True),
            SearchField(name="foreign_key", type=SearchFieldDataType.String, searchable=True),
            SearchField(name="description", type=SearchFieldDataType.String, searchable=True),
            SearchField(name="distinct_count", type=SearchFieldDataType.String, searchable=True),
            SearchField(name="top_3_values", type=SearchFieldDataType.String, searchable=True),
            SearchField(name="min_value", type=SearchFieldDataType.String, searchable=True),
            SearchField(name="max_value", type=SearchFieldDataType.String, searchable=True),
            SearchField(name="has_negative_values", type=SearchFieldDataType.String, searchable=True),
            SearchField(
                name="content_vector",
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                vector_search_dimensions=1536,  # OpenAI text-embedding-3-small dimension
                vector_search_profile_name="metadata-vector-profile"
            )
        ]
        
        # Define vector search configuration with default profile
        vector_search = VectorSearch(
            profiles=[
                VectorSearchProfile(
                    name="metadata-vector-profile",
                    algorithm_configuration_name="metadata-hnsw-config"
                )
            ],
            algorithms=[
                HnswAlgorithmConfiguration(
                    name="metadata-hnsw-config",
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
    
    def _create_metadata_text(self, row: pd.Series) -> str:
        """Create a comprehensive text representation of the metadata row with every detail"""
        def clean_text_value(value):
            """Clean NaN values for text representation"""
            if pd.isna(value):
                return "Not specified"
            else:
                return str(value)
        
        # Get all column values, preserving exact formatting and content
        table_id = clean_text_value(row['TABLEID'])
        schema_name = clean_text_value(row['SCHEMANAME'])
        table_description = clean_text_value(row['TABLE_DESCRIPTION'])
        table_name = clean_text_value(row['TABLE_NAME'])
        column_name = clean_text_value(row['COLUMNNAME'])
        data_type = clean_text_value(row['DATATYPE'])
        primary_key = clean_text_value(row['PRIMARYKEY'])
        foreign_key = clean_text_value(row['FOREIGNKEY'])
        description = clean_text_value(row['DESCRIPTION'])
        distinct_count = clean_text_value(row['distinct_count'])
        top_3_values = clean_text_value(row['top_3_values'])
        min_value = clean_text_value(row['min_value'])
        max_value = clean_text_value(row['max_value'])
        has_negative_values = clean_text_value(row['has_negative_values'])
        
        # Create comprehensive text with every detail preserved for maximum searchability
        text_parts = [
            f"TABLE ID: {table_id}",
            f"SCHEMA NAME: {schema_name}",
            f"TABLE DESCRIPTION: {table_description}",
            f"TABLE NAME: {table_name}",
            f"COLUMN NAME: {column_name}",
            f"DATA TYPE: {data_type}",
            f"PRIMARY KEY: {primary_key}",
            f"FOREIGN KEY: {foreign_key}",
            f"DESCRIPTION: {description}",
            f"DISTINCT COUNT: {distinct_count}",
            f"TOP 3 VALUES: {top_3_values}",
            f"MIN VALUE: {min_value}",
            f"MAX VALUE: {max_value}",
            f"HAS NEGATIVE VALUES: {has_negative_values}",
            f"RAW TABLE ID: {table_id}",
            f"RAW SCHEMA NAME: {schema_name}",
            f"RAW TABLE DESCRIPTION: {table_description}",
            f"RAW TABLE NAME: {table_name}",
            f"RAW COLUMN NAME: {column_name}",
            f"RAW DATA TYPE: {data_type}",
            f"RAW PRIMARY KEY: {primary_key}",
            f"RAW FOREIGN KEY: {foreign_key}",
            f"RAW DESCRIPTION: {description}",
            f"RAW DISTINCT COUNT: {distinct_count}",
            f"RAW TOP 3 VALUES: {top_3_values}",
            f"RAW MIN VALUE: {min_value}",
            f"RAW MAX VALUE: {max_value}",
            f"RAW HAS NEGATIVE VALUES: {has_negative_values}",
            f"FULL METADATA DETAILS: Table ID: {table_id} | Schema: {schema_name} | Table Description: {table_description} | Table Name: {table_name} | Column Name: {column_name} | Data Type: {data_type} | Primary Key: {primary_key} | Foreign Key: {foreign_key} | Description: {description} | Distinct Count: {distinct_count} | Top 3 Values: {top_3_values} | Min Value: {min_value} | Max Value: {max_value} | Has Negative Values: {has_negative_values}",
            f"SEARCHABLE CONTENT: {table_id} {schema_name} {table_description} {table_name} {column_name} {data_type} {primary_key} {foreign_key} {description} {distinct_count} {top_3_values} {min_value} {max_value} {has_negative_values}",
            f"COMPLETE RECORD: {table_id} | {schema_name} | {table_description} | {table_name} | {column_name} | {data_type} | {primary_key} | {foreign_key} | {description} | {distinct_count} | {top_3_values} | {min_value} | {max_value} | {has_negative_values}"
        ]
        return "\n\n".join(text_parts)
    
    def _create_metadata_dict(self, row: pd.Series) -> Dict[str, Any]:
        """Create comprehensive metadata dictionary for the vector with every detail"""
        def clean_value(value):
            """Clean NaN values and convert to appropriate type"""
            if pd.isna(value):
                return ""  # Return empty string instead of None
            elif isinstance(value, (int, float)) and pd.isna(value):
                return ""  # Return empty string instead of None
            else:
                return str(value) if value is not None else ""
        
        # Get all values with exact column names from CSV
        table_id = clean_value(row['TABLEID'])
        schema_name = clean_value(row['SCHEMANAME'])
        table_description = clean_value(row['TABLE_DESCRIPTION'])
        table_name = clean_value(row['TABLE_NAME'])
        column_name = clean_value(row['COLUMNNAME'])
        data_type = clean_value(row['DATATYPE'])
        primary_key = clean_value(row['PRIMARYKEY'])
        foreign_key = clean_value(row['FOREIGNKEY'])
        description = clean_value(row['DESCRIPTION'])
        distinct_count = clean_value(row['distinct_count'])
        top_3_values = clean_value(row['top_3_values'])
        min_value = clean_value(row['min_value'])
        max_value = clean_value(row['max_value'])
        has_negative_values = clean_value(row['has_negative_values'])
        
        return {
            "table_id": table_id,
            "schema_name": schema_name,
            "table_description": table_description,
            "table_name": table_name,
            "column_name": column_name,
            "data_type": data_type,
            "primary_key": primary_key,
            "foreign_key": foreign_key,
            "description": description,
            "distinct_count": distinct_count,
            "top_3_values": top_3_values,
            "min_value": min_value,
            "max_value": max_value,
            "has_negative_values": has_negative_values
        }
    
    def process_csv_and_push(self, csv_path: str):
        """Read CSV file and push each row to Azure AI Search"""
        print(f"Reading CSV file: {csv_path}")
        
        # Read CSV file
        df = pd.read_csv(csv_path)
        print(f"Found {len(df)} metadata records")
        
        # Process each row and create documents
        documents = []
        
        for index, row in df.iterrows():
            try:
                # Create text representation
                metadata_text = self._create_metadata_text(row)
                
                # Create embedding
                embedding = self._create_embedding(metadata_text)
                
                # Create metadata
                metadata = self._create_metadata_dict(row)
                
                # Create document for Azure AI Search - simple and clean
                document = {
                    "id": f"metadata_{index}",
                    "content": metadata_text,
                    "content_vector": embedding,
                    "table_id": metadata["table_id"],
                    "schema_name": metadata["schema_name"],
                    "table_description": metadata["table_description"],
                    "table_name": metadata["table_name"],
                    "column_name": metadata["column_name"],
                    "data_type": metadata["data_type"],
                    "primary_key": metadata["primary_key"],
                    "foreign_key": metadata["foreign_key"],
                    "description": metadata["description"],
                    "distinct_count": metadata["distinct_count"],
                    "top_3_values": metadata["top_3_values"],
                    "min_value": metadata["min_value"],
                    "max_value": metadata["max_value"],
                    "has_negative_values": metadata["has_negative_values"]
                }
                
                documents.append(document)
                print(f"Processed metadata {index + 1}/{len(df)}: {row['COLUMNNAME']} from {row['TABLE_NAME']}")
                
            except Exception as e:
                print(f"Error processing row {index}: {e}")
                continue
        
        # Upload documents to Azure AI Search in batches
        if documents:
            print(f"Uploading {len(documents)} documents to Azure AI Search...")
            self._upload_documents_in_batches(documents)
            print("Successfully pushed all metadata documents to Azure AI Search!")
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
            print(f"Document count: {stats.document_count}")
            print(f"Storage size: {stats.storage_size} bytes")
            print(f"Index name: {self.index_name}")
            return stats
        except Exception as e:
            print(f"Error getting index statistics: {e}")
            return None

def main():
    """Main function to run the metadata RAG pusher"""
    try:
        # Initialize pusher
        pusher = MetadataRAGPusher()
        
        # Process CSV file - use correct relative path from RAG directory
        csv_path = "../Data/MetaData_HML_MVP.csv"
        pusher.process_csv_and_push(csv_path)
        
        # Get index statistics
        pusher.get_index_stats()
        
        print("Metadata RAG push completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
