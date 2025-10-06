import os
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
# Document operations are now direct methods on SearchClient
from dotenv import load_dotenv
from pathlib import Path

def clear_azure_search_index(index_name: str):
    """
    Clear all documents from an Azure AI Search index given its name.
    
    Args:
        index_name (str): Name of the Azure AI Search index to clear
    """
    # Load environment variables
    load_dotenv()
    
    # Azure AI Search configuration
    service_endpoint = os.getenv("AZURE_SEARCH_SERVICE_ENDPOINT")
    api_key = os.getenv("AZURE_SEARCH_API_KEY")
    
    if not service_endpoint or not api_key:
        raise ValueError("Azure Search service endpoint and API key must be provided in environment variables")
    
    # Initialize Azure AI Search clients
    credential = AzureKeyCredential(api_key)
    search_client = SearchClient(
        endpoint=service_endpoint,
        index_name=index_name,
        credential=credential
    )
    index_client = SearchIndexClient(
        endpoint=service_endpoint,
        credential=credential
    )
    
    try:
        # Check if index exists
        existing_indexes = [idx.name for idx in index_client.list_indexes()]
        if index_name not in existing_indexes:
            print(f" Index '{index_name}' does not exist")
            return
        
        # Get all documents to delete them
        print(f" Retrieving all documents from index: {index_name}")
        
        # Search for all documents (using a wildcard search)
        results = search_client.search(search_text="*", select=["id"], top=10000)
        
        document_ids = [result["id"] for result in results]
        
        if not document_ids:
            print(f" Index '{index_name}' is already empty")
            return
        
        print(f" Found {len(document_ids)} documents to delete")
        
        # Delete documents in batches
        batch_size = 1000
        deleted_count = 0
        
        for i in range(0, len(document_ids), batch_size):
            batch_ids = document_ids[i:i + batch_size]
            
            # Create delete documents
            delete_docs = [{"id": doc_id} for doc_id in batch_ids]
            
            try:
                # Delete batch using the new syntax
                result = search_client.delete_documents(documents=delete_docs)
                deleted_count += len(batch_ids)
                print(f" Deleted batch {i//batch_size + 1}/{(len(document_ids) + batch_size - 1)//batch_size}")
                
            except Exception as e:
                print(f" Error deleting batch {i//batch_size + 1}: {e}")
                continue
        
        print(f" Successfully cleared {deleted_count} documents from index: {index_name}")
        print(f" Index '{index_name}' is now empty but still exists")
        
    except Exception as e:
        print(f" Error clearing index '{index_name}': {e}")
        raise

def list_azure_search_indexes():
    """List all available Azure AI Search indexes"""
    # Load environment variables
    load_dotenv()
    
    # Azure AI Search configuration
    service_endpoint = os.getenv("AZURE_SEARCH_SERVICE_ENDPOINT")
    api_key = os.getenv("AZURE_SEARCH_API_KEY")
    
    if not service_endpoint or not api_key:
        raise ValueError("Azure Search service endpoint and API key must be provided in environment variables")
    
    # Initialize Azure AI Search client
    credential = AzureKeyCredential(api_key)
    index_client = SearchIndexClient(
        endpoint=service_endpoint,
        credential=credential
    )
    
    try:
        indexes = list(index_client.list_indexes())
        print(" Available Azure AI Search indexes:")
        for idx in indexes:
            print(f"  - {idx.name}")
        return [idx.name for idx in indexes]
    except Exception as e:
        print(f" Error listing indexes: {e}")
        return []

def main():
    """Main function to clear a specific index"""
    print(" Available indexes:")
    available_indexes = list_azure_search_indexes()
    
    if not available_indexes:
        print(" No indexes found or error occurred")
        return
    
    print("\nEnter the Azure AI Search index name to clear:")
    index_name = input("Index name: ").strip()
    
    if not index_name:
        print(" No index name provided")
        return
    
    if index_name not in available_indexes:
        print(f" Index '{index_name}' not found in available indexes")
        return
    
    print(f" Clearing index: {index_name}")
    clear_azure_search_index(index_name)

if __name__ == "__main__":
    main() 