import os
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from dotenv import load_dotenv
import json
from typing import Dict, Any

# Load environment variables
load_dotenv()

class AzureSearchIndexViewer:
    """Interactive script to view contents of Azure AI Search indexes"""

    def __init__(self):
        # Azure AI Search configuration
        self.service_endpoint = os.getenv("AZURE_SEARCH_SERVICE_ENDPOINT")
        self.api_key = os.getenv("AZURE_SEARCH_API_KEY")

        if not self.service_endpoint or not self.api_key:
            raise ValueError("Azure Search service endpoint and API key must be provided in environment variables")

        self.credential = AzureKeyCredential(self.api_key)

    def get_index_names(self) -> list:
        """Get all available index names"""
        from azure.search.documents.indexes import SearchIndexClient

        try:
            index_client = SearchIndexClient(
                endpoint=self.service_endpoint,
                credential=self.credential
            )

            indexes = index_client.list_indexes()
            return [index.name for index in indexes]

        except Exception as e:
            print(f"❌ Error retrieving index names: {str(e)}")
            return []

    def view_index_contents(self, index_name: str, top_k: int = 10):
        """View contents of a specific index"""
        try:
            search_client = SearchClient(
                endpoint=self.service_endpoint,
                index_name=index_name,
                credential=self.credential
            )

            print(f"\n🔍 [INDEX VIEWER] Viewing contents of index: '{index_name}'")
            print("=" * 60)

            # Get all documents from the index
            results = search_client.search(
                search_text="*",  # Search all documents
                top=top_k,
                include_total_count=True
            )

            # Print results count
            total_count = results.get_count()
            print(f"📊 Total documents in index: {total_count}")

            if total_count == 0:
                print("⚠️  Index is empty")
                return

            print(f"\n📋 Showing first {min(top_k, total_count)} documents:")
            print("-" * 60)

            # Print each document
            for i, result in enumerate(results, 1):
                print(f"\n📄 Document #{i}:")
                print(f"   ID: {result.get('id', 'N/A')}")

                # Print all fields except vectors
                for key, value in result.items():
                    if key not in ['@search.score', 'content_vector', 'vector_field'] and value is not None:
                        if isinstance(value, (list, dict)):
                            # Truncate long lists/dicts for readability
                            if len(str(value)) > 200:
                                print(f"   {key}: {str(value)[:200]}...")
                            else:
                                print(f"   {key}: {value}")
                        else:
                            print(f"   {key}: {value}")

                if i >= top_k:
                    if total_count > top_k:
                        print(f"\n... and {total_count - top_k} more documents")
                    break

            print("\n" + "=" * 60)

        except Exception as e:
            print(f"❌ Error viewing index '{index_name}': {str(e)}")

    def interactive_mode(self):
        """Interactive mode to let user select index and view contents"""
        print("🔍 Azure Search Index Viewer")
        print("=" * 50)

        # Get available indexes
        available_indexes = self.get_index_names()

        if not available_indexes:
            print("❌ No indexes found in Azure Search service")
            return

        print(f"\n📋 Available indexes ({len(available_indexes)}):")
        for i, index_name in enumerate(available_indexes, 1):
            print(f"   {i}. {index_name}")

        print("\n📝 Enter the name of the index you want to view (or 'q' to quit):")

        while True:
            index_name = input("\nIndex name: ").strip()

            if index_name.lower() == 'q':
                print("👋 Goodbye!")
                break

            if index_name in available_indexes:
                self.view_index_contents(index_name)
            else:
                print(f"❌ Index '{index_name}' not found.")
                print(f"Available indexes: {', '.join(available_indexes)}")

    def direct_mode(self, index_name: str, top_k: int = 10):
        """Direct mode to view specific index without interaction"""
        if not index_name:
            print("❌ Index name is required")
            return

        self.view_index_contents(index_name, top_k)

def main():
    """Main function"""
    viewer = AzureSearchIndexViewer()

    # Check if index name provided as command line argument
    import sys
    if len(sys.argv) > 1:
        index_name = sys.argv[1]
        top_k = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        viewer.direct_mode(index_name, top_k)
    else:
        # Interactive mode
        viewer.interactive_mode()

if __name__ == "__main__":
    main()
