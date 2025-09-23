#!/usr/bin/env python3
"""
Comprehensive Azure OpenAI endpoint testing script
"""
import os
import requests
import sys
import socket
import time
from urllib.parse import urlparse
from dotenv import load_dotenv
from openai import AzureOpenAI

# Add parent directory to path to import from project
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables from parent directory
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

def test_network_connectivity(endpoint):
    """Test basic network connectivity and DNS resolution"""
    print(f"\n=== Network Connectivity Test ===")
    
    try:
        # Parse the endpoint URL
        parsed = urlparse(endpoint)
        hostname = parsed.hostname
        
        print(f"Testing DNS resolution for: {hostname}")
        
        # Test DNS resolution
        try:
            ip_address = socket.gethostbyname(hostname)
            print(f"✅ DNS resolved to: {ip_address}")
        except socket.gaierror as e:
            print(f"❌ DNS resolution failed: {e}")
            return False
        
        # Test basic connectivity
        print(f"Testing basic connectivity to {hostname}:443...")
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            result = sock.connect_ex((hostname, 443))
            sock.close()
            
            if result == 0:
                print("✅ Port 443 is reachable")
            else:
                print(f"❌ Port 443 is not reachable (error code: {result})")
                return False
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            return False
        
        # Test HTTPS connectivity
        print(f"Testing HTTPS connectivity...")
        try:
            response = requests.get(f"https://{hostname}", timeout=10, verify=True)
            print(f"✅ HTTPS connection successful (status: {response.status_code})")
        except requests.exceptions.SSLError as e:
            print(f"❌ SSL/TLS error: {e}")
            return False
        except requests.exceptions.ConnectionError as e:
            print(f"❌ Connection error: {e}")
            return False
        except requests.exceptions.Timeout as e:
            print(f"❌ Timeout error: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Network test failed: {e}")
        return False

def test_azure_specific_issues(endpoint, api_key):
    """Test for Azure-specific connectivity issues"""
    print(f"\n=== Azure-Specific Issues Test ===")
    
    # Test different Azure OpenAI endpoints
    azure_endpoints = [
        f"{endpoint}/openai/deployments",
        f"{endpoint}/openai/models",
        f"{endpoint}/openai/chat/completions",
    ]
    
    for test_endpoint in azure_endpoints:
        print(f"Testing: {test_endpoint}")
        try:
            response = requests.get(
                test_endpoint,
                headers={"api-key": api_key},
                timeout=10
            )
            print(f"  Status: {response.status_code}")
            
            if response.status_code == 200:
                print(f"  ✅ Endpoint accessible")
            elif response.status_code == 401:
                print(f"  ⚠️  Unauthorized (API key issue)")
            elif response.status_code == 404:
                print(f"  ❌ Not found (endpoint/resource issue)")
            elif response.status_code == 403:
                print(f"  ❌ Forbidden (permission/region issue)")
            else:
                print(f"  ⚠️  Unexpected status: {response.status_code}")
                
        except requests.exceptions.Timeout:
            print(f"  ❌ Timeout - possible network/firewall issue")
        except requests.exceptions.ConnectionError:
            print(f"  ❌ Connection error - possible network issue")
        except Exception as e:
            print(f"  ❌ Error: {e}")

def get_user_input():
    """Get endpoint and API key from user input"""
    print("=== Azure OpenAI Endpoint Tester ===")
    print("Enter your Azure OpenAI details:")
    print()
    
    endpoint = input("Enter endpoint URL: ").strip().rstrip('/')
    api_key = input("Enter API key: ").strip()
    
    return endpoint, api_key

def test_endpoint_connectivity(endpoint, api_key):
    """Test basic connectivity to the endpoint"""
    print(f"\n=== Testing Endpoint Connectivity ===")
    print(f"Endpoint: {endpoint}")
    print(f"API Key: {api_key[:10]}..." if api_key else "NOT SET")
    
    if not endpoint or not api_key:
        print("❌ Missing endpoint or API key")
        return False
    
    # Test basic connectivity
    try:
        # Test if endpoint is reachable
        response = requests.get(f"{endpoint}/openai/deployments", 
                              headers={"api-key": api_key}, 
                              timeout=10)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ Endpoint is reachable")
            deployments = response.json()
            print(f"Available deployments: {[d.get('id') for d in deployments.get('data', [])]}")
            return True
        elif response.status_code == 401:
            print("❌ Unauthorized - Check your API key")
            return False
        elif response.status_code == 404:
            print("❌ Not Found - Check your endpoint URL")
            return False
        else:
            print(f"❌ Unexpected status: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Connection error: {e}")
        return False

def test_specific_deployment(endpoint, api_key):
    """Test the specific deployment we want to use"""
    deployment = input(f"\nEnter deployment name to test (default: gpt-4o-mini): ").strip() or "gpt-4o-mini"
    api_version = input("Enter API version (default: 2024-07-18): ").strip() or "2024-07-18"
    
    print(f"\n=== Testing Specific Deployment ===")
    print(f"Deployment: {deployment}")
    print(f"API Version: {api_version}")
    
    try:
        client = AzureOpenAI(
            azure_endpoint=endpoint,
            api_key=api_key,
            api_version=api_version
        )
        
        # Test chat completion
        print("Testing chat completion...")
        response = client.chat.completions.create(
            model=deployment,
            messages=[{"role": "user", "content": "Say hello"}],
            max_tokens=10
        )
        
        print("✅ SUCCESS!")
        print(f"Response: {response.choices[0].message.content}")
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        print(f"Error type: {type(e).__name__}")
        
        # Specific error analysis
        error_str = str(e)
        if "404" in error_str:
            print("\n🔍 404 Analysis:")
            print("- The deployment name doesn't exist at this endpoint")
            print("- Check if you're using the right Azure OpenAI resource")
            print("- Verify the deployment name matches exactly (case-sensitive)")
        elif "401" in error_str:
            print("\n🔍 401 Analysis:")
            print("- Invalid API key")
            print("- API key doesn't match the endpoint")
        elif "403" in error_str:
            print("\n🔍 403 Analysis:")
            print("- API key doesn't have permission")
            print("- Check if the deployment is in the right region")
        
        return False

def test_embeddings(endpoint, api_key):
    """Test embeddings deployment"""
    embeddings_deployment = input(f"\nEnter embeddings deployment name (default: text-embedding-3-small): ").strip() or "text-embedding-3-small"
    api_version = input("Enter API version (default: 2024-07-18): ").strip() or "2024-07-18"
    
    print(f"\n=== Testing Embeddings Deployment ===")
    print(f"Embeddings Deployment: {embeddings_deployment}")
    
    try:
        client = AzureOpenAI(
            azure_endpoint=endpoint,
            api_key=api_key,
            api_version=api_version
        )
        
        # Test embeddings
        print("Testing embeddings...")
        response = client.embeddings.create(
            model=embeddings_deployment,
            input="test text"
        )
        
        print("✅ Embeddings SUCCESS!")
        print(f"Embedding dimensions: {len(response.data[0].embedding)}")
        return True
        
    except Exception as e:
        print(f"❌ Embeddings ERROR: {e}")
        return False

def suggest_fixes():
    """Suggest potential fixes based on common issues"""
    print(f"\n=== Suggested Fixes ===")
    print("1. Check Azure Portal for the correct endpoint:")
    print("   - Go to Azure Portal (not AI Foundry)")
    print("   - Find your Azure OpenAI resource")
    print("   - Go to 'Keys and Endpoint'")
    print("   - Copy the endpoint URL from there")
    print()
    print("2. Verify deployment exists:")
    print("   - In Azure Portal, go to your Azure OpenAI resource")
    print("   - Check 'Model deployments' section")
    print("   - Confirm 'gpt-4o-mini' deployment exists")
    print()
    print("3. Check API version compatibility:")
    print("   - For gpt-4o-mini, use: 2024-07-18 or later")
    print("   - For gpt-35-turbo, use: 2023-12-01-preview or later")
    print()
    print("4. Common endpoint formats:")
    print("   - Correct: https://your-resource.openai.azure.com")
    print("   - Wrong: https://your-resource.openai.azure.com/")
    print("   - Wrong: https://ai.azure.com/... (AI Foundry URL)")
    print()
    print("5. Alternative: Use AI Foundry 'Get endpoint' button:")
    print("   - In your AI Foundry screenshot, click 'Get endpoint'")
    print("   - Copy the endpoint and API key from there")

def main():
    print("Azure OpenAI Endpoint Diagnostic Tool")
    print("=" * 50)
    
    # Get user input
    endpoint, api_key = get_user_input()
    
    # Test network connectivity first
    if test_network_connectivity(endpoint):
        # Test Azure-specific issues
        test_azure_specific_issues(endpoint, api_key)
        
        # Test basic connectivity
        if test_endpoint_connectivity(endpoint, api_key):
            # Test specific deployment
            test_specific_deployment(endpoint, api_key)
            # Test embeddings
            test_embeddings(endpoint, api_key)
    else:
        print("\n❌ Network connectivity issues detected. Check:")
        print("- Internet connection")
        print("- Corporate firewall/proxy settings")
        print("- DNS resolution")
        print("- SSL/TLS configuration")
    
    # Always show suggestions
    suggest_fixes()

if __name__ == "__main__":
    main()
