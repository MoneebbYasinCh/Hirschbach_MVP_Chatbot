# Testing Directory

This directory contains testing scripts for the Hirschbach AI Risk Intelligence Platform.

## Files

### test_endpoint.py
Comprehensive Azure OpenAI endpoint testing script that:
- Tests basic connectivity to your Azure OpenAI endpoint
- Verifies your deployment names and API versions
- Tests both chat completions and embeddings
- Provides detailed error analysis and suggestions

## Usage

Run from the project root directory:
```bash
python testing/test_endpoint.py
```

Or run from within the testing directory:
```bash
cd testing
python test_endpoint.py
```

## What it tests

1. **Endpoint Connectivity**: Verifies your Azure OpenAI endpoint is reachable
2. **Deployment Verification**: Checks if your specified deployments exist
3. **API Key Validation**: Tests if your API key is valid
4. **Chat Completions**: Tests the main LLM deployment
5. **Embeddings**: Tests the embeddings deployment

## Common Issues

- **404 Not Found**: Wrong endpoint URL (usually using AI Foundry URL instead of Azure OpenAI resource URL)
- **401 Unauthorized**: Invalid API key
- **403 Forbidden**: API key doesn't have permission or wrong region

## Getting the Correct Endpoint

1. Go to Azure Portal (portal.azure.com)
2. Search for "Azure OpenAI"
3. Find your Azure OpenAI resource
4. Go to "Keys and Endpoint"
5. Copy the endpoint URL from there

OR

1. In AI Foundry, click "Get endpoint" button next to any deployment
2. Copy the endpoint and API key from the popup
