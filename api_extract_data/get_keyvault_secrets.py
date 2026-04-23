"""
Azure Key Vault fetch script
Fetches the secrets from the project's key vault.
"""

# ============================================================================
# IMPORTS
# ============================================================================

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import HttpResponseError, ClientAuthenticationError
import logging

# ============================================================================
# Logging setup
# ============================================================================

logging.basicConfig(
    filename='app.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ============================================================================
# Function setup
# ============================================================================


def get_secret_client(vault_url: str) -> SecretClient:
    """
    Creates a single authenticated Key Vault client.
    Can be reused for each fetch.
    
    DefaultAzureCredential automatically uses:
    - Service Principal locally (reads AZURE_CLIENT_ID, AZURE_TENANT_ID,
      AZURE_CLIENT_SECRET from local .env)
    - Managed Identity in production
    """
    try:
        credential = DefaultAzureCredential()
        return SecretClient(vault_url=vault_url, credential=credential)
    except ClientAuthenticationError as e:
        logging.error(f"Failed to authenticate to Key Vault: {e}")
        raise

def get_secret(client: SecretClient, secret_name: str) -> str:
    """
    Fetches a single secret from Azure Key Vault by name.
    """
    try:
        return client.get_secret(secret_name).value
    except HttpResponseError as e:
        logging.error(f"Failed to fetch secret '{secret_name}': {e}")
        raise
