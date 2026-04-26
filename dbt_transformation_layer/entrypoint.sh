#!/bin/bash
# Fetches private key from Key Vault at container startup
# Writes to /tmp/rsa_key.p8 — never baked into image

python3 -c "
import os
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
client = SecretClient(vault_url=os.environ['AZURE_VAULT_URL'], credential=credential)
key = client.get_secret('dbt-private-key').value
with open('/tmp/rsa_key.p8', 'w') as f:
    f.write(key)
print('Private key fetched from Key Vault.')
"

# Run the dbt command passed to the container
exec "$@"