### Cloud setup:

Within this project, Azure services were leveraged to built a production-like pipeline. Specifically, Azure Blob Storage and Key-vault were used to securely handle secrets and data storage. 

The following section provides a high-over description of the steps required to set-up the specific details within the configuration in order to run the pipeline. It will not be a guide on how to work with these particular services and therefore already assumes a basic level of understanding, such as how to create secrets and a Blob container.

#### Key-vault and handling sensitive information:

After creating a Key-vault and adding the required secrets, we can take the following components (subscription id, resource-group name and key-valt name) and create a service principal (think of this like a technical user identity). 

The output of the following code provides us with the tenant id, client id and secret.

```powershell
az ad sp create-for-rbac --name "airflow-local-dev" --role "Key Vault Secrets User" --scopes /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/<vault-name>
```
Place the components in the `example_env.txt` file so that the Python script can authenticate and fetch the secrets. Rename the file to `.env`.

Afterwards, create the necessary secrets using the following code:

```powershell
az keyvault secret set --vault-name <vault-name> --name "<secret-name>" --value "secret value"
```

If facing trouble creating secrets this way, you might need to add the role *Key Vault Secrets Officer* to the account you are accessing the CLI with to create secrets.


By setting this up, we not only handle sensitive information securely from the start of development, but it also closely mimics the production setting with no code changes necessary.

To run the project yourself, ensure that the following secrets are stored under the following name:

- *az-stor-access-key*
- *az-stor-account-name*
- *az-stor-container-name*
- *igdb-client-id*
- *igdb-client-secret*
- *snowflake-account*
- *snowflake-database*
- *snowflake-password*
- *snowflake-schema*
- *snowflake-username*
- *snowflake-warehouse*

#### Cloud Storage:

This project used Azure Blob Storage to store the data and the following steps give a high-over on how to set this up. The same steps would apply for GCS and S3 (GCP and AWS, respectively), but please refer to the Snowflake documentation for more details.

* Create a Storage Account under said resource group.

* Afterwards, create a Storage Container within this Storage Account.

* Find the Tenant ID, within Azure AD

* Fill in your Tenant ID and Container name in the code within `sql/stages/create_external_stage.sql` to create a storage integration.