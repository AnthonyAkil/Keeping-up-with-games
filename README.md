# Keeping up with games

Using data to stay up-to-date on video games!

## Business case

I haven't played video games for a while and I am out of touch with what games are considered cool and which upcoming releases are highly anticipated. Let's resolve this using the Internet Game DataBase (IGDB) data from Twitch!

To do so, the end goal should be to:
* Find insights into **what games are currently popular**
* Have an overview of upcoming releases

Explicit tasks:
* Build a datamodel that can be leveraged for BI.
* Create a dashboard that provides insights that higlights features of successful games.


## Project methodology and reasoning

### Architecture
![Architecture](diagrams\Architecture.png)

#### Tech Stack Rationale

* An ELT structure was adopted to preserve source data integrity, while providing a clear and logical progression through the Bronze, Silver, and Gold layers.
* Python, Snowflake, and Power BI were selected as the core stack based on existing 
proficiency, maximising both delivery speed and output quality.
* dbt was used to handle the transformation layer within the ELT pipeline.
* Utilizing Airflow to orchestrate the whole pipeline.
* Source data is coming from the IGDB API, referencing multiple of it's endpoints.
* Key-pair authentication is configured for the Snowflake service user to ensure secure access.




## Getting started

### IGDB API setup:

In order to get the API Client ID and Secret, follow the steps outlined within the [documentation](https://api-docs.igdb.com/#account-creation).


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



### Snowflake setup:

Within this project, Snowflake was leveraged for it's datawarehouse capabilities.

The following section provides a high-over description of the steps required to create the Snowflake objects used within this project. It will not be a guide on how to set-up a Snowflake account or how to navigate through the UI. Moreover, a basic level of understanding of SQL is required.

* Continuing from the last step in the *Cloud Storage* setup, follow the remaining steps within the `sql/stages/create_external_stage.sql` file to provide Snwoflake with the required access to the Storage Account. This creates the external stage.

* For one-time object creations execute the sql files in the `sql/database`, `sql/schemas`, `sql/tables` and `sql/users` folders, in that order. 

### dbt setup:

To create a Snowflake system user that dbt can utilize to authenticate and execute the transformations, we first need to use a key-pair authentication. I recommend using *openssl* for this, which is already included in Git Bash, or you can install it using PowerShell as follows:

```powershell
winget install -e --id ShiningLight.OpenSSL
```

* First, generate a private, unencrypted key:

```powershell 
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
```

* Now generate a public key by referencing the private key:

```powershell
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

* Copy-paste the public key into the designated section within the `sql/users/dbt_system_user.sql` script and run it to create the role, user and grants needed for dbt to operate within Snowflake. 
<MIGHT NEED TO SAY MORE ABOUT WHY WE SPECIFY CERTAIN SCHEMAS ETC'>


If we want to initialize dbt we simply run 

```powershell
dbt init
```

and provide the requested information such as user, role and private key. Move the wkdir to the dbt project folder and test the connection by running 

```powershell
dbt debug
```

**Note:** the `profiles.yml` will be generated outside of the current wkdir and for the sake of file transparency I have moved that within the dbt project folder.


### Docker setup:
Assuming basic understanding and practical knowledge on containers and Docker, one can change the working directory to the `airflow_orchestration_layer` and run the following two commands to build and run the airflow docker image and container, respectively:

```powershell
docker compose build
```

```powershell
docker compose up
```

After the containers are succesfully running, the Airflow UI can be accessed via `localhost:8080` in the browser. Note that the username and password are provided when docker spins up the containers.

* GCHR login using Github PAT to pull dbt image

```powershell
docker login ghcr.io
```


### What went wrong and learnings

#### Optimizing too early:
I really like incrementally loading my fact tables and wanted to implement this as well for the `IGDB.SILVER.CLEAN_APP` table and related bridge tables. However, I noticed that the run time on dbt grew quite significantly and that's when it hit me: the dataset for this project is too small to truly benefit from the optimization that incremental load brings.

Since the *incremental_strategy='append'* could very easily append duplicate records with the current setup and although *incremental_strategy='merge'* resolves this issue, it would have to make a full source/destination table scan with minor to no upside to show for it.

Instead of optimizing too early by sacrificing transformation efficiency for a *'scalable'* load method, I made the concious choice of staying flexible in how we want to handle reducing transformation time in the future. The benefic is that the transformation time is **reduced to a third** of the incremental strategy.

### To include in future iterations:

* dbt: package-lock.yml file caused issues when building the dbt image 