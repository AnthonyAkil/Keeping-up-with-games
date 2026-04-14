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




### Setup

#### Cloud Storage:

This project used Azure Blob Storage to store the data and the following steps give a high-over on how to set this up. The same steps would apply for GCS and S3 (GCP and AWS, respectively), but please refer to the Snowflake documentation for more details.

* First, assuming already have an Azure account and a resource group, create a Storage Account under said resource group.

* Afterwards, create a Storage Container within this Storage Account.

* Find the Tenant ID, within Azure AD

* Fill in your Tenant ID and Container name in the code within `sql/stages/create_external_stage.sql` to create a storage integration.

* Follow the remaining steps within the `sql/stages/create_external_stage.sql` file to provide Snwoflake with the required access to the Storage Account.

* When this is enabled, we finally can create the external stage


#### dbt:

To create a Snowflake system user that dbt can utilize to authenticate and execute the transformations, we first need to use a key-pair authentication. I recommend using *openssl* for this, which is already included in Git Bash, or you can install it using PowerShell as follows:

```powershell
winget install -e --id ShiningLight.OpenSS
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
