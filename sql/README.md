### Snowflake setup:

Within this project, Snowflake was leveraged for it's datawarehouse capabilities.

The following section provides a high-over description of the steps required to create the Snowflake objects used within this project. Although it is straightforward, this section will not be a guide on how to set-up a [Snowflake trial account](https://signup.snowflake.com/) or how to navigate through the UI. Moreover, a basic understanding of SQL and how to execute it is required.

#### External stage setup:

After following the [Azure resources setup steps](IaC/README.md) and having created a Blob Container, follow the remaining steps within the `sql/stages/create_external_stage.sql` file, where you need to input your Azure Tenant ID, Storage Account name and Blob Container name, to provide Snowflake with the required access to the Storage Account and specified Containers.

#### dbt system user setup:

To create Snowflake system users that dbt can utilize to authenticate and utilize the data, we first need to set-up a key-pair authentication. I recommend using *openssl* for this, which is already included in Git Bash, or you can install it using PowerShell as follows:

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

* Copy-paste the respective public key content into the designated section within the `sql/users/dbt_system_user.sql` script and run it to create the role, user and grants needed for dbt to utilize the data within Snowflake.


#### Object creation:

* For one-time object creations execute the sql files in the `sql/database`, `sql/schemas`, `sql/tables` and `sql/users` folders, in the respective order. 