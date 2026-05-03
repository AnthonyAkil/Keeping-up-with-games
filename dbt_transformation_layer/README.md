### dbt profiles.yml configuration

Optional, but useful for local development and testing outside of the dbt Docker container, create an environment variable that points to the location of the *rsa_key.p8* file as such:

```powershell
[System.Environment]::SetEnvironmentVariable("DBT_PRIVATE_KEY_PATH", "C:\path\to\your\private_key.p8", "User")
```

Close the terminal/IDE and reopen it for the vatiable to be recognized. 

The `profiles.yml` file already references the environment variable that points to the `private_key.p8` filepath within the Docker container for *private_key_path* to utilize the environment variable. SO having this environment variable locally ensures so changes need to be made when switching from local development to the production environment within the container (with the current way of handling the private key reference).