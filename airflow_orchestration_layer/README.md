### Docker setup:
Assuming basic understanding and practical knowledge on Docker images and containers, one can open Docker desktop and run the following two commands to build the required images to build spin-up the containers:

```powershell
cd .\airflow_orchestration_layer\
docker compose build
```

Initializing the container can be done via:

```powershell
docker compose up
```

After the containers are succesfully running, the Airflow UI can be accessed via `http://localhost:8080/`. Note that the username and password for the UI are provided in the logs when Docker spins up the containers.


**Note** that you might need to login into the Github Container Registry (GHCR) to pull one of the images required within the Dockerfile. You can do that using the following code, after which you authenticatr your Github account:

```powershell
docker login ghcr.io
```
