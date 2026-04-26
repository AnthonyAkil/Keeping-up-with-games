FROM ghcr.io/dbt-labs/dbt-snowflake:1.9.latest

WORKDIR /usr/app/dbt

COPY requirements-dbt.txt .

# Instal dependencies and disable pip-level cache
RUN pip install --no-cache-dir -r requirements-dbt.txt

# Copy only dbt dependency files (for dbt deps caching)
COPY dbt_project.yml packages.yml ./

RUN dbt deps --no-use-colors

# Copy the rest of the project (this changes most often)
COPY . .

# Explicitly putting entrypoint in different location
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]