FROM apache/airflow:3.1.3

# Copy requirements.txt to the container
COPY requirements.txt /requirements.txt

# Install the additional packages
RUN pip install --no-cache-dir -r /requirements.txt