FROM apache/airflow:3.0.6

# Copy requirements file
COPY requirements.txt /opt/airflow/requirements.txt

# Install additional packages for S3/MinIO integration
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

USER airflow
