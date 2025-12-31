# Use the official Airflow image as the base (best practice)
FROM apache/airflow:2.8.1-python3.10

USER root

# Copy requirements first (better layer caching)
COPY requirements.txt /opt/airflow/requirements.txt

USER airflow

# Add python deps here (keep it minimal)
# Postgres provider is already included in most Airflow images,
# but pinning explicitly is fine if you need it.
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt