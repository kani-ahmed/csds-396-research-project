# Use the official Apache Airflow image as the base image
FROM apache/airflow:latest-python3.12

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow

# Install virtualenv and required system packages
USER root
RUN apt-get update && \
    apt-get install -y virtualenv pkg-config libmariadb-dev git && \
    rm -rf /var/lib/apt/lists/*

# Create a directory for your virtual environment and set permissions
RUN mkdir -p /home/airflow/dbvenv && \
    chown -R airflow: /home/airflow/dbvenv

# Switch to the airflow user
USER airflow

# Copy requirements.txt into the container
COPY requirements.txt /home/airflow/dbvenv/requirements.txt

# Create and activate the virtual environment for database tasks, then install dependencies from requirements.txt
RUN python -m virtualenv /home/airflow/dbvenv && \
    /home/airflow/dbvenv/bin/pip install --upgrade pip && \
    /home/airflow/dbvenv/bin/pip install -r /home/airflow/dbvenv/requirements.txt

# Copy your Airflow configuration, scripts, and database module into the container
COPY dags /opt/airflow/dags
COPY plugins /opt/airflow/plugins
COPY models /opt/airflow/models
COPY database.py /opt/airflow/database.py
COPY extensions.py /opt/airflow/extensions.py

# Set the entrypoint to use the Airflow entrypoint
ENTRYPOINT ["/entrypoint"]

# Set default command to run webserver
CMD ["webserver"]

# Set the PYTHONPATH environment variable
ENV PYTHONPATH="/opt/airflow:/opt/airflow/models:/opt/airflow/plugins"
