# Use the official Airflow image as the base
FROM apache/airflow:latest

# Set the AIRFLOW_HOME environment variable
ENV AIRFLOW_HOME=/usr/local/airflow

# Switch to the root user
USER root

# Create the AIRFLOW_HOME directory and change its ownership to the airflow user

RUN mkdir -p ${AIRFLOW_HOME} && chown -R airflow: ${AIRFLOW_HOME}
# RUN touch ${AIRFLOW_HOME}/airflow.cfg  
# RUN chmod 777 ${AIRFLOW_HOME}/airflow.cfg

# Switch back to the airflow user
USER airflow

# Initialize the Airflow database
RUN airflow db init

# Customize the airflow.cfg file
RUN echo "[core]" > ${AIRFLOW_HOME}/airflow.cfg && \
    echo "airflow_home = ${AIRFLOW_HOME}" >> ${AIRFLOW_HOME}/airflow.cfg && \
    echo "executor = LocalExecutor" >> ${AIRFLOW_HOME}/airflow.cfg && \
    echo "" >> ${AIRFLOW_HOME}/airflow.cfg && \
    echo "[webserver]" > ${AIRFLOW_HOME}/airflow.cfg && \
    echo "base_url = http://localhost:8080" >> ${AIRFLOW_HOME}/airflow.cfg && \
    echo "web_server_host = 0.0.0.0" >> ${AIRFLOW_HOME}/airflow.cfg && \
    echo "web_server_port = 8080" >> ${AIRFLOW_HOME}/airflow.cfg



    # docker exec -it 11c078b17dca  airflow users create --username admin --password admin --firstname First --lastname Last --role Admin --email admin@example.com