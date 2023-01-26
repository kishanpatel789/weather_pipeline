
FROM apache/airflow:2.2.3 

ENV AIRFLOW_HOME=/opt/airflow 

USER root 
RUN apt-get update -qq && apt-get install vim -qqq && apt-get install unzip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ENV AWS_HOME=/home/aws

RUN TMP_DIR="$(mktemp -d)" \ 
 && curl -fL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "${TMP_DIR}/awscliv2.zip" \
 && unzip "${TMP_DIR}/awscliv2.zip" -d "${AWS_HOME}" \
 && sudo "${AWS_HOME}/aws/install" \
 && rm -rf "${TMP_DIR}" \
 && aws --version

# RUN mkdir "${AIRFLOW_HOME}/data/" \
#  && chown "default:default" "${AIRFLOW_HOME}/data/"

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID

