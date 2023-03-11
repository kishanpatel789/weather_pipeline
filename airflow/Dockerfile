
FROM apache/airflow:2.2.3 

ENV AIRFLOW_HOME=/opt/airflow 

USER root 
RUN apt-get update -qq && apt-get install vim -qqq && apt-get install unzip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

# set up AWS cli
ENV AWS_HOME=/home/aws

RUN TMP_DIR="$(mktemp -d)" \ 
 && curl -fL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "${TMP_DIR}/awscliv2.zip" \
 && unzip "${TMP_DIR}/awscliv2.zip" -d "${AWS_HOME}" \
 && sudo "${AWS_HOME}/aws/install" \
 && rm -rf "${TMP_DIR}" \
 && aws --version

# set up spark
ENV SPARK_DIR=/home/spark 

ENV JAVA_HOME="${SPARK_DIR}/jdk-11.0.2"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

ENV SPARK_HOME="${SPARK_DIR}/spark-3.3.2-bin-hadoop3"
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN mkdir -p ${SPARK_DIR} \
 && cd "${SPARK_DIR}" \
 && curl -fLO https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz \
 && tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz \
 && rm openjdk-11.0.2_linux-x64_bin.tar.gz

RUN cd "${SPARK_DIR}" \
 && curl -fLO https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz \
#  && curl -fLO https://dlcdn.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz \
 && tar xzfv spark-3.3.2-bin-hadoop3.tgz \
 && rm spark-3.3.2-bin-hadoop3.tgz 

ENV PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"

# RUN mkdir "${AIRFLOW_HOME}/data/" \
#  && chown "default:default" "${AIRFLOW_HOME}/data/"

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID

