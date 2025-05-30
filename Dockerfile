FROM bitnami/spark:3.2.4

ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.2.4/spark-sql-kafka-0-10_2.12-3.2.4.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.0/kafka-clients-2.8.0.jar /opt/bitnami/spark/jars/

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

WORKDIR /app
