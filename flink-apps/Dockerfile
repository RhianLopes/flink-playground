FROM flink:1.20.3-scala_2.12

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip curl && \
    pip3 install --no-cache-dir "apache-flink==1.20.*" && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3 /usr/bin/python

RUN curl -fL "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar" \
    -o /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar

COPY apps/ /opt/flink/usrlib/
RUN chown -R flink:flink /opt/flink/usrlib

USER flink
WORKDIR /opt/flink
