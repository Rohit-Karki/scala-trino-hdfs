FROM bitnami/spark:3.3.2

USER root

# Install Scala + sbt + basic build tools
RUN apt-get update && \
    apt-get install -y curl unzip default-jdk scala && \
    curl -L -o /sbt.zip https://github.com/sbt/sbt/releases/download/v1.8.2/sbt-1.8.2.zip && \
    unzip /sbt.zip -d /opt/ && \
    ln -s /opt/sbt/bin/sbt /usr/local/bin/sbt

WORKDIR /app

COPY ./scala-spark-project /app

RUN sbt compile

CMD ["sbt", "run"]
