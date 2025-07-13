FROM bitnami/spark:3.3.2

USER root

RUN apt-get update && apt-get install -y python3 python3-pip

RUN pip3 install flask

# Set up environment
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$SPARK_HOME/bin:$PATH

WORKDIR /app

# Copy Python Flask server
COPY spark_submit_server.py /app/

EXPOSE 5000

CMD ["python3", "/app/spark_submit_server.py"]
