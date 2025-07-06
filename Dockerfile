# # Dockerfile

# FROM bitnami/spark:3.3.2

# USER root

# # Install required tools and Python packages
# RUN apt-get update && apt-get install -y python3-pip && \
#     pip3 install pyspark pandas

# # Copy your PySpark script
# COPY script.py /app/pyspark_script.py

# # Set working directory
# WORKDIR /app

# CMD ["spark-submit", "pyspark_script.py"]
