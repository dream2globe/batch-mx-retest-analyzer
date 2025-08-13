# Use the official Apache Spark image as a base
FROM apache/spark:3.5.1

# Set environment variables
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3
ENV JAVA_HOME=/opt/java
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH

# Install Python and pip
USER root
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip3 install uv

# Create a non-root user
RUN useradd -ms /bin/bash spark
USER spark
WORKDIR /home/spark

# Copy the application files
COPY --chown=spark:spark . .
COPY --chown=spark:spark config.yml .

# Install Python dependencies using uv
RUN uv pip install --no-cache-dir -r requirements.txt

# Expose the Spark UI and driver ports
EXPOSE 4040 30011

# Set the entrypoint for the Spark application
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[*]", "main.py"]
