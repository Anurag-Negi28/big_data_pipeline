FROM apache/spark-py:v3.3.0

# Switch to root user to install dependencies
USER root

# Install python3-venv to enable virtual environment creation
RUN apt-get update && apt-get install -y python3-venv && rm -rf /var/lib/apt/lists/*

# Create and activate virtual environment
RUN python3 -m venv /opt/spark-venv
ENV PATH="/opt/spark-venv/bin:$PATH"

# Install Python dependencies
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application code
COPY . /app

# Set environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=/opt/spark-venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/spark-venv/bin/python

# Ensure Spark runs as non-root user for security
#USER spark

# Command to run the pipeline
CMD ["python", "main.py"]
