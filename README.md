# Big Data Pipeline

A comprehensive end-to-end big data processing pipeline built with Apache Spark and Python, designed for scalable data ingestion, processing, storage, and serving.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Components](#components)
- [Docker Deployment](#docker-deployment)
- [Testing](#testing)
- [Monitoring](#monitoring)
- [Contributing](#contributing)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## 🎯 Overview

This big data pipeline provides a robust framework for processing large datasets using Apache Spark. It supports multiple data sources, formats, and output destinations while maintaining high performance and scalability.

### Key Capabilities

- **Data Ingestion**: CSV, JSON, Parquet, and API data sources
- **Data Processing**: ETL transformations, aggregations, and data quality checks
- **Data Storage**: Multiple output formats (Parquet, CSV, JSON)
- **Data Serving**: REST API endpoints for data access
- **Monitoring**: Comprehensive logging and metrics collection

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Ingestion     │    │   Processing    │    │   Storage       │
│                 │    │                 │    │                 │    │                 │
│ • CSV Files     │───▶│ • File Reader   │───▶│ • Transformations│───▶│ • Parquet       │
│ • JSON APIs     │    │ • API Connector │    │ • Aggregations  │    │ • CSV           │
│ • Databases     │    │ • Batch/Stream  │    │ • Data Quality  │    │ • JSON          │
│ • Streaming     │    │ • Validation    │    │ • ML Features   │    │ • Data Lake     │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
                                              ┌─────────────────┐
                                              │   Serving       │
                                              │                 │
                                              │ • REST APIs     │
                                              │ • Reports       │
                                              │ • Dashboards    │
                                              │ • Exports       │
                                              └─────────────────┘
```

## ✨ Features

- **Modular Design**: Loosely coupled components for easy maintenance
- **Scalable Processing**: Leverages Spark's distributed computing capabilities
- **Multiple Data Sources**: Support for various input formats and sources
- **Configurable**: YAML-based configuration for easy customization
- **Docker Support**: Containerized deployment for consistency
- **Comprehensive Logging**: Detailed logging for monitoring and debugging
- **Testing Suite**: Unit and integration tests for reliability
- **SQL Support**: BigQuery, Presto, and SparkSQL examples

## 📋 Prerequisites

### Software Requirements

- **Python**: 3.8 or higher
- **Java**: OpenJDK 11 or higher (for Spark)
- **Docker**: For containerized deployment (optional)
- **Docker Compose**: For multi-container orchestration (optional)

### System Requirements

- **Memory**: Minimum 4GB RAM (8GB+ recommended)
- **Storage**: At least 10GB free space for data and logs
- **CPU**: 2+ cores recommended for parallel processing

## 🚀 Installation

### Option 1: Local Installation

1. **Clone the repository**

   ```bash
   git clone https://github.com/yourusername/big_data_pipeline.git
   cd big_data_pipeline
   ```

2. **Create virtual environment**

   ```bash
   python -m venv big_data_venv

   # On Windows
   big_data_venv\Scripts\activate

   # On macOS/Linux
   source big_data_venv/bin/activate
   ```

3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Set environment variables**

   ```bash
   # Windows
   set JAVA_HOME=C:\Program Files\Java\jdk-11
   set SPARK_HOME=C:\spark

   # macOS/Linux
   export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
   export SPARK_HOME=/opt/spark
   ```

### Option 2: Docker Installation

1. **Clone and build**

   ```bash
   git clone https://github.com/yourusername/big_data_pipeline.git
   cd big_data_pipeline
   docker-compose build
   ```

2. **Run the pipeline**
   ```bash
   docker-compose up
   ```

## ⚙️ Configuration

The pipeline is configured through `config/pipeline_config.yaml`:

```yaml
# Pipeline Configuration
spark:
  app_name: "BigDataPipeline"
  master: "local[*]"
  executor_memory: "2g"
  driver_memory: "1g"

data:
  raw_data_path: "data/raw/"
  processed_data_path: "data/processed/"
  output_data_path: "data/output/"

ingestion:
  batch_size: 1000
  file_format: "csv"

processing:
  partition_columns:
    - "date"
    - "category"
  cache_tables: true

serving:
  output_format: "parquet"
  api_port: 8080

logging:
  level: "DEBUG"
  file_path: "logs/pipeline.log"
```

### Configuration Options

| Section      | Parameter         | Description                       | Default         |
| ------------ | ----------------- | --------------------------------- | --------------- |
| `spark`      | `app_name`        | Spark application name            | BigDataPipeline |
| `spark`      | `master`          | Spark master URL                  | local[*]        |
| `spark`      | `executor_memory` | Memory per executor               | 2g              |
| `data`       | `raw_data_path`   | Input data directory              | data/raw/       |
| `ingestion`  | `batch_size`      | Records per batch                 | 1000            |
| `processing` | `cache_tables`    | Cache intermediate tables         | true            |
| `serving`    | `output_format`   | Output file format                | parquet         |
| `logging`    | `level`           | Log level (DEBUG/INFO/WARN/ERROR) | INFO            |

## 🎮 Usage

### Running the Complete Pipeline

```bash
# Local execution
python main.py

# Docker execution
docker-compose up
```

### Running Individual Components

```bash
# Data ingestion only
python -m src.ingestion.data_ingestion

# Data processing only
python -m src.processing.data_processor

# Data serving only
python -m src.serving.data_serving
```

### Using the API

Once the serving component is running:

```bash
# Get processed data
curl http://localhost:8080/api/data

# Get specific metrics
curl http://localhost:8080/api/metrics/customer

# Export data
curl http://localhost:8080/api/export/csv
```

## 📁 Project Structure

```
big_data_pipeline/
├── README.md                   # This file
├── requirements.txt            # Python dependencies
├── main.py                     # Main pipeline orchestrator
├── Dockerfile                  # Docker configuration
├── docker-compose.yml          # Docker Compose configuration
├── .gitignore                  # Git ignore patterns
│
├── config/                     # Configuration files
│   └── pipeline_config.yaml    # Main pipeline configuration
│
├── src/                        # Source code
│   ├── ingestion/              # Data ingestion modules
│   │   ├── __init__.py
│   │   ├── data_ingestion.py   # Main ingestion logic
│   │   └── api_connector.py    # API data connectors
│   │
│   ├── processing/             # Data processing modules
│   │   ├── __init__.py
│   │   ├── data_processor.py   # Main processing logic
│   │   └── transformations.py  # Data transformations
│   │
│   ├── serving/                # Data serving modules
│   │   ├── __init__.py
│   │   └── data_serving.py     # API endpoints and exports
│   │
│   ├── storage/                # Data storage modules
│   │   ├── __init__.py
│   │   └── data_storage.py     # Storage operations
│   │
│   └── utils/                  # Utility modules
│       ├── __init__.py
│       ├── logger.py           # Logging configuration
│       └── spark_session.py    # Spark session management
│
├── data/                       # Data directories
│   ├── raw/                    # Raw input data
│   ├── processed/              # Processed data
│   ├── output/                 # Final output data
│   └── test_output/            # Test results
│
├── sql/                        # SQL query examples
│   ├── sparksql_examples.sql   # Spark SQL queries
│   ├── bigquery_examples.sql   # BigQuery queries
│   └── presto_examples.sql     # Presto queries
│
├── logs/                       # Log files
│   └── pipeline.log            # Main pipeline log
│
├── tests/                      # Test files
│   └── test_pipeline.py        # Pipeline tests
│
└── artifacts/                  # Build artifacts
```

## 🔧 Components

### Data Ingestion (`src/ingestion/`)

- **File-based ingestion**: CSV, JSON, Parquet files
- **API ingestion**: REST API data sources
- **Data validation**: Schema validation and data quality checks
- **Batch processing**: Configurable batch sizes for large datasets

### Data Processing (`src/processing/`)

- **ETL operations**: Extract, Transform, Load operations
- **Data transformations**: Column mapping, data type conversions
- **Aggregations**: Group-by operations, statistical calculations
- **Data quality**: Null checks, duplicate removal, validation rules

### Data Storage (`src/storage/`)

- **Multiple formats**: Parquet, CSV, JSON output formats
- **Partitioning**: Date-based and category-based partitioning
- **Compression**: Configurable compression options
- **Data lake**: Organized storage structure

### Data Serving (`src/serving/`)

- **REST API**: HTTP endpoints for data access
- **Export functionality**: Data export in various formats
- **Report generation**: Automated business reports
- **Real-time queries**: Interactive data exploration

### Utilities (`src/utils/`)

- **Spark session management**: Centralized Spark configuration
- **Logging**: Structured logging with configurable levels
- **Configuration management**: YAML-based settings

## 🐳 Docker Deployment

### Building the Image

```bash
docker-compose build
```

### Running the Pipeline

```bash
# Run in foreground
docker-compose up

# Run in background
docker-compose up -d

# View logs
docker-compose logs -f spark_pipeline
```

### Stopping the Pipeline

```bash
docker-compose down
```

### Accessing Spark UI

- **Local**: http://localhost:8081
- **Container**: Access through exposed port 8081

## 🧪 Testing

### Running Tests

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run specific test file
pytest tests/test_pipeline.py -v
```

### Test Structure

- **Unit tests**: Individual component testing
- **Integration tests**: End-to-end pipeline testing
- **Data validation tests**: Data quality and schema validation

## 📊 Monitoring

### Logging

- **File logging**: Detailed logs in `logs/pipeline.log`
- **Console logging**: Real-time output during execution
- **Log levels**: DEBUG, INFO, WARN, ERROR
- **Structured logging**: JSON format for log analysis

### Metrics

- **Processing metrics**: Record counts, processing times
- **Performance metrics**: Memory usage, CPU utilization
- **Error tracking**: Exception logging and error rates
- **Data quality metrics**: Validation results, data completeness

### Spark UI

Access the Spark Web UI for detailed job monitoring:

- **Local**: http://localhost:4040 (when running locally)
- **Docker**: http://localhost:8081 (mapped port)

## 🤝 Contributing

1. **Fork the repository**
2. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Make your changes**
4. **Add tests** for new functionality
5. **Run tests** to ensure everything works
   ```bash
   pytest tests/
   ```
6. **Commit your changes**
   ```bash
   git commit -am 'Add new feature'
   ```
7. **Push to the branch**
   ```bash
   git push origin feature/your-feature-name
   ```
8. **Create a Pull Request**

### Code Style

- Follow PEP 8 for Python code style
- Use type hints where appropriate
- Add docstrings for all functions and classes
- Keep functions small and focused

## 🔍 Troubleshooting

### Common Issues

#### 1. **"Python worker failed to connect back"**

```bash
# Set correct Python path
export PYSPARK_PYTHON=/path/to/your/python
export PYSPARK_DRIVER_PYTHON=/path/to/your/python
```

#### 2. **Memory errors**

```yaml
# Increase memory in config/pipeline_config.yaml
spark:
  executor_memory: "4g"
  driver_memory: "2g"
```

#### 3. **Permission errors on Windows**

- Run terminal as Administrator
- Check firewall settings for Java/Python processes

#### 4. **Docker build fails**

```bash
# Clean build
docker-compose build --no-cache
```

#### 5. **Import errors**

```bash
# Ensure you're in the correct directory and virtual environment
cd big_data_pipeline
source big_data_venv/bin/activate  # or big_data_venv\Scripts\activate on Windows
```

### Log Analysis

Check `logs/pipeline.log` for detailed error information:

```bash
# View recent logs
tail -f logs/pipeline.log

# Search for errors
grep "ERROR" logs/pipeline.log
```

### Performance Tuning

- **Increase parallelism**: Adjust `spark.master` to `local[4]` for 4 cores
- **Optimize memory**: Balance `executor_memory` and `driver_memory`
- **Enable caching**: Set `cache_tables: true` for repeated data access
- **Partition data**: Use appropriate partition columns for large datasets

## 📈 Performance Considerations

### Spark Configuration

```yaml
spark:
  app_name: "BigDataPipeline"
  master: "local[*]" # Use all available cores
  executor_memory: "4g" # Increase for large datasets
  driver_memory: "2g" # Increase for complex operations
  sql.adaptive.enabled: true # Enable adaptive query execution
  sql.adaptive.coalescePartitions.enabled: true
```

### Data Optimization

- **Partitioning**: Partition by frequently queried columns
- **File formats**: Use Parquet for better compression and performance
- **Caching**: Cache frequently accessed datasets
- **Broadcasting**: Broadcast small lookup tables

## 🔐 Security Considerations

- **Data encryption**: Enable encryption for sensitive data
- **Access control**: Implement proper authentication and authorization
- **Network security**: Use secure connections for data transfer
- **Audit logging**: Log all data access and modifications

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📞 Support

For questions and support:

- **Issues**: Create an issue on GitHub
- **Documentation**: Check the project wiki
