# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Environment Setup

This repository provides a PySpark tutorial environment that can be run either in Google Colab or using Docker. The primary development environment uses Docker with Jupyter notebooks.

### Docker Development
- Start the development environment: `docker compose up`
- This launches a Jupyter notebook server with Spark support at http://localhost:8888
- Spark UI is available at http://localhost:4040
- The environment uses the official Jupyter Scipy notebook with Spark 3.x, Hadoop 3, and OpenJDK 17

### Kafka Integration
For streaming examples, set up Kafka using the provided docker-compose files:
- Single broker: `docker compose -f kafka/docker-compose-single.yml up`
- Full setup: `docker compose -f kafka/docker-compose.yml up`

## Testing

The repository includes unit tests for PySpark functions:
- Test runner: `python -m unittest test_df.py` (from `3.Spark_SQL/unittest/` directory)
- Tests use the standard unittest framework with SparkSession setup/teardown
- Test files are located in `3.Spark_SQL/unittest/`

## Project Structure

The repository is organized as a progressive tutorial with the following main sections:

1. **Setup (0.Set-up/)** - Initial PySpark installation and configuration
2. **Basic Tutorials (1.Pyspark-tutorial/)** - RDD operations, DataFrames, MLlib examples
3. **Core Concepts (2.RDD, Dataframe/)** - Deep dive into RDD and DataFrame APIs
4. **Spark SQL (3.Spark_SQL/)** - SQL operations, UDFs, joins, and unit testing
5. **Advanced ML (4.Spark_ML_advanced/)** - File systems, partitioning, schema evolution, bucketing
6. **Streaming (5.Spark_Streaming/)** - Real-time data processing with Kafka integration
7. **Advanced Topics (advanced_week1-3/)** - Performance tuning, optimization techniques

## Key Architecture Patterns

### Spark Session Configuration
Most examples use a standard SparkSession pattern:
```python
spark = SparkSession.builder \
    .appName("AppName") \
    .master("local[*]") \
    .getOrCreate()
```

### Performance Optimization Techniques
- **Broadcast Variables**: Used for small lookup tables (see `advanced_week1/broadcastVariable.py`)
- **Broadcast Joins**: Explicit broadcast hints for join optimization
- **Bucketing**: Pre-partitioning tables for efficient joins
- **Caching**: Strategic DataFrame caching for iterative algorithms
- **Partitioning**: Custom partitioning strategies for data skew handling

### Streaming Architecture
- Kafka integration for real-time data ingestion
- Structured Streaming with checkpointing
- Schema-aware stream processing
- Multiple output formats (console, files, databases)

## Common Development Tasks

### Running Spark Applications
- Standalone Python scripts: `python script_name.py`
- With Kafka dependencies: `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 script_name.py`
- Jupyter notebooks: Use the Docker environment for interactive development

### Data Formats
The project demonstrates various data formats:
- CSV files with headers
- JSON for streaming data
- Parquet for optimized storage
- Avro for schema evolution

### JDBC Integration
JDBC drivers can be added to `/usr/local/spark-3.3.1-bin-hadoop3/jars` in the Docker container. The Dockerfile includes examples for Redshift JDBC driver installation.

## Spark Configuration Notes

- Default shuffle partitions configured for local development (typically 3-4 partitions)
- Adaptive Query Execution (AQE) examples in advanced sections
- Custom scheduler configurations (FAIR vs FIFO) demonstrated
- Memory management examples including driver OOM scenarios