# PySpark Tutorial

A comprehensive PySpark tutorial with hands-on examples covering RDD operations, DataFrames, Spark SQL, MLlib, and Streaming.

## 🚀 Quick Start with Docker (Recommended)

### Prerequisites
- Docker and Docker Compose installed
- At least 4GB RAM available for the container

### Launch Environment
```bash
# Clone the repository
git clone <repository-url>
cd pyspark-tutorial

# Start the Jupyter environment with Spark
docker compose up
```

### Access the Environment
- **Jupyter Notebook**: http://localhost:8888
- **Spark UI**: http://localhost:4040
- **Additional Port**: http://localhost:9999

The environment includes:
- **Spark 3.x** with Hadoop 3
- **OpenJDK 17**
- **Jupyter Notebook** with PySpark integration
- **Python libraries**: pandas, pyarrow, grpcio

### Project Structure
```
.
├── 0.set-up/                    # PySpark installation and setup
├── 1.pyspark-tutorial/          # Basic RDD operations, DataFrames, MLlib
├── 2.rdd-dataframe/             # Advanced RDD and DataFrame operations  
├── 3.spark-sql/                 # SQL operations, UDFs, joins, unit testing
├── 4.spark-ml-advanced/         # Performance optimization, partitioning, bucketing
├── 5.spark-streaming/           # Real-time data processing with Kafka
├── sample/                      # Sample data files for tutorials
├── kafka/                       # Kafka configuration for streaming examples
├── Dockerfile.spark             # Jupyter + Spark container configuration
├── docker-compose.yml           # Docker services configuration
└── README.md
```

## 📚 Learning Path

### 1. **Setup & Basics** (`0.set-up/`, `1.pyspark-tutorial/`)
- PySpark installation and configuration
- RDD operations and transformations
- Basic DataFrame operations
- MLlib examples (logistic regression, recommendations)

### 2. **Core Concepts** (`2.rdd-dataframe/`)
- Advanced RDD and DataFrame APIs
- Data loading and processing
- Schema management
- Data transformations

### 3. **Spark SQL** (`3.spark-sql/`)
- SQL operations and queries
- User-defined functions (UDFs)
- Joins and aggregations
- Unit testing with PySpark

### 4. **Advanced Topics** (`4.spark-ml-advanced/`)
- Performance optimization techniques
- Broadcast variables and joins
- Partitioning strategies (bucketing, custom partitioning)
- Schema evolution
- Scheduler configurations (FAIR vs FIFO)

### 5. **Streaming** (`5.spark-streaming/`)
- Structured Streaming fundamentals
- Kafka integration
- Real-time data processing
- Checkpoint management

## 🔧 Advanced Configuration

### Custom JDBC Drivers
Add JDBC drivers to the Spark classpath by modifying `Dockerfile.spark`:
```dockerfile
RUN cd /usr/local/spark/jars && \
    wget https://jdbc.postgresql.org/download/postgresql-42.7.0.jar
```

### Kafka Integration
For streaming examples, start Kafka using the provided configurations:
```bash
# Single broker setup
docker compose -f kafka/docker-compose-single.yml up

# Full Kafka cluster
docker compose -f kafka/docker-compose.yml up
```

### Testing
Run the unit tests included in the project:
```bash
# Navigate to the test directory in the container
cd 3.spark-sql/unittest/
python -m unittest test_df.py
```

## 🔄 Alternative: Google Colab Setup

If you prefer using Google Colab instead of Docker:

### Modern Colab Setup (2023+)
```python
# Install PySpark compatible with Colab's Java 11
!pip install pyspark==3.3.1 py4j==0.10.9.5 
!pip install -q findspark

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf()
conf.set("spark.app.name", "tutorial-session")
conf.set("spark.master", "local[*]")

spark = SparkSession.builder\
        .config(conf=conf)\
        .getOrCreate()
```

### Legacy Colab Setup (Pre-2023)
For older Colab environments requiring Java 8:

```python
# Install Java 8 and Spark
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q https://downloads.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop2.tgz
!tar -xvf spark-3.3.1-bin-hadoop2.tgz
!pip install -q findspark pyspark

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.3.1-bin-hadoop2"

!update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java

import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("spark3_test").master("local[*]").getOrCreate()
```

## 🎯 Key Features

- **Progressive Learning**: Structured tutorials from basic to advanced concepts
- **Hands-on Examples**: Real-world data processing scenarios
- **Performance Focus**: Optimization techniques and best practices
- **Modern Stack**: Latest Spark 3.x with contemporary Python libraries
- **Production Ready**: Docker containerization for consistent environments

## 🤝 Contributing

Feel free to contribute by adding more examples, improving documentation, or reporting issues.


