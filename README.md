# Pyspark-tutorial

## How to use spark using python by Colab or Docker
| Pyspark을 사용하며 실습 환경은 Google Colab과 개인컴퓨터에서 설치하는 Spark standalone 모드의 Spark입니다.

### 2023.07 (Docker)
- docker를 이용한 spark 설치
  - root 경로에서 `docker compose up` 실행
    ```
    .
    ├── 0.Set-up
    ├── 1.Pyspark-tutorial
    ├── 2.RDD, Dataframe
    ├── 3.Spark_SQL
    ├── 4.Spark_ML
    ├── Dockerfile ## here
    ├── README.md
    ├── docker-compose.yml ## here
    ├── hadoop
    └── spark
    
    ```
  - 만약 필요한 jdbc 드라이버가 존재한다면 /usr/local/spark-3.3.1-bin-hadoop3/jars 경로에 넣어주면 됩니다. (Dockerfile에 작성 요망)  
    ```
    RUN cd /usr/local/spark-3.3.1-bin-hadoop3/jars && \
      if ! wget -P /usr/local/spark-3.3.1-bin-hadoop3/jars https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/2.1.0.14/redshift-jdbc42-2.1.0.14.jar; then \
          echo "Failed to download Redshift JDBC driver"; \
      fi
    ```


### 2023.03.01 (Colab)
- colab 내 java의 버젼 : 11.0.17 
  ```
  openjdk version "11.0.17" 2022-10-18
  OpenJDK Runtime Environment (build 11.0.17+8-post-Ubuntu-1ubuntu220.04)
  OpenJDK 64-Bit Server VM (build 11.0.17+8-post-Ubuntu-1ubuntu220.04, mixed mode,  sharing)
  ```
- spark 3.3.1이 위의 버젼과 호환이 가능해짐
- 다음과 같이 설정이 바뀜
```
!pip install pyspark==3.3.1 py4j==0.10.9.5 
!pip install -q findspark

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf()
conf.set("spark.app.name", "logistic-regression")
conf.set("spark.master", "local[*]")

# Singleton pattern
spark = SparkSession.builder\
        .config(conf=conf)\
        .getOrCreate()

# csv read to dataframe
df = spark.read.format("text").load("1800.csv")
```

### 2023 이전 

- Spark Version
  - spark-3.3.1
  - hadoop2
  - JAVA : openjdk 1.8.0_352

- Pyspark was run in colab and practiced.

- colab 내부에 아래의 코드를 통해 spark 실습 환경을 구성한 후 spark context 및 session을 구동시켜야합니다.
  - Within the colab, you must configure the spark practice environment using the code below before running the spark context and session.
  
- need to update-alternatives (Java 버젼 세팅 , Java Version Settings)
  - openjdk version "1.8.0_352"
    - (OpenJDK Runtime Environment (build 1.8.0_352-8u352-ga-1~20.04-b08))
    
```python

!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q https://downloads.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop2.tgz
!tar -xvf spark-3.3.1-bin-hadoop2.tgz
!pip install -q findspark
!pip install pyspark

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.3.1-bin-hadoop2"

# need to update-alternatives => openjdk version "1.8.0_352"
!update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java
!java -version

import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("spark3_test").master("local[*]").getOrCreate()

import pyspark

# sc object generation
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("myfriend")
sc = SparkContext.getOrCreate(conf=conf)


```


