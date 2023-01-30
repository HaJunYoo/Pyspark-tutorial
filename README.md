# Pyspark-tutorial

### python을 이용한 spark 이용법 by Colab

- pyspark를 colab에서 구동하여 실습하였습니다. 

- colab 내부에 아래의 코드를 통해 spark 실습 환경을 구성한 후 spark context 및 session을 구동시켜야합니다.

- need to update-alternatives (Java 버젼 세팅)

```python

!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q https://downloads.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop2.tgz
!tar -xvf spark-3.3.1-bin-hadoop2.tgz
!pip install -q findspark
!pip install pyspark

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.3.1-bin-hadoop2"

# need to update-alternatives
!update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java

import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("spark3_test").master("local[*]").getOrCreate()

import pyspark

```
