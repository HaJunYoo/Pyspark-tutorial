# 기본 이미지 설정
# spark-3.3.1-bin-hadoop3/
# spark 3.3.1, scala 2.12 
FROM jupyter/all-spark-notebook:spark-3.3.1

# root 사용자로 전환
USER root

# jovyan 그룹 생성 후 해당 그룹에 jovyan 사용자 추가
RUN groupadd jovyan && usermod -a -G jovyan jovyan 

# jovyan을 sudo 사용자로 만들고 기본 비밀번호 설정
# sudo 권한 부여
# 비밀번호 설정
ENV GRANT_SUDO=yes
RUN echo "jovyan ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers 
RUN echo "jovyan:jovyan" | chpasswd 

# 패키지 업데이트 및 필요한 패키지 설치
RUN apt-get update -y && \
    apt-get install -y wget && \
    apt-get install -y default-jdk && \
    apt-get install netcat -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# Scala 2.12.14 및 sbt 설치
RUN wget https://downloads.lightbend.com/scala/2.12.14/scala-2.12.14.deb && \
    dpkg -i scala-2.12.14.deb && \
    apt-get update && \
    apt-get install -y scala && \
    wget https://github.com/sbt/sbt/releases/download/v1.5.5/sbt-1.5.5.tgz && \
    tar xzvf sbt-1.5.5.tgz && \
    mv sbt /usr/local && \
    echo "export PATH=/usr/local/sbt/bin:$PATH" >> /etc/bash.bashrc



# JDBC 드라이버 다운로드
RUN cd /usr/local/spark-3.3.1-bin-hadoop3/jars && \
    if [ ! -f redshift-jdbc42-2.1.0.14.jar ]; then \
        if ! wget -P /usr/local/spark-3.3.1-bin-hadoop3/jars https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/2.1.0.14/redshift-jdbc42-2.1.0.14.jar; then \
            echo "Failed to download Redshift JDBC driver"; \
        fi \
    else \
        echo "Redshift JDBC driver already exists"; \
    fi

# jars 디렉토리의 소유권을 jovyan에게 변경
RUN chown -R jovyan:jovyan /usr/local/spark-3.3.1-bin-hadoop3/jars

# Python 패키지 설치
RUN pip install --no-cache-dir spylon-kernel py4j==0.10.9.5 && \
    python -m spylon_kernel install

# Python 패키지 설치를 위해 jovyan으로 다시 전환
USER jovyan

# Jupyter Lab 활성화
ENV JUPYTER_ENABLE_LAB=yes

# 포트 설정
EXPOSE 8888 4040 9999

# 작업 디렉토리 설정
WORKDIR /home/jovyan

# 볼륨 설정
VOLUME /home/jovyan

# Jupyter notebook 시작
CMD ["start-notebook.sh"]