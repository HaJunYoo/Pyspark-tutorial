# Base image
FROM jupyter/all-spark-notebook:spark-3.3.1

# Make jovyan a sudoer and set default password
USER root

ENV GRANT_SUDO=yes
RUN echo "jovyan ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
RUN echo "jovyan:jovyan" | chpasswd

# Update packages and install requirements
RUN apt-get update -y && \
    apt-get install -y wget && \
    apt-get install -y default-jdk scala && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# Download JDBC driver
RUN mkdir -p /usr/local/lib/python3.10/dist-packages/pyspark/jars/ && \
    wget -P /usr/local/lib/python3.10/dist-packages/pyspark/jars https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/2.1.0.14/redshift-jdbc42-2.1.0.14.jar

# Change ownership of the jars directory to jovyan
RUN chown -R jovyan:jovyan /usr/local/lib/python3.10/dist-packages/pyspark/jars

# Install Python packages
RUN pip install --no-cache-dir spylon-kernel py4j==0.10.9.5 && \
    python -m spylon_kernel install

# Switch back to jovyan to install Python packages
USER jovyan

# Enable Jupyter Lab
ENV JUPYTER_ENABLE_LAB=yes

# Ports
EXPOSE 8888 4040

# Working directory
WORKDIR /home/jovyan

# Volumes
VOLUME /home/jovyan

# Start Jupyter notebook
CMD ["start-notebook.sh"]

# docker build -t my_spark_image .
# docker run -p 8888:8888 -p 4040:4040 --name jupyter --restart always my_spark_image
