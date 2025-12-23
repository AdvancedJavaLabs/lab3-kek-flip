# Download hadoop
FROM eclipse-temurin:17-jdk AS base

ENV HADOOP_VERSION=3.4.2
RUN wget -O /tmp/hadoop.tar.gz "https://dlcdn.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION-lean.tar.gz"


# Install dependencies
FROM base AS deps

RUN apt-get update && \
    apt-get install -y --no-install-recommends ssh pdsh sudo


# Install hadoop
FROM deps AS hadoop-installation

ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONFIG_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV HADOOP_USER=hadoop
ENV HADOOP_UID=1001
ENV HADOOP_GID=1001

RUN groupadd -g $HADOOP_GID $HADOOP_USER && \
    useradd -u $HADOOP_UID -g $HADOOP_GID -r -m -s /bin/bash $HADOOP_USER && \
    usermod -aG sudo $HADOOP_USER && \
    echo "$HADOOP_USER ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

RUN tar -xzf /tmp/hadoop.tar.gz -C /opt && \
    mv /opt/hadoop-$HADOOP_VERSION /opt/hadoop && \
    rm /tmp/hadoop.tar.gz


# Configure hadoop
FROM hadoop-installation AS hadoop-configuration

COPY hadoop/core-site.xml $HADOOP_CONFIG_DIR/core-site.xml
COPY hadoop/hdfs-site.xml $HADOOP_CONFIG_DIR/hdfs-site.xml


# Prepare runtime
FROM hadoop-configuration AS runtime

RUN echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
RUN echo "export HDFS_NAMENODE_USER=$HADOOP_USER" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
RUN echo "export HDFS_DATANODE_USER=$HADOOP_USER" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
RUN echo "export HDFS_HADOOP_USER=$HADOOP_USER" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

ENV ARTIFACT_NAME=sales-analyzer
ENV JAR_NAME=$ARTIFACT_NAME-1.0.jar
ENV BUILD_PATH=$ARTIFACT_NAME/build/libs
COPY $BUILD_PATH/$JAR_NAME .

COPY *.csv .

COPY hadoop/start.sh start.sh
RUN chmod +x start.sh

USER $HADOOP_USER

RUN ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 600 ~/.ssh/authorized_keys

ENTRYPOINT ["./start.sh"]
