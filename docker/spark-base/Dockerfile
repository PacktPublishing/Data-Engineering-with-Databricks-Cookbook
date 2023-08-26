FROM base

# -- Layer: Image Metadata
ARG build_date
ARG delta_package_version
ARG spark_xml_package_version

LABEL org.label-schema.build-date=${build_date}
LABEL org.label-schema.description="Data Engineering wih Apache Spark and Delta Lake Cookbook - Spark base image"
LABEL org.label-schema.schema-version="1.0"

# -- Layer: Apache Spark
ARG spark_version
ARG hadoop_version

RUN curl https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark.tgz && \
    tar -xf spark.tgz && \
    mv spark-${spark_version}-bin-hadoop${hadoop_version} /usr/bin/ && \
    echo "alias pyspark=/usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/bin/pyspark" >> ~/.bashrc && \
    echo "alias spark-shell=/usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/bin/spark-shell" >> ~/.bashrc && \
    mkdir /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/logs && \
    rm spark.tgz

ENV SPARK_HOME /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

# -- Runtime
WORKDIR ${SPARK_HOME}

USER root

ARG NBuser=NBuser
ARG GROUP=NBuser

RUN groupadd -r ${GROUP} && useradd -r -m -g ${GROUP} ${NBuser}

RUN chown -R "${NBuser}":"${GROUP}" /home/"${NBuser}"/ \
    && chown -R "${NBuser}":"${GROUP}" "${SPARK_HOME}"\
    && chown -R "${NBuser}":"${GROUP}" "${SHARED_WORKSPACE}"

USER ${NBuser}


RUN ${SPARK_HOME}/bin/spark-shell --packages io.delta:${delta_package_version} \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
 && ${SPARK_HOME}/bin/spark-shell --packages com.databricks:${spark_xml_package_version} \
 && ${SPARK_HOME}/bin/spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1