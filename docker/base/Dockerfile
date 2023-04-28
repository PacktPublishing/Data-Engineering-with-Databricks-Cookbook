ARG java_image_tag=17-jre
FROM eclipse-temurin:${java_image_tag}

# -- Layer: Image Metadata

ARG build_date
ARG delta_spark_version
ARG deltalake_version
ARG pandas_version

LABEL org.label-schema.build-date=${build_date}
LABEL org.label-schema.description="Data Engineering wih Apache Spark and Delta Lake Cookbook  - Cluster base image"
LABEL org.label-schema.schema-version="1.0"

# -- Layer: OS + Python + Scala

ARG shared_workspace=/opt/workspace

RUN mkdir -p ${shared_workspace}/data && \
    mkdir -p /usr/share/man/man1  && \
    apt-get update -y && \
    apt-get install -y --no-install-recommends curl python3 r-base && \
    ln -s /usr/bin/python3 /usr/bin/python

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3-pip python3-dev && \
    pip3 install --upgrade pip

# We are explicitly pinning the versions of various libraries which this Docker image runs on.
RUN pip3 install --quiet --no-cache-dir \
    delta-spark==${delta_spark_version} \
    deltalake==${deltalake_version} \
    pandas==${pandas_version}

ENV SCALA_HOME="/usr/bin/scala"
ENV PATH=${PATH}:${SCALA_HOME}/bin
ENV SHARED_WORKSPACE=${shared_workspace}

# -- Runtime

VOLUME ${shared_workspace}
CMD ["bash"]
