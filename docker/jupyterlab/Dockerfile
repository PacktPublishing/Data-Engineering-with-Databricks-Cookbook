FROM base

# -- Layer: Image Metadata

ARG build_date

LABEL org.label-schema.build-date=${build_date}
LABEL org.label-schema.name="Data Engineering wih Apache Spark and Delta Lake Cookbook - JupyterLab Image"
LABEL org.label-schema.description="JupyterLab image"


# -- Layer: Notebooks and data

# ADD docker/jupyterlab/kafka-producer.py /

# -- Layer: JupyterLab + Python kernel for PySpark

ARG spark_version
ARG jupyterlab_version
ARG sparksql_magic_version
ARG kafka_python_version

RUN pip3 install --no-cache-dir wget==3.2 \
    pyspark==${spark_version} \
    jupyterlab==${jupyterlab_version} \
    sparksql-magic==${sparksql_magic_version} \
    kafka-python==${kafka_python_version}

EXPOSE 8888

WORKDIR ${SHARED_WORKSPACE}
# COPY docker/jupyterlab/00-first.py /root/.ipython/profile_default/startup/00-first.py
CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=;python