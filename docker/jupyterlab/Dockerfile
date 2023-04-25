FROM base

# -- Layer: Image Metadata

ARG build_date

LABEL org.label-schema.build-date=${build_date}
LABEL org.label-schema.name="Data Engineering wih Apache Spark and Delta Lake Cookbook - JupyterLab Image"
LABEL org.label-schema.description="JupyterLab image"


# -- Layer: Notebooks and data

# ADD workspace/ ${SHARED_WORKSPACE}/

# -- Layer: JupyterLab + Python kernel for PySpark

ARG spark_version
ARG jupyterlab_version

RUN pip3 install wget==3.2 \
    pyspark==${spark_version} \
    jupyterlab==${jupyterlab_version}

EXPOSE 8888

WORKDIR ${SHARED_WORKSPACE}
CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=
