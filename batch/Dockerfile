FROM {{ service_base_image.image }}

COPY pipeline-docs.tar.gz /batch/
RUN cd /batch/docs && \
  tar xvf pipeline-docs.tar.gz && \
  rm -rf /batch/pipeline-docs.tar.gz

COPY batch/setup.py batch/MANIFEST.in /batch/
COPY batch/batch /batch/batch/
RUN pip3 install --no-cache-dir /batch && \
  rm -rf /batch

EXPOSE 5000
