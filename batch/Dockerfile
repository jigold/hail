FROM {{ service_base_image.image }}

COPY batch/setup.py batch/MANIFEST.in /batch/
COPY batch/batch /batch/batch/
RUN pip3 install --no-cache-dir /batch && \
  rm -rf /batch
RUN pip3 install line_profiler
RUN pip3 install aiologger

EXPOSE 5000
