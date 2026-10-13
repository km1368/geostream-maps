FROM docker.arvancloud.ir/apache/airflow:3.3.1-python3.12

COPY requirements.txt /requirements.txt
# CPython 3.12 (not 3.14): pyiceberg/pyarrow publish prebuilt wheels for it, so
# no gcc/libc-headers build step is needed. On the python3.14 base, pyiceberg
# only ships a source distribution and compiling it requires build-essential.
RUN pip install --no-cache-dir --constraint \
    "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt" \
    --index-url https://mirror-pypi.runflare.com/simple \
    -r /requirements.txt