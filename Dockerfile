FROM quay.io/astronomer/astro-runtime:10.6.0
RUN pip install --no-cache-dir --user apache-airflow-providers-github

