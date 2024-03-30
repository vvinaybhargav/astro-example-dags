FROM quay.io/astronomer/astro-runtime:10.6.0 
RUN apt-get update && apt-get install -y git  
RUN pip install --no-cache-dir --user apache-airflow-providers-git 
