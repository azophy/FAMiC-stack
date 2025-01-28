BELAJAR AIRFLOW
===============

personal experiment with Apache Airflow. based on:
- [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)

## Install
1. clone repo
2. setup airflow user:
  ```bash
  echo -e "AIRFLOW_UID=$(id -u)" > .env
  ```
3. docker compose up -d
