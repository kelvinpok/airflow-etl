Implementation
========

Local machine:Window
1. run git clone {this repo}
2. astro dev start
3. Modify the ./spark/notebooks/pyspark/Dockerfile 
AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY as .env mentioned, such as ENV AWS_ACCESS_KEY_ID=xxxxxxx
4. run cd ./spark/notebooks/pyspark
5. run docker build . -t airflow/spark-app
6. access localhost:8080 in the browser for airflow UI with username and password as admin to login
7. run the dag
