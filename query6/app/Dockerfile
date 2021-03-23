FROM bde2020/spark-master:2.4.5-hadoop2.7

WORKDIR /spark-app
COPY . .
RUN pip install -r requirements.txt

CMD ["/spark/bin/spark-submit", "query6.py"]