# Выпускная работа Андреева Д.С. по курсу Data Engineer - Otus - 11-2019



## Подготовка Kafka на первом сервере с Ubuntu 16.04
* Устанавливаем JAVA: https://tecadmin.net/install-oracle-java-8-ubuntu-via-ppa/

* Устанавливаем Spark
```
mkdir spark

cd spark

sudo wget http://mirror.linux-ia64.org/apache/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz

tar -xzvf spark-2.4.5-bin-hadoop2.7.tgz

rm spark-2.4.5-bin-hadoop2.7.tgz

cd ~/

vim ~/.bashrc

cd ~/spark

export SPARK_HOME=~/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

source ~/.bashrc

sudo ~/spark/spark-2.4.5-bin-hadoop2.7/sbin/start-master.sh 

sudo ~/spark/spark-2.4.5-bin-hadoop2.7/bin/spark-submit ~/spark/test.py


```
* Устанавливаем Docker: https://docs.docker.com/install/linux/docker-ce/ubuntu/

* Устанавливаем Docker Compose: https://docs.docker.com/compose/install/

* Качаем и запускаем Confluent Platform  https://docs.confluent.io/current/quickstart/cos-docker-quickstart.html#
```
mkdir ~/git

cd git

git clone https://github.com/confluentinc/examples
cd examples
git checkout 5.4.1-post

cd cp-all-in-one-community/

sudo docker-compose up -d --build
```


```
# для запуска уже после установки
cd ~/git/examples/cp-all-in-one
sudo docker-compose up -d --build

```


* Заходим на http://104.198.248.19:9021 и Создаем топики 'opty-input-topic' и 'opty-predicted-topic':

* Создаем Connector для opty-input-topic :
```
{
  "name": "datagen-opty-input",
  "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "kafka.topic": "opty-input-topic",
  "max.interval": "100",
  "iterations": "1000000000",
  "quickstart": "opty-input-connector"
}
```
