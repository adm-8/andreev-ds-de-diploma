# Выпускная работа Андреева Д.С. по курсу Data Engineer

## Автоматизация принятия решения по кредитным заявкам 

## Описание задачи:
Представим себе ситуацию, что в одном из банков до сих пор не используют систему автоматического принятия первичного решения по кредитным заявкам (КЗ) , поступающих с сайта\мобилки\партнёров. При этом у банка накопилось достаточно исторических данных, чтобы на их основе посроить модель машинного обучения и автоматизировать процесс принятия первичного решения по заявкам. 

Кроме того, по возможности, хотелось бы для задач аналитики заявок перехать на колоночную БД Vertica и прикрутить к ней аналитическую систему типа Tableau или PowerBI.

При таких вводных общая схема прцоесса будет выглядить следующим образом:

![MainProcessSchema.png](https://raw.githubusercontent.com/adm-8/andreev-ds-de-diploma/master/images/MainProcessSchema.png) 

### Основную задачу можно разделить на части:

* Генерация исторических данных по КЗ, Обучение ML модели, сохранение её в файловой системе
* Отправка кредитной заявки в Kafka (Opportunity Input Topic)
* Чтение данных по кредитным заявкам из Kafka, применение модели из п.1 для получения предсказаний
* Отправка результатов в Kafka (Opportunity Output Topic)
* Складирование результатов в Parquet файл в файловую систему

#### Вызов №1
* Чтение данных из Parquet-файла в Vertica
* Создание аналитической витрины в Vertica

#### Вызов №2
* Подключение к Vertica какой-нибудь системы отчетов, например Tableau или PowerBI
* Создание графического отчета на основе данных из Vertica

#### Вызов №3 
* Вместо обычной фаловой системы прикрутить всё это добро, используя Hadoop

## Необходимые компоненты системы:
* Сервер на Linux 
* Самописный скрипт для генерации данных 
* Kafka
* Spark Streaming \ ML

* Vertica
* Tableau\PowerBI
* Hadoop

#

#

#

#

#

#

### Устанавливаем KAFKA: https://tecadmin.net/install-apache-kafka-ubuntu/
* Устанавливаем JAVA:
```
sudo apt update
sudo apt install default-jdk

```
* Качаем и устанавливаем KAFKA
```
wget http://www-us.apache.org/dist/kafka/2.4.0/kafka_2.13-2.4.0.tgz
tar xzf kafka_2.13-2.4.0.tgz
sudo mv kafka_2.13-2.4.0 /usr/local/kafka

```
* Настраиваем Systemd Unit Files
Создаем файл настрек для Zookeeper:
```
sudo touch /etc/systemd/system/zookeeper.service
```
И добавляем в него следующее:
```
[Unit]
Description=Apache Zookeeper server
Documentation=http://zookeeper.apache.org
Requires=network.target remote-fs.target
After=network.target remote-fs.target

[Service]
Type=simple
ExecStart=/usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties
ExecStop=/usr/local/kafka/bin/zookeeper-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target
```
Создаем файл настрек для Kafka
```
sudo touch /etc/systemd/system/kafka.service
```
И добавляем в него следующее:
```
[Unit]
Description=Apache Kafka Server
Documentation=http://kafka.apache.org/documentation.html
Requires=zookeeper.service

[Service]
Type=simple
Environment="JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64"
ExecStart=/usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties
ExecStop=/usr/local/kafka/bin/kafka-server-stop.sh

[Install]
WantedBy=multi-user.target
```
Применяем изменения:
```
sudo systemctl daemon-reload
```
* Запускаем Kafka Server
```
sudo systemctl start zookeeper
sudo systemctl start kafka
sudo systemctl status kafka

```
* Создаем топики:
```
cd /usr/local/kafka

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic OptyInputTopic

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic OptyOutputTopic

```
* Разрешаем внешние соединения, для этого правим файл : /usr/local/kafka/config/server.properties
```
listeners=PLAINTEXT://:9092
```
и указат айпишник самого сервака, в моем случае:
```
advertised.listeners=PLAINTEXT://34.71.139.131:9092

```
* Проверяем создаение сообений:
```
cd /usr/local/kafka
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic OptyInputTopic
```
* Проверяем чтение сообщений:
```
cd /usr/local/kafka
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic OptyInputTopic --from-beginning
```










## Решение
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
cd ~/examples/cp-all-in-one

sudo docker container stop $(sudo docker container ls -a -q -f "label=io.confluent.docker") && sudo docker system prune -a -f --volumes

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
