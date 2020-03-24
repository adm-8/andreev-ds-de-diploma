# Выпускная работа Андреева Д.С. по курсу Data Engineer - Otus - 11-2019

## Постановка задачи:

![MainProcessSchema.png](https://raw.githubusercontent.com/adm-8/andreev-ds-de-diploma/master/images/MainProcessSchema.png) 

### Основную задачу можно разделить на части:

* Генерация данных, Обучение ML модели
* Отправка кредитной заявки в Kafka (Opportunity Input Topic)
* Чтение данных по кредитным заявкам из Kafka, применение модели из п.1 для получения предсказаний
* Отправка результатов в Kafka (Opportunity Output Topic)
* Складирование результатов в Parquet файл 

#### Вызов №1
* Чтение данных из Parquet-файла в Vertica
* Создание аналитической витрины в Vertica

#### Вызов №2
* Подключение к Vertica какой-нибудь системы отчетов, например Tableau или PowerBI
* Создание графического отчета на основе данных из Vertica

## Необходимые компоненты:























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
