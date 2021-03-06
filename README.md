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

# Результат

![main_chema___result](https://github.com/adm-8/andreev-ds-de-diploma/blob/master/images/AndreevDS-DE-Diploma_result.png?raw=true)

**Отправка кредитной заявки в Kafka (Opportunity Input Topic):**

![kafka_producer___result](https://github.com/adm-8/andreev-ds-de-diploma/blob/master/images/kafka_producer___result.JPG?raw=true)

**Чтение данных по кредитным заявкам из Kafka, применение модели из п.1 для получения предсказаний, Отправка результатов в Kafka (Opportunity Output Topic):**

![kafka_producer___result](https://github.com/adm-8/andreev-ds-de-diploma/blob/master/images/kafka_consumer___result.JPG?raw=true)

**Джоин двух стримов и складирование результатов в Parquet файл в файловую систему:**

![kafka_producer___result](https://github.com/adm-8/andreev-ds-de-diploma/blob/master/images/kafka_consumer_join___result.JPG?raw=true)

**Чтение данных из таблицы через VSQL, в которую уже загнали данные из внешней таблицы на основе Parquet-файла:**

![vertica___result](https://github.com/adm-8/andreev-ds-de-diploma/blob/master/images/vertica___result.JPG?raw=true)

**Чтение данных из таблицы-аггрегата, топ10 год+месяц по положительным выдачам:**

![agg1___result](https://github.com/adm-8/andreev-ds-de-diploma/blob/master/images/OPTY_Y_M_AGG_top10___result.JPG?raw=true)

**Чтение данных из таблицы-аггрегата, общее кол-во заявок по региону и должности :**

![agg2___result](https://github.com/adm-8/andreev-ds-de-diploma/blob/master/images/OPTY_R_J_AGG___result.JPG?raw=true)

# Запуск основного процесса (делать только посе настроек ниже):

Стартуем необходиые службы:
```
sudo docker run -p 5433:5433 -d -v ~/andreev-ds-de-diploma/data/JoinedData:/tmp/data dataplatform/docker-vertica
sudo systemctl start zookeeper
sudo systemctl start kafka
sudo systemctl status kafka

```
Заходим в папку со скриптами:
```
cd ~/andreev-ds-de-diploma/python/
```
Запускаем генерацию файлов с данными:
```
sudo /var/spark/spark-2.4.5-bin-hadoop2.7/bin/spark-submit ~/andreev-ds-de-diploma/python/data_generator.py
```
Запскаем процесс обучения и сохранения модели:
```
sudo /var/spark/spark-2.4.5-bin-hadoop2.7/bin/spark-submit ~/andreev-ds-de-diploma/python/train_ml_model.py --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=python3
```
Запускаем процесс загрузки данных по КЗ в очередь кафки:
```
sudo /var/spark/spark-2.4.5-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 ~/andreev-ds-de-diploma/python/kafka_producer.py
```
Проверяем очередь OptyInputTopic (в новом окне SSH):
```
cd /usr/local/kafka && bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic OptyInputTopic
```
Запускаем процесс получения данных по КЗ из кафки и применения ML модели к ним (в новом окне SSH):
```
cd ~/andreev-ds-de-diploma/python/ && sudo /var/spark/spark-2.4.5-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 ~/andreev-ds-de-diploma/python/kafka_consumer.py
```
Проверяем очередь OptyOutputTopic (в новом окне SSH):
```
cd /usr/local/kafka && bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic OptyOutputTopic
```
Запускаем процесс объединения результатов прогнозирования и самой заявки:
```
cd ~/andreev-ds-de-diploma/python/ && sudo /var/spark/spark-2.4.5-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 ~/andreev-ds-de-diploma/python/kafka_consumer_join.py
```
Проверяем, что кол-во файлов увеличивается, следовательно результаты пишутся в Parquet-файлы:
```
cd ~/andreev-ds-de-diploma/data/JoinedData && ls | wc -l
```


## Прикручиваем Vertica (Вызов №1):
Идём в папку с проектом:
```
cd ~/andreev-ds-de-diploma/
```
Запускаем докер, подпихнув папку с данными:
```
sudo docker run -p 5433:5433 -d -v ~/andreev-ds-de-diploma/data/JoinedData:/tmp/data dataplatform/docker-vertica
```
#### После того как запустился докер с вертикой, необходимо выполнить скрипты:
```
cd ~/andreev-ds-de-diploma/sql/
```
Создание внешней таблицы, смотрящей на PARQUET файл, и внутренних таблиц:
```
~/vsql/opt/vertica/bin/vsql -h34.71.139.131 -Udbadmin -f "CREATE_TABLES.sql"
```
Перенос данных из внешней таблицы во внутреннюю:
```
~/vsql/opt/vertica/bin/vsql -h34.71.139.131 -Udbadmin -f "SP_MERGE_OPTY_FROM_EXT_TO_INT.sql"
```
Пересчёт аггрегатов:
```
~/vsql/opt/vertica/bin/vsql -h34.71.139.131 -Udbadmin -f "SP_CALC_AGG.sql"
```
Далее можем соединиться с базой и посмотреть, что у нас в итоге получилось. Соединение без пароля, указываем только имя пользователя dbadmin и IP адрес с вертикой, в моем случае получилось:
```
~/vsql/opt/vertica/bin/vsql -h34.71.139.131 -Udbadmin
```
Варинт для соединения с винды:
```
cmd /K chcp 65001
vsql -h34.71.139.131 -Udbadmin
```
#### посмотрим что в аггрегатах:
Получение топ10 год+месяц по кол-ву положительных одобрений:
```
SELECT * FROM DED.OPTY_Y_M_AGG
WHERE RES = 'POS'
ORDER BY CNT DESC
LIMIT 10;
```
Получение кол-ва результатов, сгруппированных по региону и занимаемой должности:
```
SELECT * FROM DED.OPTY_R_J_AGG;
```

#

#

# Клонирование проекта и настройка окружения

```
cd ~
sudo git clone https://github.com/adm-8/andreev-ds-de-diploma.git

cd ~/andreev-ds-de-diploma/
mkdir data

```
**Обновим питон до версии 3.7**:
https://phoenixnap.com/kb/how-to-install-python-3-ubuntu

Сделаем третий питон дефолтным:
```
sudo update-alternatives --install /usr/bin/python python /usr/bin/python3.7 1
sudo update-alternatives --install /usr/bin/python python /usr/bin/python2.7 2

sudo update-alternatives  --set python /usr/bin/python3.7
```

Установим необходимые пакеты:
```
sudo apt-get install python3-pip
sudo pip3 install --upgrade pip

sudo pip3 install numpy
sudo pip3 install pandas
sudo pip3 install sklearn
```

# Установка \ настройка необходимого ПО на Ubuntu

## Устанавливаем KAFKA: https://tecadmin.net/install-apache-kafka-ubuntu/
#### Устанавливаем JAVA:
```
sudo apt update
sudo apt install default-jdk

```
После установки JAVA необходимо создать переменные окружения, для этого в конец **/etc/environment** добавим:
```
JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
JRE_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre
``` 

#### Качаем и устанавливаем KAFKA
```
wget http://www-us.apache.org/dist/kafka/2.4.0/kafka_2.13-2.4.0.tgz
tar xzf kafka_2.13-2.4.0.tgz
sudo mv kafka_2.13-2.4.0 /usr/local/kafka

```
#### Настраиваем Systemd Unit Files
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
#### Разрешаем внешние соединения, для этого правим файл : /usr/local/kafka/config/server.properties
```
listeners=PLAINTEXT://:9092
```
и указат айпишник самого сервака, в моем случае:
```
advertised.listeners=PLAINTEXT://34.71.139.131:9092

```
Применяем изменения:
```
sudo systemctl daemon-reload
```
#### Запускаем Kafka Server
```
sudo systemctl start zookeeper
sudo systemctl start kafka
sudo systemctl status kafka

```
#### Создаем топики:
```
cd /usr/local/kafka

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic OptyInputTopic

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic OptyOutputTopic

```
#### Проверяем создаение сообений:
```
cd /usr/local/kafka
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic OptyInputTopic
```
#### Проверяем чтение сообщений:
```
cd /usr/local/kafka

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic OptyInputTopic --from-beginning

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic OptyOutputTopic --from-beginning
```

## Устанавливаем Spark
```
cd /var

mkdir spark

cd spark

sudo wget http://mirror.linux-ia64.org/apache/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz

tar -xzvf spark-2.4.5-bin-hadoop2.7.tgz

rm spark-2.4.5-bin-hadoop2.7.tgz

```
Далее необходимо добавить переменные среды в **/etc/environment** :
```
SPARK_HOME=/var/spark/spark-2.4.5-bin-hadoop2.7

PATH=$PATH:/var/spark/spark-2.4.5-bin-hadoop2.7/sbin:/var/spark/spark-2.4.5-bin-hadoop2.7/bin

```
Выставим треью версию питона по умолчанию, скопировав шаблон:
```
/var/spark/spark-2.4.5-bin-hadoop2.7/conf
cp spark-env.sh.template spark-env.sh

```
И добавим в него:
```
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
```
Запустим мастера:
```
sudo /var/spark/spark-2.4.5-bin-hadoop2.7/sbin/start-master.sh 
```

# Установка Vertica

## Устанавливаем Docker: 
https://docs.docker.com/install/linux/docker-ce/ubuntu/

## Устанавливаем Docker Compose: 
https://docs.docker.com/compose/install/

## Качаем докер с Vertica:
```
sudo docker pull dataplatform/docker-vertica
```
## Устанавливаем VSQL:
```
mkdir ~/vsql
cd ~/vsql
sudo wget https://www.vertica.com/client_drivers/9.3.x/9.3.1-0/vertica-client-9.3.1-0.x86_64.tar.gz
tar -xzvf vertica-client-9.3.1-0.x86_64.tar.gz
```

#

#

#

#

## Установка Airflow

Документация : https://airflow.apache.org/docs/stable/installation.html#initiating-airflow-database

#### Устанавливаем Airflow:
```
sudo python2.7 -m pip install apache-airflow
```

*Поскольку мы будем запускать Bash команды, а BashOperator поставляется из коробки, то никаких дополнительных пакетов для Airflow устанавливать не будем. Но не стоит забывать, что их есть и очень много. Подробнее об этом можно почитать в документации по ссылке выше.*

#### Настройка базы для Airflow:
Поскольку у нас этот проект больше похож на MVP, не будем заморачиваться с разворачиваением MySQL или PostgreSQL и сразу натравим Airflow на дефолтную SQLite:
```
airflow initdb
```

