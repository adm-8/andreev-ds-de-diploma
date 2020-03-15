# Выпускная работа Андреева Д.С. по курсу Data Engineer - Otus - 11-2019



## Подготовка Kafka на первом сервере с Ubuntu 16.04

* Устанавливаем Docker: https://docs.docker.com/install/linux/docker-ce/ubuntu/

* Устанавливаем Docker Compose: https://docs.docker.com/compose/install/

* Качаем и запускаем Confluent Platform (только Step1): https://docs.confluent.io/current/quickstart/ce-docker-quickstart.html
```
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
