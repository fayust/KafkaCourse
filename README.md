Проект для изучения возможностей Kafka MQ. 

Проект написан на Kotlin c использованием системы сборки Gradle.

Используется следующий стек технологий:

zookeeper:3.8.0
Kafka 7.9.0-ccs от confluentinc
Kotlin 1.8.10
gradle 8.14
плагин checkstyle 8.45

Для сборки на Windows используйте команду 
./gradlew.bat build 

На Linux
./gradlew build

Перед запуском проекта необходимо запустить Kafka. Это можно сделать через docker-compose из корня проекта командой
docker-compose up -d

Кафка извне будет доступна по адресу 
localhost:29092

Внутри докер сети
kafka1:9092

С автором можно связаться  по почте baratrumus@gmail.com