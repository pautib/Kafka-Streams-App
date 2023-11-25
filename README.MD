Just to remember how to initialize Zookeper and the kafka cluster.
Note: Zookeper manages kafka clusters. It tells which cluster to use to store events on topics. Zookeper will be deprecated in April 2024. Kraft will be used on Apache Kafka instead

Go to C:\java\kafka>

Start Zookeeper service

Linux IGT machine: bin/zookeeper-server-start.sh config/zookeeper.properties
Local windows machine: bin\windows\zookeeper-server-start.bat config\zookeeper.properties


Start Kafka broker service (in a new terminal, if done on windows local machine)
Linux IGT machine: bin/kafka-server-start.sh config/server.properties
Local windows machine: bin\windows\kafka-server-start.bat config\server.properties


If errors are displayed review zookeeper.properties contain the correct dataDir path (C:/java/kafka/zookeeper-data)
