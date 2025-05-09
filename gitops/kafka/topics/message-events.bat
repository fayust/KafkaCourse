kafka-topics --create ^
  --bootstrap-server localhost:29092,localhost:29192,localhost:29292 ^
  --topic message-events ^
  --partitions 3 ^
  --replication-factor 3