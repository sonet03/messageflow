# messageflow

## Run Producer
docker build -t kafka-producer -f ProducerApp/Dockerfile .
docker run --rm --network opc_kafka-net kafka-producer

## Run Consumers
docker build -t kafka-consumer -f ConsumerApp/Dockerfile .
docker run --rm --network opc_kafka-net kafka-producer


## Check published messages
docker exec -it kafka kafka-console-consumer.sh   --bootstrap-server kafka:9092   --topic kafka-topic-test   --from-beginning