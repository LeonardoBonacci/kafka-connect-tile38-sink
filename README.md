# Kafka Connect Tile38 Sink Connector

## Usage

Kafka Connect Tile 38 Sink is a Kafka Connector that translates record data into SET and DELETE queries that are performed against Tile38. Only sinking data is supported. [Check out Tile38!](https://tile38.com/)

### Record Formats and Structures
The following record formats are supported:

* Avro 
* JSON with Schema
* Plain JSON

### Topics

Write something about the topic-command configuration.... Each configured Kafka Connect Tile38 Connector will only output data into a single database instance.

### Tombstone messages

Write something about tombstone messages....

## Configuration

### Connector Properties
Name |	Description	| Type	| Default |	Importance | Example
------------ | ------------- | ------------- | ------------- | ------------- | -------------
tile38.host	| Tile38 server host. | string | localhost |	high | localhost 
tile38.port |	Tile38 server host port number. | int | 9851 |	high | 9851
topics | Kafka topics read by the connector | comma-separated string | | high | foo,bar
flush.timeout.ms | Used for periodic flushing | int | 10000 | low | 1234
behavior.on.error | Error handling behavior | string | medium | 'LOG' or 'FAIL'.
tile38.topic.foo | Example command for 'foo' topic | string | | low | foo event.id FIELD route event.route POINT event.lat event.lon
tile38.topic.bar | Example command for 'bar' topic | string | | low | anything event.the_key POINT event.latitude event.longitude

# Build and run info

* Build: 'mvn clean package'
* Launch 'docker-compose up -d'
* curl -X POST -H "Content-Type: application/json" --data @config/connector-source-foo.json http://localhost:8083/connectors | jq
* curl -X POST -H "Content-Type: application/json" --data @config/connector-source-bar.json http://localhost:8083/connectors | jq
* curl -X POST -H "Content-Type: application/json" --data @config/connector-sink.json http://localhost:8083/connectors | jq
* Kafka topic: 'foo' and 'bar'
* At last: docker run --net=host -it tile38/tile38 tile38-cli
* Run 'scan foo' and 'scan bar'

* curl localhost:8083/connectors | jq
* curl -X DELETE -H "Content-type: application/json" http://localhost:8083/connectors/tile | jq

# TODO
* batch insert
* ssl

