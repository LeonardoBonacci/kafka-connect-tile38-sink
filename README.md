Welcome to yet another Kafka Connect connector: this time it's a Sink to Tile38 (https://tile38.com/)!

# Run

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

#TODO

* only works for Json and Avro
* tombstones/deletes
* batch insert
* ssl

