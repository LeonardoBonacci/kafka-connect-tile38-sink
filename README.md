Welcome to yet another Kafka Connect connector!

# Run

* Build: 'mvn clean package'
* Launch 'docker-compose up -d'
* curl -X POST -H "Content-Type: application/json" --data @config/connector_source.json http://localhost:8083/connectors | jq
* curl -X POST -H "Content-Type: application/json" --data @config/connector_sink.json http://localhost:8083/connectors | jq
* Kafka topic: 'fleet'
* At last: docker run --net=host -it tile38/tile38 tile38-cli
* Run 'scan fleet'
