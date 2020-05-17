Welcome to yet another Kafka Connect connector!

# Run

* First build the project using 'mvn clean package'
* Launch with 'docker-compose up -d'
* curl -X POST -H "Content-Type: application/json" --data @config/connector_source.config http://localhost:8083/connectors | jq
* curl -X POST -H "Content-Type: application/json" --data @config/connector_sink.config http://localhost:8083/connectors | jq
* Kafka topic: 'fleet'
* At last: docker run --net=host -it tile38/tile38 tile38-cli
* Run 'scan fleet'
