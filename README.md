# Kafka Connect Tile38 Sink Connector

## Usage

Kafka Connect Tile 38 Sink is a Kafka Connector that translates record data into Redis SET and DEL commands that are executed against Tile38. Only sinking data is supported. [Check out Tile38](https://tile38.com/)! For streaming (back) into Kafka a [webhook](https://tile38.com/commands/sethook/) is available. This connector supports *at least once semantics*. 

### Record Formats and Structures
The following record formats are supported:

* Plain JSON
* JSON with Schema
* Avro 
* Protobuf

### Topics

Note that the Sink instance can be configured to monitor multiple topics. Just evaluate the property *topics* with a list of topic separated by comma. For example:
```
{
  "name": "tile38-sink",
  "config": {
    "tasks.max": "1",
    "connector.class": "guru.bonacci.kafka.connect.tile38.Tile38SinkConnector",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "topics": "fooTopic,barTopic",
    "tile38.topic.fooTopic": "<YOUR_COMMAND_HERE>",
    "tile38.topic.barTopic": "<YOUR_COMMAND_HERE>",
    "tile38.host": "tile38",
    "tile38.port": 9851
  }
}

```
Each topic configured within the *topics* property is required to have a corresponding command defined, following the pattern *tile38.topic.<topic name>*.

### Commands

Communication with Tile38 happens through the [Redis protocol](https://tile38.com/commands/). Based on each Kafka message that passes the Tile38 Sink Connector generates a [SET command](https://tile38.com/commands/set/). The command pattern is: 
```
SET key id [FIELD name value ...] [EX seconds] [NX|XX] (OBJECT geojson)|(POINT lat lon [z])|(BOUNDS minlat minlon maxlat maxlon)|(HASH geohash)|(STRING value)
```
This command can be made variable - or record specific - by using tokens of the format *event.my-field*, where *my-field* is the name of a Kafka field in the topic for which this command is specified. The value of Kafka record field *my-field* substitutes *event.my-field*. 

A few examples of syntactically correct commands:
```
fleet truck1 POINT 33.5123 -112.2693
fleet event.id POINT 33.5123 -112.2693
fleet event.id POINT event.lat event.lat
fleet event.id FIELD speed event.speed POINT event.latitude event.latitude
props event.identifier BOUNDS event.southwestlatitude, event.southwestlongitude, event.northeastlatitude, event.northeastlongitude 
```

Specified event fields that do not match any topic value field name result in invalid commands. This will cause runtime errors. A few hints: 
- Referring to nested fields is possible using the dot notation, as in *event.nest.my-field*
- Only value fields are permitted. 
- Using anything other than a SET command is not supported at this stage.

### Expire

[Expire](https://tile38.com/commands/expire/) functionality is available. The unit is seconds. How to use it:
```
tile38.topic.foo=SET foo event.id FIELD route event.route POINT event.lat event.lon
tile38.topic.foo.expire=5
```

### Tombstone messages

Tombstone messages are supported. They compile into [DEL commands](https://tile38.com/commands/del/). 

### Single Message Transformer on ID field
The ID field (of the SET command) is restrictive. If you're using a Kafka record field of which the string value may consist of multiple words - or, from another perspective, contains spaces - you can use the following single message transformer to remove white spaces before sinking to Tile38:

```
  "transforms": "RemoveSpaces",
  "transforms.RemoveSpaces.type":"guru.bonacci.kafka.connect.tile38.transforms.RemoveWhiteSpaces",
  "transforms.RemoveSpaces.field":"id",
  "transforms.RemoveSpaces.topic":"fooTopic"
```

## Configuration

### Connector Properties
Name |	Description	| Type	| Default |	Importance | Example
------------ | ------------- | ------------- | ------------- | ------------- | -------------
tile38.host	| Tile38 server host | string | localhost |	high | localhost 
tile38.port |	Tile38 server host port number | int | 9851 |	high | 9851
topics | Kafka topics read by the connector | comma-separated string | | high | foo,bar
flush.timeout.ms | Used for periodic flushing | int | 10000 | low | 1234
behavior.on.error | Error handling behavior | string | FAIL | medium | LOG or FAIL
tile38.password | Tile38's password | string | "" | low | foo123
tile38.topic.foo | Example command for 'foo' topic | string | | low | SET foo event.id FIELD route event.route POINT event.lat event.lon
tile38.topic.foo.expire | Expire time for 'foo' keyed id's | int | | low | 5 
tile38.topic.bar | Example command for 'bar' topic | string | | low | SET anything event.the_key POINT event.latitude event.longitude
***and*** | ***a*** | ***few*** | ***boring*** | ***connection*** | ***settings***
socket.tcp.no.delay.enabled | Use TCP-no-delay | boolean | false | low |
socket.keep.alive.enabled | Enable keepalive | boolean | false | low |
socket.connect.timeout.ms | Wait ms before socket timeout | long | 10000 | low |
request.queue.size | Max number of queued requests | int | 2147483647 | low |
auto.reconnect.enabled | Redis client automatic reconnect | boolean | true | low |

# Build and run info

* Build: 'mvn clean package'
* Launch 'docker-compose up -d'
* curl -X POST -H "Content-Type: application/json" --data @config/connector-source-stations.json http://localhost:8083/connectors | jq
* curl -X POST -H "Content-Type: application/json" --data @config/connector-source-trains.json http://localhost:8083/connectors | jq
* curl -X POST -H "Content-Type: application/json" --data @config/connector-sink.json http://localhost:8083/connectors | jq
* Kafka topic: 'stations' and 'trains'
* At last: docker run --net=host -it tile38/tile38 tile38-cli
* Run 'auth foo123'
* Then run 'scan station' and/or 'scan train'

* curl localhost:8083/connectors | jq
* curl -X DELETE -H "Content-type: application/json" http://localhost:8083/connectors/tile38-sink | jq

