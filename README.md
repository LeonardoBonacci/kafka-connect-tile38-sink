Welcome to yet another Kafka Connect connector!

# Useful resources

* https://github.com/jcustenborder/kafka-connect-archtype
* https://docs.confluent.io/3.2.0/connect/managing.html#common-rest-examples
* https://www.udemy.com/course/kafka-connect/
* https://github.com/lensesio/fast-data-dev/#enable-additional-connectors
* https://hackernoon.com/writing-your-own-sink-connector-for-your-kafka-stack-fa7a7bc201ea
* https://github.com/skynyrd/kafka-connect-elastic-sink
* https://tile38.com/
* https://github.com/lettuce-io/lettuce-core/wiki/Custom-commands,-outputs-and-command-mechanics
* https://tile38.com/topics/replication/

# Warning

The docker-compose file is made for a windows environment.
You'll need to change kafka's ADV_HOST variable on other operating systems, and possibly alter the connector's volume mount.

# Run

* First build the project using 'mvn clean package'
* Launch with 'docker-compose up -d kafka-cluster tile38'
* Wait a while, grab a coffee, and go to localhost:3030 and configure the connector with the settings supplied in Tile38SinkConnector.properties
* This creates the connector and the kafka topic I_AM_HERE
* Now run 'docker-compose up -d source'. This will pull a prepared docker image from the docker hub that generates test date. 
* At last: docker run --net=host -it tile38/tile38 tile38-cli
* Run 'scan trains'
* Nothing spectacular happening at first sight, now look better and see the coordinates changing while the (one) 'train moves' :)

# Not working

* When shit hits the fan, here's your savior: http://localhost:3030/logs/connect-distributed.log
* Please let me know what needs fixing..
