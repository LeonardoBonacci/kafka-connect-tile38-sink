package guru.bonacci.kafka.connect;

import static guru.bonacci.kafka.connect.CommandGenerators.from;
import static guru.bonacci.kafka.connect.CommandGenerator.from;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.List;
import java.util.Map;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class Tile38Service {

	private final RedisClient client;
	private final RedisCommands<String, String> sync;
	private final CommandTemplates cmdTemplates;
	private final CommandGenerators cmds;
    

	Tile38Service(Tile38SinkConnectorConfig config) {
		this.cmdTemplates = config.cmdTemplates;
    	this.client = RedisClient.create(String.format("redis://%s:%d", config.getTile38Url(), config.getTile38Port()));
		this.sync = client.connect().sync();

		this.cmds = from(cmdTemplates.allTopics()
				.collect(toMap(identity(), topic -> from(cmdTemplates.commandForTopic(topic)))));
    }

    void writeForTopic(String topic, List<InternalSinkRecord> events) {
    	events.forEach(event -> {
			CommandArgs<String, String> cmd = cmds.by(topic).compile(event.getValue());
			String resp = sync.dispatch(CommandType.SET, new StatusOutput<>(StringCodec.UTF8), cmd);
			log.info("tile38 answers {}", resp);
		});	
    }

    void writeData(Map<String, List<InternalSinkRecord>> data) {
    	data.entrySet().forEach(d -> writeForTopic(d.getKey(), d.getValue()));
    }

    void close() {
		client.shutdown();
	}
}
