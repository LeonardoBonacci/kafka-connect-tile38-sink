package guru.bonacci.kafka.connect;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Tile38Service {

	private final RedisClient client;
	private final RedisCommands<String, String> sync;
	private final CommandTemplates cmdTemplates;
	private final Map<String, CommandGenerator> cmdGenerators;
    

	public Tile38Service(Tile38SinkConnectorConfig config) {
    	this.cmdTemplates = config.cmdTemplates;
    	this.client = RedisClient.create(String.format("redis://%s:%d", config.getTile38Url(), config.getTile38Port()));
		this.sync = client.connect().sync();

		//TODO read from config
		List<String> topics = Arrays.asList("foo", "bar");
		this.cmdGenerators = topics.stream()
				.collect(Collectors.toMap(Function.identity(), 
										topic -> new CommandGenerator(cmdTemplates.commandForTopic(topic))));
    }

    public void writeForTopic(String topic, List<InternalSinkRecord> events) {
    	events.forEach(event -> {
			CommandArgs<String, String> cmd = cmdGenerators.get(topic).compile(event.getValue());
			String resp = sync.dispatch(CommandType.SET, new StatusOutput<>(StringCodec.UTF8), cmd);
			log.info("tile38 answers {}", resp);
		});	
    }

    public void writeData(Map<String, List<InternalSinkRecord>> data) {
    	data.entrySet().forEach(d -> writeForTopic(d.getKey(), d.getValue()));
    }

    public void close() {
		client.shutdown();
	}
}
