package guru.bonacci.kafka.connect.tile38.writer;

import static guru.bonacci.kafka.connect.tile38.commands.CommandGenerator.from;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;

import guru.bonacci.kafka.connect.tile38.commands.CommandGenerators;
import guru.bonacci.kafka.connect.tile38.commands.CommandTemplates;
import guru.bonacci.kafka.connect.tile38.config.Tile38SinkConnectorConfig;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class Tile38Writer {

	private final RedisClient client;
	private final RedisCommands<String, String> sync;
	private final CommandTemplates cmdTemplates;
	private final CommandGenerators cmds;
    

	public Tile38Writer(Tile38SinkConnectorConfig config) {
		this.cmdTemplates = config.getCmdTemplates();
    	this.client = RedisClient.create(String.format("redis://%s:%d", config.getTile38Url(), config.getTile38Port()));
		this.sync = client.connect().sync();

		this.cmds = CommandGenerators.from(cmdTemplates.configuredTopics()
				.collect(toMap(identity(), topic -> from(cmdTemplates.commandForTopic(topic)))));
    }

    void writeForTopic(String topic, List<Tile38Record> events) {
    	events.forEach(event -> {
    		CommandArgs<String, String> cmd = cmds.by(topic).compile(event.getValue());
			
			try {
				String resp = sync.dispatch(CommandType.SET, new StatusOutput<>(StringCodec.UTF8), cmd);
				log.info("tile38 answers {}", resp);
			} catch (RedisCommandExecutionException e) {
                log.warn("Exception {} while executing query: {}, with data: {}", e.getMessage(), cmd.toCommandString(), event.getValue());
				throw new ConnectException(e);
			}
    	});	
    }

    public void writeData(Map<String, List<Tile38Record>> data) {
    	data.entrySet().forEach(d -> writeForTopic(d.getKey(), d.getValue()));
    }

    public void close() {
		client.shutdown();
	}
}
