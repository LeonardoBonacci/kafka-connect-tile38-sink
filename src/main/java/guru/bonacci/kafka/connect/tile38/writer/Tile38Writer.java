package guru.bonacci.kafka.connect.tile38.writer;

import static guru.bonacci.kafka.connect.tile38.commands.CommandGenerator.from;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static io.lettuce.core.codec.StringCodec.UTF8;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.errors.ConnectException;

import guru.bonacci.kafka.connect.tile38.commands.CommandGenerators;
import guru.bonacci.kafka.connect.tile38.commands.CommandTemplates;
import guru.bonacci.kafka.connect.tile38.config.Tile38SinkConnectorConfig;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.sync.RedisCommands;
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

	// DEL fleet truck1
    void writeForTopic(String topic, List<Tile38Record> events) {
    	events.forEach(event -> {
    		Pair<CommandType, CommandArgs<String, String>> cmd = cmds.by(topic).compile(event);
			
			try {
				String resp = sync.dispatch(cmd.getLeft(), new StatusOutput<>(UTF8), cmd.getRight());
				log.debug("tile38 answers {}", resp);
			} catch (RedisCommandExecutionException e) {
                log.warn("Exception {} while executing query: {}, with data: {}", e.getMessage(), cmd.getRight().toCommandString(), event.getValue());
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
