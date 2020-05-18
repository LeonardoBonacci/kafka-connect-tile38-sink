package guru.bonacci.kafka.connect;

import java.util.Collection;

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
	private final CommandGenerator cmdGenerator;
    
    public Tile38Service(Tile38SinkConnectorConfig config) {
    	this.cmdTemplates = new CommandTemplates(config);
    	this.client = RedisClient.create(String.format("redis://%s:%d", config.getTile38Url(), config.getTile38Port()));
		this.sync = client.connect().sync();
		this.cmdGenerator = new CommandGenerator(cmdTemplates.commandForTopic("foo"));
    }

    public void write(Collection<InternalSinkRecord> events) {
    	events.forEach(event -> {
			CommandArgs<String, String> cmd = cmdGenerator.generate(event.getValue());
			String resp = sync.dispatch(CommandType.SET, new StatusOutput<>(StringCodec.UTF8), cmd);
			log.info("tile38 answers {}", resp);
		});	
    	
    }

    public void close() {
		client.shutdown();
	}
}
