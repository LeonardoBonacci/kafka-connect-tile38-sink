package guru.bonacci.kafka.connect.tile38.writer;

import static guru.bonacci.kafka.connect.tile38.commands.CommandGenerator.from;
import static io.lettuce.core.codec.StringCodec.UTF8;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.errors.ConnectException;

import com.google.common.collect.Lists;

import guru.bonacci.kafka.connect.tile38.commands.CommandGenerators;
import guru.bonacci.kafka.connect.tile38.commands.CommandTemplates;
import guru.bonacci.kafka.connect.tile38.config.Tile38SinkConnectorConfig;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class Tile38Writer {

	private final RedisClient client;
	private final RedisAsyncCommands<String, String> async;
	private final CommandTemplates cmdTemplates;
	private final CommandGenerators cmds;
    

	public Tile38Writer(Tile38SinkConnectorConfig config) {
		this.cmdTemplates = config.getCmdTemplates();
    	this.client = RedisClient.create(String.format("redis://%s:%d", config.getHost(), config.getPort()));
		this.async = client.connect().async();
		// disable auto-flushing to allow for batch inserts
		this.async.setAutoFlushCommands(false);

		this.cmds = CommandGenerators.from(cmdTemplates.configuredTopics()
				.collect(toMap(identity(), topic -> from(cmdTemplates.commandForTopic(topic)))));
    }

    void writeForTopic(String topic, List<Tile38Record> events) {
    	events.forEach(event -> {
    		Pair<CommandType, CommandArgs<String, String>> cmd = cmds.by(topic).compile(event);
			
			try {
				List<RedisFuture<?>> futures = Lists.newArrayList();

				// due to a not-too-beautiful lettuce design 'set' and 'del' have different command outputs
				// since this will be re-written in batch and async mode we leave this if-else for now
				if (cmd.getLeft() == CommandType.SET) {
					futures.add(async.dispatch(cmd.getLeft(), new StatusOutput<>(UTF8), cmd.getRight()));
				} else {
					futures.add(async.dispatch(cmd.getLeft(), new IntegerOutput<>(UTF8), cmd.getRight()));
				}
				
				// write all commands to the transport layer
				async.flushCommands();
			
				// synchronization example: Wait until all futures complete
				boolean result = LettuceFutures.awaitAll(5, TimeUnit.SECONDS,
				                   futures.toArray(new RedisFuture[futures.size()]));

				log.info(futures.size() + " commands executed: " + result);
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
