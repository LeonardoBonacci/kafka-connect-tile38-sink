package guru.bonacci.kafka.connect.tile38.writer;

import static guru.bonacci.kafka.connect.tile38.commands.CommandGenerator.from;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;

import guru.bonacci.kafka.connect.tile38.commands.CommandGenerators;
import guru.bonacci.kafka.connect.tile38.commands.CommandTemplates;
import guru.bonacci.kafka.connect.tile38.config.Tile38SinkConnectorConfig;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Tile38Writer {

	@Getter private final RedisClient client; // for testing
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

	private void wait(RedisFuture<?>... futures) {
		// Wait until all futures complete
		boolean completed = LettuceFutures.awaitAll(100, MILLISECONDS, futures);

		if (!completed) {
			// Only the non-completed tasks can be cancelled
			for (RedisFuture<?> f : futures) {
				f.cancel(true);
			}

			throw new RetriableException(
					String.format("Timeout after %s ms while waiting for operation to complete.", 100));
		}
	}

    void writeForTopic(final String topic, List<Tile38Record> events) {
    	final RedisFuture<?>[] futures = events.stream()
    			.map(event -> cmds.by(topic).compile(event)) // create cmd
    			.map(cmd -> async.dispatch(cmd.getLeft(), cmd.getMiddle(), cmd.getRight())) // execute cmd
    			.toArray(RedisFuture[]::new); // collect futures
				
		try {
			// write all commands to the transport layer
			async.flushCommands();
			wait(futures);

			log.info(futures.length + " commands executed");
		} catch (RedisCommandExecutionException e) {
            log.warn("Exception {} while executing query", e.getMessage());
			throw new ConnectException(e);
		}
    }

    public void writeData(Map<String, List<Tile38Record>> data) {
    	data.entrySet().forEach(d -> writeForTopic(d.getKey(), d.getValue()));
    }

    public void close() {
		client.shutdown();
	}
}
