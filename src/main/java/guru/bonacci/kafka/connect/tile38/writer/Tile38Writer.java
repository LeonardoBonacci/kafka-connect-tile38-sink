package guru.bonacci.kafka.connect.tile38.writer;

import static guru.bonacci.kafka.connect.tile38.commands.CommandGenerator.from;
import static guru.bonacci.kafka.connect.tile38.validators.BehaviorOnErrorValues.FAIL;
import static io.lettuce.core.LettuceFutures.awaitAll;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.stream.Stream;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;

import guru.bonacci.kafka.connect.tile38.commands.CommandGenerators;
import guru.bonacci.kafka.connect.tile38.commands.CommandTemplates;
import guru.bonacci.kafka.connect.tile38.config.Tile38SinkConnectorConfig;
import guru.bonacci.kafka.connect.tile38.validators.BehaviorOnErrorValues;
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

	private final CommandGenerators cmds;

	// Configurations
	private final int flushTimeOutMs;
    private final BehaviorOnErrorValues behaviorOnError;
    
	public Tile38Writer(Tile38SinkConnectorConfig config) {
		this.client = RedisClient.create(
    			String.format("redis://%s:%d", config.getHost(), config.getPort()));

		// disable auto-flushing to allow for batch inserts
		this.async = client.connect().async();
		this.async.setAutoFlushCommands(false);

		this.flushTimeOutMs = config.getFlushTimeOut();
		this.behaviorOnError = config.getBehaviorOnError();
		
		final CommandTemplates cmdTemplates = config.getCmdTemplates();
		// We need an available command generator for each configured topic
		this.cmds = CommandGenerators.from(cmdTemplates.configuredTopics()
				.collect(toMap(identity(), topic -> from(cmdTemplates.templateForTopic(topic)))));
    }


	public void write(Stream<Tile38Record> records) {
		final RedisFuture<?>[] futures = records
				.map(event -> cmds.by(event.getTopic()).compile(event)) // create command
				.map(cmd -> async.dispatch(cmd.getLeft(), cmd.getMiddle(), cmd.getRight())) // execute command
				.toArray(RedisFuture[]::new); // collect futures

		// async batch insert
		async.flushCommands();

		//TODO Can this be called non-blocking?
		wait(futures);
		
		log.trace(futures.length + " commands executed");
    }

	private void wait(RedisFuture<?>... futures) {
		// Wait until all commands are executed
		try {
			boolean completed = awaitAll(flushTimeOutMs, MILLISECONDS, futures);
			if (!completed) {
				// Only the non-completed tasks can be cancelled
				for (RedisFuture<?> f : futures) {
					f.cancel(true);
				}

				throw new RetriableException(
						String.format("Timeout after %s ms while waiting for operation to complete.", flushTimeOutMs));
			}
		} catch (RedisCommandExecutionException e) {
			log.warn(e.getMessage());
			if (FAIL.equals(behaviorOnError)) {
				throw new ConnectException(e);
			}
		}
	}

    public void close() {
		client.shutdown();
	}
}
