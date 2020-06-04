package guru.bonacci.kafka.connect.tile38.writer;

import static guru.bonacci.kafka.connect.tile38.commands.CommandGenerator.from;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.time.Duration;
import java.util.stream.Stream;

import guru.bonacci.kafka.connect.tile38.commands.CommandGenerators;
import guru.bonacci.kafka.connect.tile38.commands.CommandTemplates;
import guru.bonacci.kafka.connect.tile38.config.Tile38SinkConnectorConfig;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.Getter;

public class Tile38Writer {

	@Getter private final RedisClient client; // for testing
	private final RedisAsyncCommands<String, String> async;

	private final CommandGenerators cmds;

	public Tile38Writer(Tile38SinkConnectorConfig config) {
		final SocketOptions socketOptions = SocketOptions.builder()
		          .tcpNoDelay(config.getTcpNoDelay())
		          .connectTimeout(Duration.ofMillis(config.getConnectTimeout()))
		          .keepAlive(config.getKeepAliveEnabled())
		          .build();

		final ClientOptions.Builder clientOptions = ClientOptions.builder()
				.socketOptions(socketOptions)
				.requestQueueSize(config.getRequestQueueSize())
				.autoReconnect(config.getAutoReconnectEnabled());
		
		this.client = RedisClient.create(
    			String.format("redis://%s:%d", config.getHost(), config.getPort()));
	    this.client.setOptions(clientOptions.build());

	    // disable auto-flushing to allow for batch inserts
		this.async = client.connect().async();
		this.async.setAutoFlushCommands(false);
		
		final CommandTemplates cmdTemplates = config.getCmdTemplates();
		// a command generator for each configured topic
		this.cmds = CommandGenerators.from(cmdTemplates.configuredTopics()
				.collect(toMap(identity(), topic -> from(cmdTemplates.templateForTopic(topic)))));
    }


	public RedisFuture<?>[] write(Stream<Tile38Record> records) {
		final RedisFuture<?>[] futures = records
				.map(event -> cmds.generatorForTopic(event.getTopic()).compile(event)) // create command
				.map(cmd -> async.dispatch(cmd.getLeft(), cmd.getMiddle(), cmd.getRight())) // execute command
				.toArray(RedisFuture[]::new); // collect futures

		// async batch insert
		async.flushCommands();
		return futures;
    }

    public void close() {
		client.shutdown();
	}
}
