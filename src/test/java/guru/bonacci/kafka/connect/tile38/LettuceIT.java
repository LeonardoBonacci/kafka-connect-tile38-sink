package guru.bonacci.kafka.connect.tile38;

import static io.lettuce.core.codec.StringCodec.UTF8;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.google.common.collect.Lists;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;

/**
 * Some Lettuce commands experiments
 */
@Testcontainers
public class LettuceIT {

	@SuppressWarnings("rawtypes")
	@Container
	private DockerComposeContainer composeContainer = new DockerComposeContainer(
			new File("src/test/resources/docker-compose.yml")).withExposedService("tile38", 9851);


	private String host;
	private Integer port;
	private RedisClient client;

	@BeforeEach
	void setup() {
		this.host = composeContainer.getServiceHost("tile38", 9851);
		this.port = composeContainer.getServicePort("tile38", 9851);
    	this.client = RedisClient.create(String.format("redis://%s:%d", host, port));
	}

	@Test
	void runSync() {
		RedisCommands<String, String> sync = client.connect().sync();
		CommandArgs<String, String> cmdArgs = new CommandArgs<>(UTF8);
		cmdArgs.add("fleet");
		cmdArgs.add("truck1");
		cmdArgs.add("POINT");
		cmdArgs.add("10.01");
		cmdArgs.add("10.02");
		String resp = sync.dispatch(CommandType.SET, new StatusOutput<>(UTF8), cmdArgs);
		System.out.println(resp);

		cmdArgs = new CommandArgs<>(UTF8);
		cmdArgs.add("fleet");
		cmdArgs.add("truck1");
		resp = sync.dispatch(CommandType.GET, new StatusOutput<>(UTF8), cmdArgs);
		System.out.println(resp);
	}

	@Test
	void runAsync() throws InterruptedException, ExecutionException {
		StatefulRedisConnection<String, String> connection = client.connect();
		RedisAsyncCommands<String, String> commands = connection.async();

		CommandArgs<String, String> cmdArgs = new CommandArgs<>(UTF8);
		cmdArgs.add("fleet");
		cmdArgs.add("truck1");
		cmdArgs.add("POINT");
		cmdArgs.add("10.01");
		cmdArgs.add("10.02");

		// perform a series of independent calls
		RedisFuture<?> futures = commands.dispatch(CommandType.SET, new StatusOutput<>(UTF8), cmdArgs);
		System.out.println(futures.get().toString());
		
		// later
		connection.close();
	}

	@Test
	void runAsync2() throws InterruptedException, ExecutionException {
		StatefulRedisConnection<String, String> connection = client.connect();
		RedisAsyncCommands<String, String> commands = connection.async();

		// disable auto-flushing
		commands.setAutoFlushCommands(false);
	
		// perform a series of independent calls
		List<RedisFuture<?>> futures = Lists.newArrayList();
		for (int i = 0; i < 100; i++) {
			RedisFuture<?> future = commands.dispatch(CommandType.SET, new StatusOutput<>(UTF8), cmdArgs(i));
		    futures.add(future);
		}
	
		// write all commands to the transport layer
		commands.flushCommands();
	
		// synchronization example: Wait until all futures complete
		boolean result = LettuceFutures.awaitAll(5, TimeUnit.SECONDS,
		                   futures.toArray(new RedisFuture[futures.size()]));
		System.out.println(result);
		System.out.println("SHUHUU");

		for (int i = 0; i < 100; i++) {
			CommandArgs<String, String> cmdArgs = new CommandArgs<>(UTF8);
			cmdArgs.add("fleet");
			cmdArgs.add("truck" + 1);
			RedisCommands<String, String> sync = client.connect().sync();
			String resp = sync.dispatch(CommandType.GET, new StatusOutput<>(UTF8), cmdArgs);
			System.out.println(resp);
		}	
	}

	@Test
	void runAsync3() throws InterruptedException, ExecutionException {
		StatefulRedisConnection<String, String> connection = client.connect();
		RedisAsyncCommands<String, String> commands = connection.async();

		// disable auto-flushing
		commands.setAutoFlushCommands(false);
	
		// perform a series of independent calls
		List<RedisFuture<?>> futures = Lists.newArrayList();
		for (int i = 0; i < 100; i++) {
			RedisFuture<?> future = commands.dispatch(CommandType.SET, new StatusOutput<>(UTF8), cmdArgs(i));
			RedisFuture<?> futured = commands.dispatch(CommandType.DEL, new IntegerOutput<>(UTF8), delArgs(i));
			futures.add(future);
			futures.add(futured);
		}
	
		// write all commands to the transport layer
		commands.flushCommands();
	
		// synchronization example: Wait until all futures complete
		boolean result = LettuceFutures.awaitAll(5, TimeUnit.SECONDS,
		                   futures.toArray(new RedisFuture[futures.size()]));
		System.out.println(result);
		System.out.println("SHUHUU");

		for (int i = 0; i < 100; i++) {
			CommandArgs<String, String> cmdArgs = new CommandArgs<>(UTF8);
			cmdArgs.add("fleet");
			cmdArgs.add("truck" + 1);
			RedisCommands<String, String> sync = client.connect().sync();
			String resp = sync.dispatch(CommandType.GET, new StatusOutput<>(UTF8), cmdArgs);
			System.out.println(resp);
		}	
	}

	public CommandArgs<String, String> cmdArgs(int nr) {
		CommandArgs<String, String> cmdArgs = new CommandArgs<>(UTF8);
		cmdArgs.add("fleet");
		cmdArgs.add("truck" + nr);
		cmdArgs.add("POINT");
		cmdArgs.add("10.0" + nr);
		cmdArgs.add("20.0" + nr);
		return cmdArgs;
	}

	public CommandArgs<String, String> delArgs(int nr) {
		CommandArgs<String, String> cmdArgs = new CommandArgs<>(UTF8);
		cmdArgs.add("fleet");
		cmdArgs.add("truck" + nr);
		return cmdArgs;
	}

	@AfterEach
	public void after() {
		client.shutdown();
	}

}
