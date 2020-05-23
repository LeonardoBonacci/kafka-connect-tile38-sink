package guru.bonacci.kafka.connect.tile38;

import static com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper.write;
import static io.lettuce.core.codec.StringCodec.UTF8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.List;
import java.util.stream.Stream;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonParser;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Testcontainers
public class Tile38SinkTaskIT {

	@SuppressWarnings("rawtypes")
	@Container
	private DockerComposeContainer composeContainer = new DockerComposeContainer(
			new File("src/test/resources/docker-compose.yml")).withExposedService("tile38", 9851);


	private String host;
	private String port;
	private Tile38SinkTask task;
	
	@BeforeEach
	void setup() {
	    log.info("Set up containers");

		host = composeContainer.getServiceHost("tile38", 9851);
		port = String.valueOf(composeContainer.getServicePort("tile38", 9851));
		this.task = new Tile38SinkTask();
	}

	@AfterEach
	public void after() {
		this.task.stop();
	}

	@Test
	public void emptyAssignment() {
		final String topic = "foo";
		SinkTaskContext context = mock(SinkTaskContext.class);
		when(context.assignment()).thenReturn(ImmutableSet.of());
		this.task.initialize(context);

		Assertions.assertThrows(ConfigException.class, () -> {
			this.task.start(ImmutableMap.of(SinkTask.TOPICS_CONFIG, topic, Tile38SinkConnectorConfig.TILE38_URL, host,
					Tile38SinkConnectorConfig.TILE38_PORT, port));
		});
	}

	@Test
	public void invalidTopics() {
		final String topic = "*#^$(^#$(&(#*$&($&(Q#";
		SinkTaskContext context = mock(SinkTaskContext.class);
		when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
		this.task.initialize(context);

		Assertions.assertThrows(ConfigException.class, () -> {
			this.task.start(ImmutableMap.of(SinkTask.TOPICS_CONFIG, topic, 
					Tile38SinkConnectorConfig.TILE38_URL, host,
					Tile38SinkConnectorConfig.TILE38_PORT, port, 
					"tile38.topic.foo", "foo event.id FIELD route event.route POINT event.lat event.lon"));
		});
	}

	@Test
	public void unconfiguredTopic() {
		final String topic = "foo,bar";
		SinkTaskContext context = mock(SinkTaskContext.class);
		when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
		this.task.initialize(context);

		Assertions.assertThrows(ConfigException.class, () -> {
			this.task.start(ImmutableMap.of(SinkTask.TOPICS_CONFIG, topic, 
					Tile38SinkConnectorConfig.TILE38_URL, host,
					Tile38SinkConnectorConfig.TILE38_PORT, port, 
					"tile38.topic.foo", "foo event.id FIELD route event.route POINT event.lat event.lon"));
		});
	}

	@Test
	public void putEmpty() {
		final String topic = "foo";
		SinkTaskContext context = mock(SinkTaskContext.class);
		when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
		this.task.initialize(context);

		this.task.start(ImmutableMap.of(SinkTask.TOPICS_CONFIG, topic, Tile38SinkConnectorConfig.TILE38_URL, host,
				Tile38SinkConnectorConfig.TILE38_PORT, "" + port, "tile38.topic.foo",
				"foo event.id FIELD route event.route POINT event.lat event.lon"));

		this.task.put(ImmutableList.of());
	}

	private static Stream<Arguments> provideInputForValidWrite() {
	    return Stream.of(
	      Arguments.of("12.3", "4.56", "7.89"),
	      Arguments.of("0.3", "1111.2", "6.9"),
	      Arguments.of("15", "56", "89"),
	      Arguments.of("0.3", "0", "0"),
	      Arguments.of("1", "-0.0001", "-123.89"),
	      Arguments.of("-12.3", "-4.56", "7.89")
	    );
	}
	
	@ParameterizedTest
	@MethodSource("provideInputForValidWrite")
	public void putValidWrite(String route, String lat, String lon) {
	    log.info("Executing putValidWrite with arguments {} {} {}", route, lat, lon);

	    final String topic = "foo";
		SinkTaskContext context = mock(SinkTaskContext.class);
		when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
		this.task.initialize(context);
		this.task.start(ImmutableMap.<String, String>builder().put(SinkTask.TOPICS_CONFIG, topic)
				.put(Tile38SinkConnectorConfig.TILE38_URL, host).put(Tile38SinkConnectorConfig.TILE38_PORT, port)
				.put("tile38.topic.foo", "foo event.id FIELD route event.route POINT event.lat event.lon").build());

		final String key = "fooid";

		Schema schema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).field("route", Schema.STRING_SCHEMA)
				.field("lat", Schema.STRING_SCHEMA).field("lon", Schema.STRING_SCHEMA).build();

		Struct value = new Struct(schema).put("id", key).put("route", route).put("lat", lat).put("lon", lon);

		final List<SinkRecord> records = ImmutableList.of(write(topic, Schema.STRING_SCHEMA, key, schema, value));

		this.task.put(records);

		RedisCommands<String, String> sync = this.task.getService().getSync();

		CommandArgs<String, String> get = new CommandArgs<>(UTF8);
		get.add("foo");
		get.add(key);

		String resp = sync.dispatch(CommandType.GET, new StatusOutput<>(StringCodec.UTF8), get);
		JsonParser parser = new JsonParser();
		assertEquals(parser.parse(String.format("{\"type\":\"Point\",\"coordinates\":[%s,%s]}", lon, lat)), parser.parse(resp));

		get.add("WITHFIELDS");
		resp = sync.dispatch(CommandType.GET, new StatusOutput<>(StringCodec.UTF8), get);

		assertEquals(route, resp);
	}

	private static Stream<Arguments> provideInputForInvalidWrite() {
	    return Stream.of(
	      Arguments.of("12.3", "string", "no float"),
	      Arguments.of("null", "0.1", "1.0"),
	      Arguments.of("not a float", "3.1", "4.1"),
	      Arguments.of("nothing", "0.1", "1.0"),
	      Arguments.of("1f", "3.1", "4.1"),
	      Arguments.of("1", "3.1f", "4.1"),
	      Arguments.of("1F", "3.1", "4.1"),
	      Arguments.of("1", "3.1", "4.1F"),
	      Arguments.of("event.route", "event.lat", "event.lon")
	    );
	}

	@ParameterizedTest
	@MethodSource("provideInputForInvalidWrite")
	public void putInvalidWrite(String route, String lat, String lon) {
	    log.info("Executing putValidWrite with arguments {} {} {}", route, lat, lon);

	    final String topic = "foo";
		SinkTaskContext context = mock(SinkTaskContext.class);
		when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
		this.task.initialize(context);
		this.task.start(ImmutableMap.<String, String>builder().put(SinkTask.TOPICS_CONFIG, topic)
				.put(Tile38SinkConnectorConfig.TILE38_URL, host).put(Tile38SinkConnectorConfig.TILE38_PORT, port)
				.put("tile38.topic.foo", "foo event.id FIELD route event.route POINT event.lat event.lon").build());

		final String key = "fooid";

		Schema schema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).field("route", Schema.STRING_SCHEMA)
				.field("lat", Schema.STRING_SCHEMA).field("lon", Schema.STRING_SCHEMA).build();

		Struct value = new Struct(schema).put("id", key).put("route", route).put("lat", lat).put("lon", lon);

		final List<SinkRecord> records = ImmutableList.of(write(topic, Schema.STRING_SCHEMA, key, schema, value));

		Assertions.assertThrows(RedisCommandExecutionException.class, () -> {
			this.task.put(records);
		});
	}

	private static Stream<Arguments> provideInputForIgnoredFieldWrite() {
	    return Stream.of(
	      Arguments.of("0", "0.1", "1.0"),
	      Arguments.of("0.0", "0.1", "1.0")
	    );
	}

	@ParameterizedTest
	@MethodSource("provideInputForIgnoredFieldWrite")
	public void putInvalidFieldWrite(String route, String lat, String lon) {
	    log.info("Executing putValidWrite with arguments {} {} {}", route, lat, lon);

	    final String topic = "foo";
		SinkTaskContext context = mock(SinkTaskContext.class);
		when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
		this.task.initialize(context);
		this.task.start(ImmutableMap.<String, String>builder().put(SinkTask.TOPICS_CONFIG, topic)
				.put(Tile38SinkConnectorConfig.TILE38_URL, host).put(Tile38SinkConnectorConfig.TILE38_PORT, port)
				.put("tile38.topic.foo", "foo event.id FIELD route event.route POINT event.lat event.lon").build());

		final String key = "fooid";

		Schema schema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).field("route", Schema.STRING_SCHEMA)
				.field("lat", Schema.STRING_SCHEMA).field("lon", Schema.STRING_SCHEMA).build();

		Struct value = new Struct(schema).put("id", key).put("route", route).put("lat", lat).put("lon", lon);

		final List<SinkRecord> records = ImmutableList.of(write(topic, Schema.STRING_SCHEMA, key, schema, value));

		this.task.put(records);
		
		RedisCommands<String, String> sync = this.task.getService().getSync();

		CommandArgs<String, String> get = new CommandArgs<>(UTF8);
		get.add("foo");
		get.add(key);
		get.add("WITHFIELDS");
		String resp = sync.dispatch(CommandType.GET, new StatusOutput<>(StringCodec.UTF8), get);

		assertNotEquals(route, resp);

	}

	private static Stream<Arguments> provideInvalidCommands() {
	    return Stream.of(
  	      Arguments.of("bar event.one FIELD POINT event.two event.three", "123", "1.2", "2.3"),
  	      Arguments.of("event.one event.two event.three", "no", "command", "here"),
	      Arguments.of("bar event.one FIELD POINT event.two event.three", "null", "%%", "@@"),
	      Arguments.of("bar event.one FIELD POINT event.two event.three", "$$", "1.2", "2.3"),
	      Arguments.of("bar event.one FIELD event.two event.three", "100", "1.2", "2.3")
	    );
	}
	
	@ParameterizedTest
	@MethodSource("provideInvalidCommands")
	public void putInvalidCommandsWrite(String cmdString, String one, String two, String three) {
	    final String topic = "foo";
		SinkTaskContext context = mock(SinkTaskContext.class);
		when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
		this.task.initialize(context);
		this.task.start(ImmutableMap.<String, String>builder().put(SinkTask.TOPICS_CONFIG, topic)
				.put(Tile38SinkConnectorConfig.TILE38_URL, host).put(Tile38SinkConnectorConfig.TILE38_PORT, port)
				.put("tile38.topic.foo", cmdString).build());

		Schema schema = SchemaBuilder.struct().field("one", Schema.STRING_SCHEMA).field("two", Schema.STRING_SCHEMA)
				.field("three", Schema.STRING_SCHEMA).build();

		Struct value = new Struct(schema).put("one", one).put("two", two).put("three", three);

		final List<SinkRecord> records = ImmutableList.of(write(topic, Schema.STRING_SCHEMA, one, schema, value));

		Assertions.assertThrows(RedisCommandExecutionException.class, () -> {
			this.task.put(records);
		});
	}

}
