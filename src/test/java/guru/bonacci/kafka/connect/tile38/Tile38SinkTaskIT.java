package guru.bonacci.kafka.connect.tile38;

import static com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper.write;
import static io.lettuce.core.codec.StringCodec.UTF8;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
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
import com.google.common.collect.Maps;
import com.google.gson.JsonParser;

import guru.bonacci.kafka.connect.tile38.config.Tile38SinkConnectorConfig;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;

/**
 * Sending plain Redis to Tile38 these some commands are:
 * SET foo fooid FIELD route 12.3 POINT 4.56 7.89
 * GET foo fooid -> {"type":"Point","coordinates":[7.89,4.56]}
 * GET foo fooid WITHFIELDS -> {"type":"Point","coordinates":[7.89,4.56]},"fields":{"route":12.3}
 * 
 * For reasons I have yet to comprehend the WITHFIELD variant returns only the 'fields' using Lettuce, 
 * while the entire object is present..
 */
@Testcontainers
public class Tile38SinkTaskIT {

	@SuppressWarnings("rawtypes")
	@Container
	private DockerComposeContainer composeContainer = new DockerComposeContainer(
			new File("src/test/resources/docker-compose.yml")).withExposedService("tile38", 9851);


	private String host;
	private String port;
	private Tile38SinkTask task;
	private JsonParser parser = new JsonParser();

	private static final String RESULT_STRING = "{\"type\":\"Point\",\"coordinates\":[%s,%s]}";

	@BeforeEach
	void setup() {
		this.host = composeContainer.getServiceHost("tile38", 9851);
		this.port = String.valueOf(composeContainer.getServicePort("tile38", 9851));
		this.task = new Tile38SinkTask();
	}

	@AfterEach
	public void after() {
		this.task.stop();
	}

	@Test
	public void emptyAssignment() {
		final String topic = "foo";

		Assertions.assertThrows(ConfigException.class, () -> {
			Map<String, String> config = Maps.newHashMap(provideConfig(topic));
			config.remove("tile38.topic.foo");
			this.task.start(config);
		});
	}

	@Test
	public void invalidTopics() {
		final String topic = "*#^$(^#$(&(#*$&($&(Q#";

		Assertions.assertThrows(ConfigException.class, () -> {
			this.task.start(provideConfig(topic));
		});
	}

	@Test
	public void unconfiguredTopic() {
		final String topic = "foo,bar";

		Assertions.assertThrows(ConfigException.class, () -> {
			this.task.start(provideConfig(topic));
		});
	}

	@Test
	public void empty() {
		final String topic = "foo";

		this.task.start(provideConfig(topic));
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
	public void validWrites(String route, String lat, String lon) {
	    final String topic = "foo";
	    this.task.start(provideConfig(topic));

		final String id = "fooid";
		Schema schema = getRouteSchema();
		Struct value = new Struct(schema).put("id", id).put("route", route).put("lat", lat).put("lon", lon);

		final List<SinkRecord> records = ImmutableList.of(write(topic, Schema.STRING_SCHEMA, id, schema, value));
		this.task.put(records);

		RedisCommands<String, String> sync = this.task.getService().getSync();
		CommandArgs<String, String> get = getFooCommand(id);
		String resp = sync.dispatch(CommandType.GET, new StatusOutput<>(StringCodec.UTF8), get);
		assertThat(parser.parse(resp), is(equalTo(parser.parse(String.format(RESULT_STRING, lon, lat)))));

		resp = executeWithFieldsCommand(sync, get);
		assertThat(resp, is(equalTo(route)));
	}

	public void nestedWrite() {
		final String lat = "4.56", lon = "7.89";
	    final String topic = "foo";

		Map<String, String> config = Maps.newHashMap(provideConfig(topic));
		config.put("tile38.topic.foo", "foo event.id POINT event.nested.lat event.nested.lon");
		this.task.start(config);

		final String id = "fooid";

		Schema nestedSchema = SchemaBuilder.struct()
				.field("lat", Schema.STRING_SCHEMA)
				.field("lon", Schema.STRING_SCHEMA).build();
		Schema schema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA)
				.field("nested", nestedSchema);

		Struct nestedValue = new Struct(nestedSchema).put("lat", lat).put("lon", lon);
		Struct value = new Struct(schema).put("id", id).put("nested", nestedValue);

		final List<SinkRecord> records = ImmutableList.of(write(topic, Schema.STRING_SCHEMA, id, schema, value));
		this.task.put(records);

		RedisCommands<String, String> sync = this.task.getService().getSync();
		CommandArgs<String, String> get = getFooCommand(id);
		String resp = sync.dispatch(CommandType.GET, new StatusOutput<>(StringCodec.UTF8), get);
		assertThat(parser.parse(resp), is(equalTo(parser.parse(String.format(RESULT_STRING, lon, lat)))));
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
	public void invalidWrites(String route, String lat, String lon) {
	    final String topic = "foo";
		this.task.start(provideConfig(topic));

		final String id = "fooid";
		Schema schema = getRouteSchema();
		Struct value = new Struct(schema).put("id", id).put("route", route).put("lat", lat).put("lon", lon);

		final List<SinkRecord> records = ImmutableList.of(write(topic, Schema.STRING_SCHEMA, id, schema, value));
		Assertions.assertThrows(ConnectException.class, () -> {
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
	public void invalidFieldWrites(String route, String lat, String lon) {
	    final String topic = "foo";
		this.task.start(provideConfig(topic));

		final String id = "fooid";
		Schema schema = getRouteSchema();
		Struct value = new Struct(schema).put("id", id).put("route", route).put("lat", lat).put("lon", lon);

		final List<SinkRecord> records = ImmutableList.of(write(topic, Schema.STRING_SCHEMA, id, schema, value));
		this.task.put(records);

		RedisCommands<String, String> sync = this.task.getService().getSync();
		CommandArgs<String, String> getCmd = getFooCommand(id);
		String resp = executeWithFieldsCommand(sync, getCmd);

		assertThat(resp, is(not(equalTo(route))));
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
	public void invalidCommandWrites(String cmdString, String one, String two, String three) {
	    final String topic = "foo";
		Map<String, String> config = Maps.newHashMap(provideConfig(topic));
		config.put("tile38.topic.foo", cmdString);
		this.task.start(config);

		Schema schema = SchemaBuilder.struct().field("one", Schema.STRING_SCHEMA).field("two", Schema.STRING_SCHEMA)
				.field("three", Schema.STRING_SCHEMA).build();
		Struct value = new Struct(schema).put("one", one).put("two", two).put("three", three);

		final List<SinkRecord> records = ImmutableList.of(write(topic, Schema.STRING_SCHEMA, one, schema, value));
		Assertions.assertThrows(ConnectException.class, () -> {
			this.task.put(records);
		});
	}
	
	private Map<String, String> provideConfig(String topic) {
		return ImmutableMap.of(SinkTask.TOPICS_CONFIG, topic, 
				Tile38SinkConnectorConfig.TILE38_URL, host,
				Tile38SinkConnectorConfig.TILE38_PORT, port, 
				"tile38.topic.foo", "foo event.id FIELD route event.route POINT event.lat event.lon");
	}
	
	private Schema getRouteSchema() {
		return SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).field("route", Schema.STRING_SCHEMA)
				.field("lat", Schema.STRING_SCHEMA).field("lon", Schema.STRING_SCHEMA).build();
	}

	private CommandArgs<String, String> getFooCommand(String id) {
		return getCommand("foo", id);
	}

	private CommandArgs<String, String> getCommand(String key, String id) {
		CommandArgs<String, String> cmd = new CommandArgs<>(UTF8);
		cmd.add(key);
		cmd.add(id);
		return cmd;
	}
	
	private String executeWithFieldsCommand(RedisCommands<String, String> sync, CommandArgs<String, String> cmd) {
		cmd.add("WITHFIELDS");
		return sync.dispatch(CommandType.GET, new StatusOutput<>(StringCodec.UTF8), cmd);
	}
}
