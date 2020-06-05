/**
 * 	Copyright 2020 Jeffrey van Helden (aabcehmu@mailfence.com)
 *	
 *	Licensed under the Apache License, Version 2.0 (the "License");
 *	you may not use this file except in compliance with the License.
 *	You may obtain a copy of the License at
 *	
 *	    http://www.apache.org/licenses/LICENSE-2.0
 *	
 *	Unless required by applicable law or agreed to in writing, software
 *	distributed under the License is distributed on an "AS IS" BASIS,
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *	See the License for the specific language governing permissions and
 *	limitations under the License.
 */
package guru.bonacci.kafka.connect.tile38;

import static com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper.write;
import static io.lettuce.core.codec.StringCodec.UTF8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

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
		Map<String, String> config = Maps.newHashMap(provideConfig(topic));
		config.remove("tile38.topic.foo");

		Assertions.assertThrows(ConfigException.class, () -> {
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
	public void topicRegex() {
		final String topic = "foo";
		Map<String, String> config = Maps.newHashMap(provideConfig(topic));
		config.remove("topics");
		config.put(SinkTask.TOPICS_REGEX_CONFIG, topic);
		
		Assertions.assertThrows(ConfigException.class, () -> {
			this.task.start(config);
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
	public void emptyPut() {
		final String topic = "foo";

		this.task.start(provideConfig(topic));
		this.task.put(ImmutableList.of());
	}

	static Stream<Arguments> provideForInvalidCommandTemplateStartup() {
	    return Stream.of(
	      Arguments.of("event.one event.two event.three"),
	      Arguments.of(" set "),
	      Arguments.of("set nokey")
	    );
	}

	@ParameterizedTest
	@MethodSource("provideForInvalidCommandTemplateStartup")
  	public void invalidCommandTemplateStartup(String cmdString) {
  	    final String topic = "foo";
  		Map<String, String> config = Maps.newHashMap(provideConfig(topic));
  		config.put("tile38.topic.foo", cmdString);

  		Assertions.assertThrows(ConfigException.class, () -> {
  	  		this.task.start(config);
  		});
  	}

	private static Stream<Arguments> provideForValidWrite() {
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
	@MethodSource("provideForValidWrite")
	public void validWrites(String route, String lat, String lon) {
	    final String topic = "foo";
	    this.task.start(provideConfig(topic));

		final String id = "fooid";
		Schema schema = getRouteSchema();
		Struct value = new Struct(schema).put("id", id).put("route", route).put("lat", lat).put("lon", lon);

		final List<SinkRecord> records = ImmutableList.of(write(topic, Schema.STRING_SCHEMA, id, schema, value));
		this.task.put(records);

		RedisCommands<String, String> sync = this.task.getWriter().getClient().connect().sync();
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
		config.put("tile38.topic.foo", "SET foo event.id POINT event.nested.lat event.nested.lon");
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

		RedisCommands<String, String> sync = this.task.getWriter().getClient().connect().sync();
		CommandArgs<String, String> get = getFooCommand(id);
		String resp = sync.dispatch(CommandType.GET, new StatusOutput<>(StringCodec.UTF8), get);
		assertThat(parser.parse(resp), is(equalTo(parser.parse(String.format(RESULT_STRING, lon, lat)))));
	}

	private static Stream<Arguments> provideForIgnoredFieldWrite() {
	    return Stream.of(
	      Arguments.of("0", "0.1", "1.0"),
	      Arguments.of("0.0", "0.1", "1.0")
	    );
	}

	@ParameterizedTest
	@MethodSource("provideForIgnoredFieldWrite")
	public void IgnoredFieldWrite(String route, String lat, String lon) {
	    final String topic = "foo";
		this.task.start(provideConfig(topic));

		final String id = "fooid";
		Schema schema = getRouteSchema();
		Struct value = new Struct(schema).put("id", id).put("route", route).put("lat", lat).put("lon", lon);

		final List<SinkRecord> records = ImmutableList.of(write(topic, Schema.STRING_SCHEMA, id, schema, value));
		this.task.put(records);

		RedisCommands<String, String> sync = this.task.getWriter().getClient().connect().sync();
		CommandArgs<String, String> getCmd = getFooCommand(id);
		String resp = executeWithFieldsCommand(sync, getCmd);

		assertThat(resp, is(not(equalTo(route))));
	}

	private static Stream<Arguments> provideForInvalidWriteCommands() {
	    return Stream.of(
	      Arguments.of("SET foo event.one FIELD route event.two POINT event.three event.four", "fooid", "12.3", "string", "no float"),
	      Arguments.of("SET foo  event.one FIELD route event.two POINT event.three event.four", "fooid", "null", "0.1", "1.0"),
	      Arguments.of("SET foo event.one FIELD route event.two POINT event.three event.four", "fooid", "not a float", "3.1", "4.1"),
	      Arguments.of("SET foo event.one FIELD route event.two POINT event.three event.four", "fooid", "nothing", "0.1", "1.0"),
	      Arguments.of("SET foo event.one FIELD route event.two POINT event.three event.four", "fooid", "1f", "3.1", "4.1"),
	      Arguments.of("SET foo event.one FIELD route event.two POINT event.three event.four", "fooid", "1", "3.1f", "4.1"),
	      Arguments.of("SET foo event.one FIELD route event.two POINT event.three event.four", "fooid", "1F", "3.1", "4.1"),
	      Arguments.of("SET foo event.one FIELD route event.two POINT event.three event.four", "fooid", "1", "3.1", "4.1F"),
	      Arguments.of("SET foo event.one FIELD route event.two POINT event.three event.four", "fooid", "event.route", "event.lat", "event.lon"),
  	      Arguments.of("SET bar event.one FIELD POINT event.two event.three", "123", "1.2", "2.3", null),
	      Arguments.of("SET bar event.one FIELD POINT event.two event.three", "null", "%%", "@@", null),
	      Arguments.of("SET bar event.one FIELD POINT event.two event.three", "$$", "1.2", "2.3", null),
	      Arguments.of("SET bar event.one FIELD event.two event.three", "100", "1.2", "2.3", null)
	    );
	}
	
	@ParameterizedTest
	@MethodSource("provideForInvalidWriteCommands")
	public void invalidWriteCommands(String cmdString, String one, String two, String three, String four) {
	    final String topic = "foo";
		Map<String, String> config = Maps.newHashMap(provideConfig(topic));
		config.put("tile38.topic.foo", cmdString);
		this.task.start(config);

		SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("one", Schema.STRING_SCHEMA).field("two", Schema.STRING_SCHEMA)
				.field("three", Schema.STRING_SCHEMA);
		if (four != null)
			schemaBuilder.field("four", Schema.STRING_SCHEMA);
		Schema schema = schemaBuilder.build();

		Struct valueBuilder = new Struct(schema).put("one", one).put("two", two).put("three", three);
		Struct value = four != null ? valueBuilder.put("four", four) : valueBuilder;

		final List<SinkRecord> records = ImmutableList.of(write(topic, Schema.STRING_SCHEMA, one, schema, value));
		Assertions.assertThrows(ConnectException.class, () -> {
			this.task.put(records);
		});
	}

	// below some helper methods
	private Map<String, String> provideConfig(String topic) {
		return ImmutableMap.of(SinkTask.TOPICS_CONFIG, topic, 
				Tile38SinkConnectorConfig.TILE38_HOST, host,
				Tile38SinkConnectorConfig.TILE38_PORT, port, 
				"tile38.topic.foo", "SET   foo    event.id   FIELD  route   event.route   POINT   event.lat   event.lon");
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
