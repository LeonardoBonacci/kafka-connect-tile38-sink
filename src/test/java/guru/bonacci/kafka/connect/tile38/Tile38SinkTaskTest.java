package guru.bonacci.kafka.connect.tile38;

import static com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper.write;
import static io.lettuce.core.codec.StringCodec.UTF8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
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

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;

@Testcontainers
public class Tile38SinkTaskTest {

	@SuppressWarnings("rawtypes")
	@Container
	private DockerComposeContainer composeContainer = new DockerComposeContainer(
			new File("src/test/resources/docker-compose.yml")).withExposedService("tile38", 9851);


	private String host;
	private int port;
	private Tile38SinkTask task;
	
	@BeforeEach
	void setup() {
		host = composeContainer.getServiceHost("tile38", 9851);
		port = composeContainer.getServicePort("tile38", 9851);
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
					Tile38SinkConnectorConfig.TILE38_PORT, "" + port));
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

	private static Stream<Arguments> provideInputForWrite() {
	    return Stream.of(
	      Arguments.of("12.3", "4.56", "7.89")
	    );
	}
	
	@ParameterizedTest
	@MethodSource("provideInputForWrite")
	public void putWrite(String routeVal, String latValue, String lonValue) {
		final String topic = "foo";
		SinkTaskContext context = mock(SinkTaskContext.class);
		when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
		this.task.initialize(context);
		this.task.start(ImmutableMap.<String, String>builder().put(SinkTask.TOPICS_CONFIG, topic)
				.put(Tile38SinkConnectorConfig.TILE38_URL, host).put(Tile38SinkConnectorConfig.TILE38_PORT, "" + port)
				.put("tile38.topic.foo", "foo event.id FIELD route event.route POINT event.lat event.lon").build());

		final List<SinkRecord> records = new ArrayList<>();

		final String key = "fooid";

		Schema schema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).field("route", Schema.STRING_SCHEMA)
				.field("lat", Schema.STRING_SCHEMA).field("lon", Schema.STRING_SCHEMA).build();

		Struct value = new Struct(schema).put("id", key).put("route", routeVal).put("lat", latValue).put("lon", lonValue);

		records.add(write(topic, Schema.STRING_SCHEMA, key, schema, value));

		this.task.put(records);

		RedisCommands<String, String> sync = this.task.getService().getSync();

		CommandArgs<String, String> get = new CommandArgs<>(UTF8);
		get.add("foo");
		get.add(key);

		String resp = sync.dispatch(CommandType.GET, new StatusOutput<>(StringCodec.UTF8), get);
		JsonParser parser = new JsonParser();
		assertEquals(parser.parse(String.format("{\"type\":\"Point\",\"coordinates\":[%s,%s]}", lonValue, latValue)), parser.parse(resp));

		get.add("WITHFIELDS");
		resp = sync.dispatch(CommandType.GET, new StatusOutput<>(StringCodec.UTF8), get);

		assertEquals(routeVal, resp);
	}
}
