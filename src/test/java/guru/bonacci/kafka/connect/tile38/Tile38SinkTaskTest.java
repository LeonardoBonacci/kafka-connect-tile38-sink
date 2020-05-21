package guru.bonacci.kafka.connect.tile38;

import static com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper.write;
import static io.lettuce.core.codec.StringCodec.UTF8;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

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
import org.junit.jupiter.api.extension.RegisterExtension;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.docker.compose.DockerComposeExtension;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import lombok.extern.slf4j.Slf4j;

public class Tile38SinkTaskTest {

	@RegisterExtension
    public static DockerComposeExtension docker = DockerComposeExtension.builder()
            .file("src/test/resources/docker-compose.yml")
            .saveLogsTo("build/dockerLogs/dockerComposeRuleTest")
            .build();

	
	Tile38SinkTask task;

	private static InetSocketAddress connect() {
		  try {
		    if (docker == null) {
		      throw new IllegalStateException("Docker compose rule cannot be run, is null.");
		    } else {
		      docker.before();
		      return InetSocketAddress.createUnresolved(
		          docker.containers().ip(),
		          docker.hostNetworkedPort(9851).getExternalPort());
		    }
		  } catch (IOException | InterruptedException | IllegalStateException e) {
		    throw new RuntimeException("Could not run docker compose rule.", e);
		  }
		}
	
	@BeforeEach
	public void before() {
		this.task = new Tile38SinkTask();
	}

	@Test
	public void emptyAssignment() {
		InetSocketAddress address = connect();
		final String topic = "foo";
		SinkTaskContext context = mock(SinkTaskContext.class);
		when(context.assignment()).thenReturn(ImmutableSet.of());
		this.task.initialize(context);
		
		Assertions.assertThrows(ConfigException.class, () -> {
			this.task.start(ImmutableMap.of(SinkTask.TOPICS_CONFIG, topic, 
					Tile38SinkConnectorConfig.TILE38_URL, address.getHostString(),
							Tile38SinkConnectorConfig.TILE38_PORT, "" + address.getPort()));
		  });
	}

	@Test
	public void putEmpty() {
		InetSocketAddress address = connect();
		final String topic = "foo";
		SinkTaskContext context = mock(SinkTaskContext.class);
		when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
		this.task.initialize(context);

		this.task.start(ImmutableMap.of(SinkTask.TOPICS_CONFIG, topic, 
				Tile38SinkConnectorConfig.TILE38_URL, address.getHostString(),
				Tile38SinkConnectorConfig.TILE38_PORT, "" + address.getPort(),
				"tile38.topic.foo", "foo event.id FIELD route event.route POINT event.lat event.lon"));

		this.task.put(ImmutableList.of());
	}

	@Test
	public void putWrite() {
		InetSocketAddress address = connect();
		final String topic = "foo";
		SinkTaskContext context = mock(SinkTaskContext.class);
		when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
		this.task.initialize(context);
		this.task.start(ImmutableMap.<String, String>builder()
						.put(SinkTask.TOPICS_CONFIG, topic)
						.put(Tile38SinkConnectorConfig.TILE38_URL, address.getHostString())
						.put(Tile38SinkConnectorConfig.TILE38_PORT, "" + address.getPort())
						.put("tile38.topic.foo", "foo event.id FIELD route event.route POINT event.lat event.lon")
						.build()
						);
										
		final List<SinkRecord> records = new ArrayList<>();

		final String key = "fooid";

		Schema schema = SchemaBuilder.struct()
		         .field("id", Schema.STRING_SCHEMA)
		         .field("route", Schema.STRING_SCHEMA)
		         .field("lat", Schema.STRING_SCHEMA)
		         .field("lon", Schema.STRING_SCHEMA)
		         .build();
		
        Struct value = new Struct(schema)
                .put("id", key)
                .put("route", "12.3")
		        .put("lat", "4.56")
		        .put("lon", "7.89");

		records.add(write(topic, Schema.STRING_SCHEMA, key, schema, value));

		this.task.put(records);

		RedisCommands<String, String> sync = this.task.getService().getSync();
		
		CommandArgs<String, String> get = new CommandArgs<>(UTF8);
		get.add("foo");
		get.add(key);
		
		String resp = sync.dispatch(CommandType.GET, new StatusOutput<>(StringCodec.UTF8), get);
		assertEquals("{\"type\":\"Point\",\"coordinates\":[7.89,4.56]}", resp);

		get.add("WITHFIELDS");
		resp = sync.dispatch(CommandType.GET, new StatusOutput<>(StringCodec.UTF8), get);

		assertEquals("12.3", resp);
	}

	@AfterEach
	public void after() {
		if (null != this.task) {
			this.task.stop();
		}
	}

}
