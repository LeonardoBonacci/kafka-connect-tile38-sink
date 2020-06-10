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

import static io.lettuce.core.codec.StringCodec.UTF8;
import static org.junit.Assert.assertNull;
import static com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper.write;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;
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
import io.lettuce.core.output.BooleanOutput;
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
	public void expireMany() throws InterruptedException {
		final String topic = "foo";
	    this.task.start(provideConfig(topic));

		final String id = "fooid";
		Schema schema = getRouteSchema();

		final List<SinkRecord> records = new ArrayList<>();
		for (int i=0; i<100; i++) {
			Struct value = new Struct(schema).put("id", id+i).put("route", ""+i).put("lat", ""+1.01*i).put("lon", ""+-1.01*i);
			records.add(write(topic, Schema.STRING_SCHEMA, id, schema, value));
		}	
		this.task.put(records);

		// sleep longer than x seconds
		Thread.sleep(5000);

		RedisCommands<String, String> sync = this.task.getWriter().getClient().connect().sync();
		for (int i=0; i<100; i++) {
			CommandArgs<String, String> get = getFooCommand(id);
			String resp = sync.dispatch(CommandType.GET, new StatusOutput<>(StringCodec.UTF8), get);
			assertNull(resp);
		}	
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
