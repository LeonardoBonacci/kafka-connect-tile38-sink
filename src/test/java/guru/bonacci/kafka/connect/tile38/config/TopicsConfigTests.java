package guru.bonacci.kafka.connect.tile38.config;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import guru.bonacci.kafka.connect.tile38.config.TopicsConfig;

public class TopicsConfigTests {

	public static TopicsConfig provideTopics() {
		return TopicsConfig.from(ImmutableMap.of(
				"key.converter", "org.apache.kafka.connect.storage.StringConverter", 
				"value.converter", "org.apache.kafka.connect.storage.StringConverter", 
			    "topics", "foo,bar",
			    "tile38.topic.foo", "SET foo event.query event.here",
		    	"tile38.topic.bar", "set bar event.bar query here event.there"));
	}

	@Test
	void filterByPrefix() {
		TopicsConfig topics = provideTopics();
		Set<String> configuredTopics = topics.configuredTopics();
	    assertThat(configuredTopics, hasSize(2));

	    Map<String, String> cmds = topics.getCmdsByTopic();
		assertThat(cmds, is(aMapWithSize(2)));
	    assertThat(cmds.get("foo"), is("SET foo event.query event.here"));
	    assertThat(cmds.get("bar"), is("set bar event.bar query here event.there"));
	}
}
