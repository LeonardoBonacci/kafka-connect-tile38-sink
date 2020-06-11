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
package guru.bonacci.kafka.connect.tile38.config;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

public class TopicsConfigTests {

	public static Map<String, String> provideConfig() {
		return ImmutableMap.of(
				"key.converter", "org.apache.kafka.connect.storage.StringConverter", 
				"value.converter", "org.apache.kafka.connect.storage.StringConverter", 
			    "topics", "foo,bar",
			    "tile38.topic.foo", "SET foo event.query event.here",
		    	"tile38.topic.bar", "set bar event.bar query here event.there");
	}

	public static TopicsConfig provideTopics() {
		return TopicsConfig.from(provideConfig());
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
