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
package guru.bonacci.kafka.connect.tile38.commands;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Maps;

import guru.bonacci.kafka.connect.tile38.config.TopicsConfig;
import guru.bonacci.kafka.connect.tile38.config.TopicsConfigTests;


public class CommandTemplatesTests {

	@Test
	void onlyCommandArgs() {
		CommandTemplates cmds = CommandTemplates.from(TopicsConfigTests.provideTopics());

	    assertThat(cmds.configuredTopics().count(), is(2l));

	    CommandTemplate fooCmd = cmds.templateForTopic("foo");
	    assertThat(fooCmd.getCmdString(), is("foo event.query event.here"));
	    assertThat(fooCmd.getTerms(), hasSize(2));
	    assertThat(fooCmd.getTerms(), hasItem("event.here"));
	    assertThat(fooCmd.getTerms(), hasItem("event.query"));

	    CommandTemplate barCmd = cmds.templateForTopic("bar");
	    assertThat(barCmd.getCmdString(), is("bar event.bar query here event.there"));
	    assertThat(barCmd.getTerms(), hasSize(2));
	    assertThat(barCmd.getTerms(), hasItem("event.bar"));
	    assertThat(barCmd.getTerms(), hasItem("event.there"));
	}
	
	@Test
	void commandArgsWithExpire() {
		Map<String, String> config = Maps.newHashMap(TopicsConfigTests.provideConfig());
		config.put("tile38.topic.foo.expire", "5");
		
		CommandTemplates cmds = CommandTemplates.from(TopicsConfig.from(config));

	    assertThat(cmds.configuredTopics().count(), is(2l));

	    CommandTemplate fooCmd = cmds.templateForTopic("foo");
	    assertThat(fooCmd.getCmdString(), is("foo event.query event.here"));
	    assertThat(fooCmd.getTerms(), hasSize(2));
	    assertThat(fooCmd.getTerms(), hasItem("event.here"));
	    assertThat(fooCmd.getTerms(), hasItem("event.query"));
	    assertThat(fooCmd.getExpirationInSec(), is(5));

	    CommandTemplate barCmd = cmds.templateForTopic("bar");
	    assertThat(barCmd.getCmdString(), is("bar event.bar query here event.there"));
	    assertThat(barCmd.getTerms(), hasSize(2));
	    assertThat(barCmd.getTerms(), hasItem("event.bar"));
	    assertThat(barCmd.getTerms(), hasItem("event.there"));
	    assertNull(barCmd.getExpirationInSec());
	}

	@Test
	void commandArgsInvalidExpire() {
		Map<String, String> config = Maps.newHashMap(TopicsConfigTests.provideConfig());
		config.put("tile38.topic.foooooo.expire", "5");
		
		Assertions.assertThrows(ConfigException.class, () -> {
			CommandTemplates.from(TopicsConfig.from(config));
		});
	}	
	
	@Test
	void commandArgsInvalidExpireNumber() {
		Map<String, String> config = Maps.newHashMap(TopicsConfigTests.provideConfig());
		config.put("tile38.topic.foo.expire", "abc");
		
		Assertions.assertThrows(ConfigException.class, () -> {
			CommandTemplates.from(TopicsConfig.from(config));
		});
	}	
}
