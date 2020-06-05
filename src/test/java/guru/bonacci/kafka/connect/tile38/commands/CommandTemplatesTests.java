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

import org.junit.jupiter.api.Test;

import guru.bonacci.kafka.connect.tile38.config.TopicsConfigTests;


public class CommandTemplatesTests {

	@Test
	void commandArgs() {
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
}
