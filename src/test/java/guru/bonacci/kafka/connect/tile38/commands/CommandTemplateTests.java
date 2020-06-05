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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

public class CommandTemplateTests {

	@Test
	void normal() {
		final String cmdString = "SET foo event.id to be sub event.sub and event.foo event.nest.ed";

		CommandTemplate cmd = CommandTemplate.from(cmdString); 

	    assertThat(cmd.getCmdString(), is(equalTo("foo event.id to be sub event.sub and event.foo event.nest.ed")));
	    assertThat(cmd.getKey(), is(equalTo("foo")));
	    assertThat(cmd.getTerms(), is(equalTo(ImmutableSet.of("event.foo", "event.id", "event.sub", "event.nest.ed"))));
	}

	@Test
	void trim() {
		final String spacedCmdString = "  SET foo is to be sub event.sub and event.foo event.nest.ed  ";
		final String cmdString = "SET foo is to be sub event.sub and event.foo event.nest.ed";

		CommandTemplate spacedCmd = CommandTemplate.from(spacedCmdString);
		CommandTemplate cmd = CommandTemplate.from(cmdString);

		assertThat(spacedCmd, is(equalTo(cmd)));
	}

	@Test
	void noStartWithSet() {
		final String cmdString = "event.id is to be sub event.sub and event.foo event.nest.ed";

		Assertions.assertThrows(ConfigException.class, () -> {
			CommandTemplate.from(cmdString);
		});
	}

	@Test
	void emptyCmdString() {
		final String cmdString = null;

		Assertions.assertThrows(ConfigException.class, () -> {
			CommandTemplate.from(cmdString);
		});
	}

	@Test
	void onlySet() {
		final String cmdString = "SET";

		Assertions.assertThrows(ConfigException.class, () -> {
			CommandTemplate.from(cmdString);
		});
	}

	@Test
	void onlySetAndKey() {
		final String cmdString = "SET foo";

		Assertions.assertThrows(ConfigException.class, () -> {
			CommandTemplate.from(cmdString);
		});
	}
	
	@Test
	void setGlued() {
		final String cmdString = "SETfoo event.id is to be sub event.sub and event.foo event.nest.ed";

		Assertions.assertThrows(ConfigException.class, () -> {
			CommandTemplate.from(cmdString);
		});
	}
}
