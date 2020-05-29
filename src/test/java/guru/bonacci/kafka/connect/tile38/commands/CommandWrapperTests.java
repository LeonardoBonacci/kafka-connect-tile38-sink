package guru.bonacci.kafka.connect.tile38.commands;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

public class CommandWrapperTests {

	@Test
	void startWithEvent() {
		final String cmdString = "event.id is to be sub event.sub and event.foo event.nest.ed";

		Assertions.assertThrows(ConfigException.class, () -> {
			CommandTemplate.from(cmdString);
		});
	}

	@Test
	void startTrimmedWithEvent() {
		final String cmdString = "  event.id is to be sub event.sub and event.foo event.nest.ed";

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
	void trimming() {
		final String spacedCmdString = "  foo is to be sub event.sub and event.foo event.nest.ed  ";
		final String cmdString = "foo is to be sub event.sub and event.foo event.nest.ed";

		CommandTemplate spacedCmd = CommandTemplate.from(spacedCmdString);
		CommandTemplate cmd = CommandTemplate.from(cmdString);

		assertThat(spacedCmd, is(equalTo(cmd)));
	}

	@Test
	void normal() {
		final String cmdString = "foo event.id to be sub event.sub and event.foo event.nest.ed";

		CommandTemplate cmd = CommandTemplate.from(cmdString); 

	    assertThat(cmd.getCmdString(), is(equalTo(cmdString)));
	    assertThat(cmd.getKey(), is(equalTo("foo")));
	    assertThat(cmd.getTerms(), is(equalTo(ImmutableSet.of("event.foo", "event.id", "event.sub", "event.nest.ed"))));
	}
}
