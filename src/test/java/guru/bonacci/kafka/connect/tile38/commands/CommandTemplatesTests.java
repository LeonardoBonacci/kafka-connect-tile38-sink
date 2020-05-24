package guru.bonacci.kafka.connect.tile38.commands;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import guru.bonacci.kafka.connect.tile38.config.TopicsConfigTests;


public class CommandTemplatesTests {

	@Test
	void commandArgs() {
		CommandTemplates cmds = CommandTemplates.from(TopicsConfigTests.provideTopics());

	    assertThat(cmds.configuredTopics().count(), is(2l));

	    Pair<String, Set<String>> fooCmd = cmds.commandForTopic("foo");
	    assertThat(fooCmd.getLeft(), is("foo event.query event.here"));
	    assertThat(fooCmd.getRight(), hasSize(2));
	    assertThat(fooCmd.getRight(), hasItem("event.here"));
	    assertThat(fooCmd.getRight(), hasItem("event.query"));

	    Pair<String, Set<String>> barCmd = cmds.commandForTopic("bar");
	    assertThat(barCmd.getLeft(), is("event.bar query here event.there"));
	    assertThat(barCmd.getRight(), hasSize(2));
	    assertThat(barCmd.getRight(), hasItem("event.bar"));
	    assertThat(barCmd.getRight(), hasItem("event.there"));
	}
}
