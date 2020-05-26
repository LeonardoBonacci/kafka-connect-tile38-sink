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

	    CommandWrapper fooCmd = cmds.commandForTopic("foo");
	    assertThat(fooCmd.getCmdString(), is("foo event.query event.here"));
	    assertThat(fooCmd.getTerms(), hasSize(2));
	    assertThat(fooCmd.getTerms(), hasItem("event.here"));
	    assertThat(fooCmd.getTerms(), hasItem("event.query"));

	    CommandWrapper barCmd = cmds.commandForTopic("bar");
	    assertThat(barCmd.getCmdString(), is("bar event.bar query here event.there"));
	    assertThat(barCmd.getTerms(), hasSize(2));
	    assertThat(barCmd.getTerms(), hasItem("event.bar"));
	    assertThat(barCmd.getTerms(), hasItem("event.there"));
	}
}
