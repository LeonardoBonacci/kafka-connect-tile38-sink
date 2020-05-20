package guru.bonacci.kafka.connect;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;


public class CommandTemplatesTest {

	@Test
	void commandArgs() {
		Map<String, String> config = new HashMap<String, String> (ImmutableMap.of(
				"key.converter", "org.apache.kafka.connect.storage.StringConverter", 
			    "topics", "foo,bar",
			    "tile38.topic.foo", "foo query event.here",
		    	"tile38.topic.bar", "bar event.query here"));

		CommandTemplates cmds = CommandTemplates.from(Topics.from(config));

	    assertThat(cmds.allTopics().count(), is(2l));

	    ImmutablePair<String, Set<String>> fooCmd = cmds.commandForTopic("foo");
	    assertThat(fooCmd.left, is("foo query event.here"));
	    assertThat(fooCmd.right.size(), is(1));
	    assertThat(fooCmd.right.iterator().next(), is("event.here"));

	    ImmutablePair<String, Set<String>> barCmd = cmds.commandForTopic("bar");
	    assertThat(barCmd.left, is("bar event.query here"));
	    assertThat(barCmd.right.size(), is(1));
	    assertThat(barCmd.right.iterator().next(), is("event.query"));
	}

}
