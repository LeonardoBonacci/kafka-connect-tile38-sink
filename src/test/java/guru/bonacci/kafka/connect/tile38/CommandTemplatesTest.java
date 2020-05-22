package guru.bonacci.kafka.connect.tile38;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
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

	    Pair<String, Set<String>> fooCmd = cmds.commandForTopic("foo");
	    assertThat(fooCmd.getLeft(), is("foo query event.here"));
	    assertThat(fooCmd.getRight().size(), is(1));
	    assertThat(fooCmd.getRight().iterator().next(), is("event.here"));

	    Pair<String, Set<String>> barCmd = cmds.commandForTopic("bar");
	    assertThat(barCmd.getLeft(), is("bar event.query here"));
	    assertThat(barCmd.getRight().size(), is(1));
	    assertThat(barCmd.getRight().iterator().next(), is("event.query"));
	}

}
