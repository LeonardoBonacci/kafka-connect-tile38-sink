package guru.bonacci.kafka.connect;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

public class TopicsTest {

	@Test
	void filterByPrefix() {
		Map<String, String> config = new HashMap<String, String> (ImmutableMap.of(
				"key.converter", "org.apache.kafka.connect.storage.StringConverter", 
			    "topics", "foo,bar",
			    "tile38.topic.foo", "foo query here",
		    	"tile38.topic.bar", "bar query here"));

		Map<String, String> result = Topics.filterByPrefix(config);
	    assertThat(result.size(), is(2));
	    assertThat(result.get("foo"), is("foo query here"));
	    assertThat(result.get("bar"), is("bar query here"));
	}

}
