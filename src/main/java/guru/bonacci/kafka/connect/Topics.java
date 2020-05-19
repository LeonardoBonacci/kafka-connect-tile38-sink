package guru.bonacci.kafka.connect;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class Topics {

	private final Map<String, String> cmdsByTopic;
	

	Set<String> allTopics() {
		return cmdsByTopic.keySet();
	}
	
	static Topics from(Map<String, String> config) {
		Map<String, String> cmds 
		  = ImmutableMap.of("foo", "foo event.id FIELD route event.route POINT event.lat event.lon", 
				  			"bar", "bar event.id FIELD route event.route POINT event.lat event.lon");
		
		return new Topics(cmds);
	}
}
