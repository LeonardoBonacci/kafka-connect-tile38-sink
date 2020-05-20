package guru.bonacci.kafka.connect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class Topics {

	static final String FULL_PREFIX = "tile38.topic.";

	private final Map<String, String> cmdsByTopic;

	Set<String> allTopics() {
		return cmdsByTopic.keySet();
	}

	static Topics from(Map<String, String> config) {
		return new Topics(filterByPrefix(config));
	}

	static Map<String, String> filterByPrefix(Map<String, String> config) {
		Map<String, String> topicsConfig = new HashMap<>(Maps.filterKeys(config, prop -> prop.startsWith(FULL_PREFIX)));

		// just to change the keys; compare this hack with kotlin's mapKeys... :(
		new ArrayList<>(topicsConfig.keySet())
			.forEach(topic -> topicsConfig.put(topic.replace(FULL_PREFIX, ""), topicsConfig.remove(topic)));

		return topicsConfig;
	}
}
