package guru.bonacci.kafka.connect.tile38;

import static lombok.AccessLevel.PRIVATE;
import static com.google.common.collect.Maps.filterKeys;
import static guru.bonacci.kafka.connect.tile38.Constants.COMMAND_PREFIX;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Topics configured to have a Redis command.
 */
@Getter
@RequiredArgsConstructor(access = PRIVATE)
class Topics {

	private final Map<String, String> cmdsByTopic;

	
	Set<String> configuredTopics() {
		return cmdsByTopic.keySet();
	}

	private static Map<String, String> filterByPrefix(Map<String, String> config) {
		Map<String, String> topicsConfig = new HashMap<>(filterKeys(config, prop -> prop.startsWith(COMMAND_PREFIX)));

		// just to change the keys; compare this hack with kotlin's mapKeys... :(
		new ArrayList<>(topicsConfig.keySet())
			.forEach(topic -> topicsConfig.put(topic.replace(COMMAND_PREFIX, ""), topicsConfig.remove(topic)));

		return topicsConfig;
	}
	
	static Topics from(Map<String, String> config) {
		return new Topics(filterByPrefix(config));
	}
}
