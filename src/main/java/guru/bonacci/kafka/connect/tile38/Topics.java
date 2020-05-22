package guru.bonacci.kafka.connect.tile38;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.collect.Maps.newHashMap;
import static guru.bonacci.kafka.connect.tile38.Constants.COMMAND_PREFIX;
import static lombok.AccessLevel.PRIVATE;

import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Topics configured to have a Redis command.
 */
@Getter
@ToString
@RequiredArgsConstructor(access = PRIVATE)
class Topics {

	private final Map<String, String> cmdsByTopic;

	
	Set<String> configuredTopics() {
		return cmdsByTopic.keySet();
	}

	private static Map<String, String> filterByPrefix(Map<String, String> config) {
		Map<String, String> topicsConfig = newHashMap(
				filterKeys(config, prop -> prop.startsWith(COMMAND_PREFIX)));

		// Remove the prefix from the keys
		newArrayList(topicsConfig.keySet())
			.forEach(topic -> topicsConfig.put(topic.replace(COMMAND_PREFIX, ""), topicsConfig.remove(topic)));

		return topicsConfig;
	}
	
	static Topics from(Map<String, String> config) {
		return new Topics(filterByPrefix(config));
	}
}
