package guru.bonacci.kafka.connect.tile38.commands;

import static com.google.common.collect.Maps.immutableEntry;
import static java.util.stream.Collectors.toMap;
import static lombok.AccessLevel.PRIVATE;

import java.util.Map;
import java.util.stream.Stream;

import guru.bonacci.kafka.connect.tile38.config.TopicsConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Wrapper class to access CommandTemplates by topic
 */
@Slf4j
@RequiredArgsConstructor(access = PRIVATE)
public class CommandTemplates {

	private final Map<String, CommandTemplate> cmds;

	
	public Stream<String> configuredTopics() {
		return cmds.keySet().stream();
	}

	public CommandTemplate templateForTopic(String topic) {
		return cmds.get(topic);
	}
	
	public static CommandTemplates from(TopicsConfig topics) {
		Map<String, String> cmdsByTopic = topics.getCmdsByTopic();
		log.info("Creating command template data structure for {}", cmdsByTopic);
		
		// in -> key: topic name - value: command string
		Map<String, CommandTemplate> cmdTemplates = 
				cmdsByTopic.entrySet().stream().map(cmdForTopic -> {
					CommandTemplate cmd = CommandTemplate.from(cmdForTopic.getValue());
				    return immutableEntry(cmdForTopic.getKey(), cmd);
				})
				.collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

		// out -> key: topic name - value: command template
		return new CommandTemplates(cmdTemplates);
	}
}
