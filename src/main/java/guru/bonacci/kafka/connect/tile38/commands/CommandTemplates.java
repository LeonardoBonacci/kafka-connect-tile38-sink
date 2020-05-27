package guru.bonacci.kafka.connect.tile38.commands;

import static com.google.common.collect.Maps.immutableEntry;
import static java.util.stream.Collectors.toMap;
import static lombok.AccessLevel.PRIVATE;

import java.util.Map;
import java.util.stream.Stream;

import guru.bonacci.kafka.connect.tile38.config.TopicsConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(access = PRIVATE)
public class CommandTemplates {

	private final Map<String, CommandWrapper> cmds;

	
	public Stream<String> configuredTopics() {
		return cmds.keySet().stream();
	}

	public CommandWrapper templateForTopic(String topic) {
		return cmds.get(topic);
	}
	
	public static CommandTemplates from(TopicsConfig topics) {
		Map<String, String> cmdsByTopic = topics.getCmdsByTopic();
		log.info("Creating command template data structure for {}", cmdsByTopic);
		
		Map<String, CommandWrapper> cmdTemplates = 
				cmdsByTopic.entrySet().stream().map(topicCmd -> {
					CommandWrapper cmd = CommandWrapper.from(topicCmd.getValue());
				    return immutableEntry(topicCmd.getKey(), cmd);
				})
				.collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

		return new CommandTemplates(cmdTemplates);
	}
}
