package guru.bonacci.kafka.connect.tile38.commands;

import static com.google.common.collect.Maps.immutableEntry;
import static java.util.stream.Collectors.toMap;

import java.util.Map;
import java.util.stream.Stream;

import guru.bonacci.kafka.connect.tile38.config.TopicsConfig;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommandTemplates {

	private final Map<String, CommandWrapper> cmds;

	
	private CommandTemplates(Map<String, String> cmdsByTopic) { 
		log.info("Creating command data structure for {}", cmdsByTopic);
		
		cmds = cmdsByTopic.entrySet().stream().map(topicCmd -> {
			CommandWrapper cmd = CommandWrapper.from(topicCmd.getValue());
		    return immutableEntry(topicCmd.getKey(), cmd);
		})
		.collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	public Stream<String> configuredTopics() {
		return cmds.keySet().stream();
	}

	public CommandWrapper templateForTopic(String topic) {
		return cmds.get(topic);
	}
	
	public static CommandTemplates from(TopicsConfig topics) {
		return new CommandTemplates(topics.getCmdsByTopic());
	}
}
