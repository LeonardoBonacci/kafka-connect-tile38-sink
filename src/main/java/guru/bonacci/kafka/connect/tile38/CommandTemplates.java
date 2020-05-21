package guru.bonacci.kafka.connect.tile38;

import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Sets.newHashSet;
import static guru.bonacci.kafka.connect.tile38.Constants.TOKERATOR;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.tuple.ImmutablePair.of;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.ImmutablePair;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommandTemplates {

	private final Map<String, ImmutablePair<String, Set<String>>> commands;

	
	private CommandTemplates(Map<String, String> cmdsByTopic) { 
		log.info("log something");
		
		commands = cmdsByTopic.entrySet().stream().map(topicCmd -> {
			String cmdString = topicCmd.getValue();
			
		    Set<String> terms = newHashSet(cmdString.split(" "));
		    ImmutablePair<String, Set<String>> cmd = of(cmdString, terms);
		    cmd.right.removeIf(s -> !s.startsWith(TOKERATOR));
	
		    return immutableEntry(topicCmd.getKey(), cmd);
		})
		.collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	Stream<String> allTopics() {
		return commands.keySet().stream();
	}

	ImmutablePair<String, Set<String>> commandForTopic(String topic) {
		return commands.get(topic);
	}
	
	static CommandTemplates from(Topics topics) {
		return new CommandTemplates(topics.getCmdsByTopic());
	}
}
