package guru.bonacci.kafka.connect.tile38;

import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Sets.newHashSet;
import static guru.bonacci.kafka.connect.tile38.Constants.TOKERATOR;
import static java.util.stream.Collectors.toMap;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommandTemplates {

	private final Map<String, Pair<String, Set<String>>> commands;

	
	private CommandTemplates(Map<String, String> cmdsByTopic) { 
		log.info("log something");
		
		commands = cmdsByTopic.entrySet().stream().map(topicCmd -> {
			String cmdString = topicCmd.getValue();
			
		    Set<String> terms = newHashSet(cmdString.split(" "));
		    Pair<String, Set<String>> cmd = ImmutablePair.of(cmdString, terms);
		    cmd.getRight().removeIf(s -> !s.startsWith(TOKERATOR));
	
		    return immutableEntry(topicCmd.getKey(), cmd);
		})
		.collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	Stream<String> allTopics() {
		return commands.keySet().stream();
	}

	Pair<String, Set<String>> commandForTopic(String topic) {
		return commands.get(topic);
	}
	
	static CommandTemplates from(Topics topics) {
		return new CommandTemplates(topics.getCmdsByTopic());
	}
}
