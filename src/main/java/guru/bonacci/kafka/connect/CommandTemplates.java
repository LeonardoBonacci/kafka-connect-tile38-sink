package guru.bonacci.kafka.connect;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import com.google.common.collect.Maps;

public class CommandTemplates {

	private final Map<String, ImmutablePair<String, Set<String>>> commands;

	
	private CommandTemplates(Map<String, String> cmdsByTopic) { 
		commands = cmdsByTopic.entrySet().stream().map(topicCmd -> {
			String commandString = topicCmd.getValue();
			
		    Set<String> terms = new HashSet<>();
		    CollectionUtils.addAll(terms, commandString.split(" "));
		    ImmutablePair<String, Set<String>> cmd = new ImmutablePair<>(commandString, terms);
		    cmd.right.removeIf(s -> !s.startsWith(CommandGenerator.TOKERATOR));
	
		    return Maps.immutableEntry(topicCmd.getKey(), cmd);
		})
		.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	ImmutablePair<String, Set<String>> commandForTopic(String topic) {
		return commands.get(topic);
	}
	
	static CommandTemplates from(Topics topics) {
		return new CommandTemplates(topics.getCmdsByTopic());
	}
}
