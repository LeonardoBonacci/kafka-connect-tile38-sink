package guru.bonacci.kafka.connect;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

public class CommandTemplates {

	private final ImmutablePair<String, Set<String>> command;

	
	public CommandTemplates(Tile38SinkConnectorConfig config) { 
		final String commandString = config.getCommand();
		
	    final Set<String> terms = new HashSet<>();
	    CollectionUtils.addAll(terms, commandString.split(" "));
		this.command = new ImmutablePair<>(commandString, terms);
		this.command.right.removeIf(s -> !s.startsWith(CommandGenerator.TOKERATOR));
	}

	public ImmutablePair<String, Set<String>> commandForTopic(String topic) {
		return command;
	}
}
