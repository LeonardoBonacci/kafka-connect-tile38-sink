package guru.bonacci.kafka.connect.tile38.commands;

import static lombok.AccessLevel.PRIVATE;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = PRIVATE)
public class CommandGenerators {

	private final Map<String, CommandGenerator> cmds;
	

	public CommandGenerator by(String topic) {
		return cmds.get(topic);
	}
	
	public static CommandGenerators from(Map<String, CommandGenerator> cmds) {
		if (cmds == null)
			cmds = ImmutableMap.of();

		return new CommandGenerators(cmds);
	}
}
