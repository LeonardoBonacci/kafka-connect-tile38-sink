package guru.bonacci.kafka.connect.tile38;

import static lombok.AccessLevel.PRIVATE;

import java.util.Map;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = PRIVATE)
public class CommandGenerators {

	private final Map<String, CommandGenerator> cmds;
	

	CommandGenerator by(String topic) {
		return cmds.get(topic);
	}
	
	static CommandGenerators from(Map<String, CommandGenerator> cmds) {
		return new CommandGenerators(cmds);
	}
}
