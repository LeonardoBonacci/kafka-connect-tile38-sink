package guru.bonacci.kafka.connect.tile38.commands;

import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Triple;

import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import lombok.Builder;
import lombok.Getter;

@Getter // for testing
@Builder
public class CommandResult {

	private Triple<CommandType, CommandOutput<String, String, ?>, CommandArgs<String, String>> setOrDel;
	
	private Triple<CommandType, CommandOutput<String, String, ?>, CommandArgs<String, String>> expire;
	
	public Stream<Triple<CommandType, CommandOutput<String, String, ?>, CommandArgs<String, String>>> asStream() {
		return Stream.of(setOrDel, expire);
	}
}
