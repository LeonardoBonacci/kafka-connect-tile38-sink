/**
 * 	Copyright 2020 Jeffrey van Helden (aabcehmu@mailfence.com)
 *	
 *	Licensed under the Apache License, Version 2.0 (the "License");
 *	you may not use this file except in compliance with the License.
 *	You may obtain a copy of the License at
 *	
 *	    http://www.apache.org/licenses/LICENSE-2.0
 *	
 *	Unless required by applicable law or agreed to in writing, software
 *	distributed under the License is distributed on an "AS IS" BASIS,
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *	See the License for the specific language governing permissions and
 *	limitations under the License.
 */
package guru.bonacci.kafka.connect.tile38.commands;

import static org.apache.commons.lang3.StringUtils.strip;
import static guru.bonacci.kafka.connect.tile38.Constants.TOKERATOR;
import static io.lettuce.core.codec.StringCodec.UTF8;
import static java.util.Arrays.asList;
import static java.util.function.Function.identity;
import static java.util.regex.Matcher.quoteReplacement;
import static java.util.stream.Collectors.toMap;
import static lombok.AccessLevel.PRIVATE;
import static org.apache.commons.lang3.tuple.Triple.of;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.connect.errors.DataException;

import guru.bonacci.kafka.connect.tile38.writer.Tile38Record;
import io.lettuce.core.output.BooleanOutput;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Class that facilitates Redis command generation based on sinking records
 */
@Slf4j
@RequiredArgsConstructor(access = PRIVATE)
public class CommandGenerator {

	private final CommandTemplate cmd;

	/**
 	 * Combines a SET or DEL with a possible EXPIRE command
	 */
	public CommandResult compile(final Tile38Record record) {
	    return CommandResult.builder()
	    					.setOrDel(setOrDelCmd(record))
	    					.expire(expireCmd(record))
	    					.build();
	}

	/**
	 * Generates a Redis command based on a record value.
	 * Sending a command with Lettuce requires three arguments:
	 * 1. CommandType: SET or DEL
	 * 2. CommandOutput: Lettuce forces one to anticipate on the result data type
	 * 3. CommandArgs: The actual command terms
	 */
	private Triple<CommandType, CommandOutput<String, String, ?>, CommandArgs<String, String>> setOrDelCmd(final Tile38Record record) {
		final Triple<CommandType, CommandOutput<String, String, ?>, CommandArgs<String, String>> generatedCmd;
		final CommandArgs<String, String> cmdArgs = new CommandArgs<>(UTF8);

		if (record.getValue() != null) {
			// convert the command string into a CommandArgs object.
			asList(preparedStatement(record.getValue()).split(" ")).forEach(cmdArgs::add);
		    generatedCmd = of(CommandType.SET, new StatusOutput<>(UTF8), cmdArgs);
		    
		} else { 		
			// Tombstone messages are deleted
			// Command format: DEL key id
			cmdArgs.add(cmd.getKey()); //key
			cmdArgs.add(record.getId()); //id

			generatedCmd = of(CommandType.DEL, new IntegerOutput<>(UTF8), cmdArgs);
		}
			
		log.trace("Compiled to: {} {}", generatedCmd.getLeft(), cmdArgs.toCommandString());
	    return generatedCmd;
	}

	/**
	 * Generates a Redis command based on a record value.
	 * Sending a command with Lettuce requires three arguments:
	 * 1. CommandType: EXPIRE
	 * 2. CommandOutput: Lettuce forces one to anticipate on the result data type
	 * 3. CommandArgs: The actual command terms
	 */
	private Triple<CommandType, CommandOutput<String, String, ?>, CommandArgs<String, String>> expireCmd(final Tile38Record record) {
		// no expiration or a tombstone record
		if (cmd.getExpirationInSec() == null || record.getValue() == null) {
			return null;
		}
			
		final Triple<CommandType, CommandOutput<String, String, ?>, CommandArgs<String, String>> generatedCmd;
		final CommandArgs<String, String> cmdArgs = new CommandArgs<>(UTF8);
		cmdArgs.add(cmd.getKey()); 
		cmdArgs.add(record.getId()); 
		cmdArgs.add(cmd.getExpirationInSec()); 
		generatedCmd = Triple.of(CommandType.EXPIRE, new BooleanOutput<>(UTF8), cmdArgs);
		log.trace("Compiled to: {} {}", generatedCmd.getLeft(), cmdArgs.toCommandString());
	    return generatedCmd;
	}

	/**
	 * A command statement is created by 
	 * substituting the command terms with the records actual values.
	 * 'event.XYZ' corresponds here to a records field name XYZ.
	 * Nesting is supported with dots, as in 'outermost.outer.inner.innermost'
	 */
	// visible for testing
	String preparedStatement(final Map<String, Object> recordValues) {
		// determine for each command term its corresponding record value
		log.trace("record values {}", recordValues);

		final Map<String, String> parsed = cmd.getTerms().stream()
			.collect(toMap(identity(), term -> {
			try {
				log.debug("term {}", term);
				// field name is query term without 'event.'
				String prop = term.replace(TOKERATOR, "");
				log.debug("prop {}", prop);
				// given the field name, retrieve the field value from the record
				Object val = PropertyUtils.getProperty(recordValues, prop);

				if (val == null) {
					// record does not contain required field 
					throw new IllegalAccessException();
				}
				log.debug("val {}", val);
				return String.valueOf(val);
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				log.warn("Field mismatch command {}, and sink record {}", term, recordValues);
				throw new DataException("Field mismatch between command and sink record", e);
			}
		}));

		// build the command string by replacing the command terms with record values.
		// substitute 'event.x + space' to avoid eager substitution of similar term names, 
		// as in 'foo event.bar POINT event.bar1 event.bar2'
		
		// add a temporary space to command string to substitute last term 
		String generatedCmdString = cmd.getCmdString() + " ";
		for (Map.Entry<String, String> entry : parsed.entrySet()) {
			// thereby escaping characters
			generatedCmdString = generatedCmdString.replaceAll(entry.getKey() + " ", quoteReplacement(entry.getValue() + " "));
		}

		return strip(generatedCmdString);
	}
	
	public static CommandGenerator from(CommandTemplate cmd) {
		return new CommandGenerator(cmd);
	}
}
