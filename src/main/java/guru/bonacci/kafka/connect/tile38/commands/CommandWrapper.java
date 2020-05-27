package guru.bonacci.kafka.connect.tile38.commands;

import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Sets.newHashSet;
import static guru.bonacci.kafka.connect.tile38.Constants.DEL_RESERVED;
import static guru.bonacci.kafka.connect.tile38.Constants.SET_RESERVED;
import static guru.bonacci.kafka.connect.tile38.Constants.TOKERATOR;
import static lombok.AccessLevel.PRIVATE;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.strip;

import java.util.List;
import java.util.Set;

import org.apache.kafka.common.config.ConfigException;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Getter 
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor(access = PRIVATE)
public class CommandWrapper {

	private final String cmdString;
	private final String key;
	private final Set<String> terms;
	
	
	public static CommandWrapper from(String cmdString) {
		if (isBlank(cmdString)) {
			throw new ConfigException("Command cannot be blank");
		}
		
		String trimmedCmdString = strip(cmdString);
		// list to keep order
		List<String> theTerms = copyOf(trimmedCmdString.split(" "));

		String key = theTerms.get(0);
	    if (key.startsWith(TOKERATOR))
	    	throw new ConfigException("Command {} cannot start with 'event.'. The first command word is the key. Check the docs: {}", 
	    			cmdString, "https://tile38.com/commands/set/");

	    if (of(SET_RESERVED, DEL_RESERVED).contains(key.toUpperCase()))
	    	throw new ConfigException("SET and DEL are reserved words and can be ommitted. Command {} cannot start with either one.", cmdString);
	    	
	    Set<String> terms = newHashSet(theTerms);
		// remove all command terms that do not start with 'event.'
		terms.removeIf(s -> !s.startsWith(TOKERATOR));

	    return new CommandWrapper(trimmedCmdString, key, terms);
	}
}
