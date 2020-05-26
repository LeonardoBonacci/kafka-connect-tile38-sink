package guru.bonacci.kafka.connect.tile38.commands;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static guru.bonacci.kafka.connect.tile38.Constants.TOKERATOR;
import static lombok.AccessLevel.PRIVATE;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.*;

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
		List<String> theTerms = newArrayList(trimmedCmdString.split(" "));

		String key = theTerms.get(0);
	    if (key.startsWith(TOKERATOR))
	    	throw new ConfigException("Command {} cannot start with 'event.'. The first command word is the key. Check the docs: {}", 
	    			cmdString, "https://tile38.com/commands/set/");

	    	
	    Set<String> terms = newHashSet(theTerms);
		// remove all command terms that do not start with 'event.'
		terms.removeIf(s -> !s.startsWith(TOKERATOR));

	    return new CommandWrapper(trimmedCmdString, key, terms);
	}
}
