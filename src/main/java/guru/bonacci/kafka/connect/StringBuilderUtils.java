package guru.bonacci.kafka.connect;

import java.util.regex.Pattern;

public class StringBuilderUtils {

	// (string) builder for better performance
	public static StringBuilder replaceAll(StringBuilder sb, String find, String replace){
        return new StringBuilder(Pattern.compile(find).matcher(sb).replaceAll(replace));
    }
}
