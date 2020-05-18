package guru.bonacci.kafka.connect;

import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class InternalSinkRecord {

	//TODO object for nesting?
	//private Map<String, String> key;
	private final Map<String, String> value;
}
