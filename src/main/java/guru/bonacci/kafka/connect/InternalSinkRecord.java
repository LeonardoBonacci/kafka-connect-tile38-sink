package guru.bonacci.kafka.connect;

import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
class InternalSinkRecord {

	//TODO Object for nesting?
	private final Map<String, String> value;
}
