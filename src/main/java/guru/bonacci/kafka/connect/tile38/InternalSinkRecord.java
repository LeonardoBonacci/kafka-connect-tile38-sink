package guru.bonacci.kafka.connect.tile38;

import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@RequiredArgsConstructor
class InternalSinkRecord {

	private final Map<String, Object> value;
}
