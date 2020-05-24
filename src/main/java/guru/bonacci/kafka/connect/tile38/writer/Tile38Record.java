package guru.bonacci.kafka.connect.tile38.writer;

import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@RequiredArgsConstructor
public class Tile38Record {

	private final Map<String, Object> value;
}
