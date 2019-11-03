package guru.bonacci.kafka.connect;

import com.google.gson.JsonObject;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class Record {

	private final JsonObject json;
}
