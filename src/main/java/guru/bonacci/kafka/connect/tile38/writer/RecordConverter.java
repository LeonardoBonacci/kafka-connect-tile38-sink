package guru.bonacci.kafka.connect.tile38.writer;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonMap;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RecordConverter {

	private static final Converter JSON_CONVERTER;

	static {
		JSON_CONVERTER = new JsonConverter();
		JSON_CONVERTER.configure(singletonMap("schemas.enable", "false"), false);
	}

	
	public static Tile38Record toInternalSinkRecord(SinkRecord sinkRecord) {
		return new Tile38Record(convertData(sinkRecord));
	}

	private static Map<String, Object> convertData(SinkRecord record) {
		return stringToMap(getPayload(record));
	}

	// visible for testing
	@SuppressWarnings("unchecked")
	public static Map<String, Object> stringToMap(String recordAsString) {
		try {
			return new Gson().fromJson(recordAsString, Map.class);
		} catch (JsonSyntaxException e) {
			String warning = format("Problems parsing record value %s", recordAsString);
			log.warn(warning);
			throw new DataException(warning, e);
		}
	}

	private static String getPayload(SinkRecord record) {
		if (record.value() == null) {
			return null;
		}

		Schema schema = record.valueSchema();
		Object value = record.value();

		byte[] rawJsonPayload = JSON_CONVERTER.fromConnectData(record.topic(), schema, value);
		return new String(rawJsonPayload, UTF_8);
	}
}
