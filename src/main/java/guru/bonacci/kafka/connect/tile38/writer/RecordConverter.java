	package guru.bonacci.kafka.connect.tile38.writer;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonMap;

import java.util.Map;

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

	
	public final Tile38Record convert(SinkRecord sinkRecord) {
		return new Tile38Record(convertKey(sinkRecord), convertValue(sinkRecord));
	}

	/**
	 * Expecting a String key
	 */
	private String convertKey(SinkRecord record) {
		final Object key = record.key();
		return key != null ? key.toString() : null; 
	}

	private Map<String, Object> convertValue(SinkRecord record) {
		 // Tombstone records don't need to be converted
		if (record.value() == null) {
			return null;
		}

		return jsonStringToMap(getValue(record));
	}

	// visible for testing
	@SuppressWarnings("unchecked")
	public Map<String, Object> jsonStringToMap(String recordAsString) {
		 // Tombstone records don't need to be converted
		if (recordAsString == null) {
			return null;
		}

		try {
			return new Gson().fromJson(recordAsString, Map.class);
		} catch (JsonSyntaxException e) {
			String warning = format("Problems parsing record value %s", recordAsString);
			log.warn(warning);
			throw new DataException(warning, e);
		}
	}

	private String getValue(SinkRecord record) {
		 // Tombstone records don't need to be converted
		if (record.value() == null) {
			return null;
		}

		byte[] rawJsonPayload = JSON_CONVERTER.fromConnectData(record.topic(), 
				record.valueSchema(), 
				record.value());
		return new String(rawJsonPayload, UTF_8);
	}
}
