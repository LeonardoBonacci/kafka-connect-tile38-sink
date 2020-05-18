package guru.bonacci.kafka.connect;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;

import com.google.gson.Gson;

public class DataConverter {

	private static final Converter JSON_CONVERTER;

	static {
		JSON_CONVERTER = new JsonConverter();
		JSON_CONVERTER.configure(Collections.singletonMap("schemas.enable", "false"), false);
	}

	public static InternalSinkRecord toInternalSinkRecord(SinkRecord sinkRecord) {
		return new InternalSinkRecord(convertData(sinkRecord));
	}

	@SuppressWarnings("unchecked")
	private static Map<String, String> convertData(SinkRecord record) {
		String recordAsString = getPayload(record);
		return new Gson().fromJson(recordAsString, Map.class);
	}

	private static String getPayload(SinkRecord record) {
		if (record.value() == null) {
			return null;
		}

		Schema schema = record.valueSchema();
		Object value = record.value();

		byte[] rawJsonPayload = JSON_CONVERTER.fromConnectData(record.topic(), schema, value);
		return new String(rawJsonPayload, StandardCharsets.UTF_8);
	}
}
