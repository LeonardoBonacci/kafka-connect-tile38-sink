package guru.bonacci.kafka.connect.utils;

import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;

import com.google.gson.Gson;

import guru.bonacci.kafka.connect.InternalSinkRecord;

public class ConverterFunctions {

	public static InternalSinkRecord InternalSinkRecord(SinkRecord sinkRecord) {
		return new InternalSinkRecord(convertData(sinkRecord));
	}
	
	@SuppressWarnings("unchecked")
	private static Map<String, String> convertData(SinkRecord data) {
	    String recordAsString = String.valueOf(data.value());
	    return new Gson().fromJson(recordAsString, Map.class);
    }
}

