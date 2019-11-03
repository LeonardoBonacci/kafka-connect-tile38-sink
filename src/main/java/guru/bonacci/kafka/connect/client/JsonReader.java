package guru.bonacci.kafka.connect.client;

import java.math.BigDecimal;
import java.math.RoundingMode;

import com.google.gson.JsonObject;

public class JsonReader {

	static String readOptionalString(JsonObject json, String fieldName) {
		try {
			return readString(json, fieldName);
		} catch (ClassCastException | IllegalStateException e) {
			//TODO log something?
			return null;
		} 
	}

	static String readString(JsonObject json, String fieldName) {
		return json.get(fieldName).getAsString();
	}
	
	static Double readOptionalDouble(JsonObject json, String fieldName) {
		try {
			return readDouble(json, fieldName);
		} catch (ClassCastException | IllegalStateException e) {
			//TODO log something?
			return null;
		} 
	}

	static Double readDouble(JsonObject json, String fieldName) {
		Double tooPrecise = json.get(fieldName).getAsDouble();
		return BigDecimal.valueOf(tooPrecise)
			    .setScale(3, RoundingMode.HALF_UP)
			    .doubleValue();
	}
}
