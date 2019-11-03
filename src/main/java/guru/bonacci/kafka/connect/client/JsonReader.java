package guru.bonacci.kafka.connect.client;

import java.math.BigDecimal;
import java.math.RoundingMode;

import com.google.gson.JsonObject;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonReader {

	static String readString(JsonObject json, String fieldName) {
		try {
			return json.get(fieldName).getAsString();
		} catch (NullPointerException ne) {
			log.error("Could not read field {} from json {}", fieldName, json);
			throw ne;
		}
	}
	
	static Double readDouble(JsonObject json, String fieldName) {
		try {
			Double tooPrecise = json.get(fieldName).getAsDouble();
			return BigDecimal.valueOf(tooPrecise)
				    .setScale(3, RoundingMode.HALF_UP)
				    .doubleValue();
		} catch (NullPointerException ne) {
			log.error("Could not read field {} from json {}", fieldName, json);
			throw ne;
		}
	}
}
