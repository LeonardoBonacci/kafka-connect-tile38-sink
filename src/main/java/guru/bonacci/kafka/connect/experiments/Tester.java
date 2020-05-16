package guru.bonacci.kafka.connect.experiments;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.beanutils.PropertyUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class Tester {

	public static void main(String[] args) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {

		String query = "event.id is to be sub event.sub and event.foo event.nest.ed";
		List<String> events = Arrays.asList("event.id", "event.sub", "event.foo", "event.nest.ed");

		Map<String,String> json = getJson();
		Map<String,String> parsed = events.stream()
				.collect(Collectors.toMap(Function.identity(), ev -> {
			try {
				String prop = ev.replace("event.", "");
				String result = ((String)PropertyUtils.getProperty(json, prop));
				return result != null ? result : ev;
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				return ev;
			}
		}));
		for (Map.Entry<String,String> entry : parsed.entrySet()) {
			query = query.replaceAll(entry.getKey(), entry.getValue());
		}
		
		System.out.println(query);
	}
	
	public static Map<String, String> getJson() {
		JsonObject sinkRecord = new JsonObject();
		sinkRecord.addProperty("id", "fooid");
		sinkRecord.addProperty("sub", "foosub");
//		sinkRecord.addProperty("foo", "foofoo");

		JsonObject nestRecord = new JsonObject();
		nestRecord.addProperty("ed", "fooed");
		sinkRecord.add("nest", nestRecord);

		System.out.println(sinkRecord.toString());

		Gson gson = new Gson();
		return gson.fromJson(sinkRecord.toString(), Map.class);
	}
}
