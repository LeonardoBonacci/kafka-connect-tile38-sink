package guru.bonacci.kafka.connect.experiments;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.beanutils.PropertyUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class Tester {

	private static final String TOKEN = "event";
	private static final String SEPARATOR = ".";
	private static final String TOKERATOR = TOKEN + SEPARATOR;
	
	public static void main(String[] args) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		String.valueOf(null);
		
		String query = "event.id is to be sub event.sub and event.foo event.nest.ed";

		Stream<String> events = Arrays.asList(query.split(" ")).stream().filter(s -> s.startsWith(TOKERATOR));

		Map<String,String> json = getJson();
		Map<String,String> parsed = 
				events.collect(Collectors.toMap(Function.identity(), ev -> {
			try {
				String prop = ev.replace(TOKERATOR, "");
				String result = ((String)PropertyUtils.getProperty(json, prop));
				return result != null ? result : ev;
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				// ignore
				return ev;
			}
		}));
		for (Map.Entry<String,String> entry : parsed.entrySet()) {
			query = query.replaceAll(entry.getKey(), entry.getValue());
		}
		
		System.out.println(query);
	}
	
	@SuppressWarnings("unchecked")
	public static Map<String, String> getJson() {
		JsonObject sinkRecord = new JsonObject();
		sinkRecord.addProperty("id", "fooid");
		sinkRecord.addProperty("sub", "foosub");
		sinkRecord.addProperty("foo", "foofoo");

		JsonObject nestRecord = new JsonObject();
		nestRecord.addProperty("ed", "fooed");
		sinkRecord.add("nest", nestRecord);

		System.out.println(sinkRecord.toString());

		Gson gson = new Gson();
		return gson.fromJson(sinkRecord.toString(), Map.class);
	}
}
