package guru.bonacci.kafka.connect;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryHelperTest {

	@BeforeAll
	static void setup() {
	    log.info("@BeforeAll - executes once before all test methods in this class");
	}
	
	@SuppressWarnings("unchecked")
	@Test
	void preparedStatement() {
		final String queryString = "event.id is to be sub event.sub and event.foo event.nest.ed";

		JsonObject sinkRecord = new JsonObject();
		sinkRecord.addProperty("id", "fooid");
		sinkRecord.addProperty("sub", "foosub");
		sinkRecord.addProperty("foo", "foofoo");

		JsonObject nestedRecord = new JsonObject();
		nestedRecord.addProperty("ed", "fooed");
		sinkRecord.add("nest", nestedRecord);

		ImmutablePair<String, List<String>> q = new ImmutablePair<>(queryString, Arrays.asList(queryString.split(" ")));
		Map<String, String> json = new Gson().fromJson(sinkRecord.toString(), Map.class);

		String result = new QueryHelper(q, json).preparedStatement();
	    assertThat(result, is("fooid is to be sub foosub and foofoo fooed"));
	}

	@SuppressWarnings("unchecked")
	@Test
	void commandArgs() {
		final String queryString = "event.id is to be sub event.sub and event.foo event.nest.ed";

		JsonObject sinkRecord = new JsonObject();
		sinkRecord.addProperty("id", "fooid");
		sinkRecord.addProperty("sub", "foosub");
		sinkRecord.addProperty("foo", "foofoo");

		JsonObject nestRecord = new JsonObject();
		nestRecord.addProperty("ed", "fooed");
		sinkRecord.add("nest", nestRecord);

		ImmutablePair<String, List<String>> q = new ImmutablePair<>(queryString, Arrays.asList(queryString.split(" ")));
		Map<String, String> json = new Gson().fromJson(sinkRecord.toString(), Map.class);

		String result = new QueryHelper(q, json).generateCommand().toCommandString();
	    assertThat(result, is("fooid is to be sub foosub and foofoo fooed"));
	}

}
