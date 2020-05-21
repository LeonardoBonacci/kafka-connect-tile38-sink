package guru.bonacci.kafka.connect.tile38;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import guru.bonacci.kafka.connect.tile38.CommandGenerator;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommandGeneratorTest {

	@BeforeAll
	static void setup() {
	    log.info("@BeforeAll - executes once before all test methods in this class");
	}
	
	@SuppressWarnings("unchecked")
	@Test
	void preparedStatement() {
		final String cmdString = "event.id is to be sub event.sub and event.foo event.nest.ed";

		JsonObject sinkRecord = new JsonObject();
		sinkRecord.addProperty("id", "fooid");
		sinkRecord.addProperty("sub", "foosub");
		sinkRecord.addProperty("foo", "foofoo");

		JsonObject nestedRecord = new JsonObject();
		nestedRecord.addProperty("ed", "fooed");
		sinkRecord.add("nest", nestedRecord);

		ImmutablePair<String, Set<String>> q = new ImmutablePair<>(
				cmdString, 
				new HashSet<String>(Arrays.asList(cmdString.split(" "))));
		Map<String, String> json = new Gson().fromJson(sinkRecord.toString(), Map.class);

		String result = CommandGenerator.from(q).preparedStatement(json);
	    assertThat(result, is("fooid is to be sub foosub and foofoo fooed"));
	}

	@SuppressWarnings("unchecked")
	@Test
	void commandArgs() {
		final String cmdString = "event.id is to be sub event.sub and event.foo event.nest.ed";

		JsonObject sinkRecord = new JsonObject();
		sinkRecord.addProperty("id", "fooid");
		sinkRecord.addProperty("sub", "foosub");
		sinkRecord.addProperty("foo", "foofoo");

		JsonObject nestRecord = new JsonObject();
		nestRecord.addProperty("ed", "fooed");
		sinkRecord.add("nest", nestRecord);

		ImmutablePair<String, Set<String>> q = new ImmutablePair<>(
				cmdString, 
				new HashSet<String>(Arrays.asList(cmdString.split(" "))));
		Map<String, String> json = new Gson().fromJson(sinkRecord.toString(), Map.class);

		String result = CommandGenerator.from(q).compile(json).toCommandString();
	    assertThat(result, is("fooid is to be sub foosub and foofoo fooed"));
	}

}
