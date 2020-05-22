package guru.bonacci.kafka.connect.tile38;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.Sets;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class CommandGeneratorTests {

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

		Pair<String, Set<String>> q = new ImmutablePair<>(
				cmdString, 
				Sets.newHashSet(cmdString.split(" ")));
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

		Pair<String, Set<String>> q = new ImmutablePair<>(
				cmdString, 
				Sets.newHashSet(cmdString.split(" ")));
		Map<String, String> json = new Gson().fromJson(sinkRecord.toString(), Map.class);

		String result = CommandGenerator.from(q).compile(json).toCommandString();
	    assertThat(result, is("fooid is to be sub foosub and foofoo fooed"));
	}
}
