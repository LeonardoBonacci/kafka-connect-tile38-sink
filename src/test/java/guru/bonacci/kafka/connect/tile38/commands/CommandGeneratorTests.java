package guru.bonacci.kafka.connect.tile38.commands;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;
import com.google.gson.JsonObject;

import guru.bonacci.kafka.connect.tile38.commands.CommandGenerator;
import guru.bonacci.kafka.connect.tile38.writer.RecordConverter;

public class CommandGeneratorTests {

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
		Map<String, Object> json = RecordConverter.stringToMap(sinkRecord.toString());

		String result = CommandGenerator.from(q).preparedStatement(json);
	    assertThat(result, is(equalTo("fooid is to be sub foosub and foofoo fooed")));
	}

	@Test
	void prepareInvalidStatements() {
		final String cmdString = "event.four event.one FIELD POINT event.two event.three";

		JsonObject sinkRecord = new JsonObject();
		sinkRecord.addProperty("one", "null");
		sinkRecord.addProperty("two", "%%");
		sinkRecord.addProperty("three", "@@");
		sinkRecord.addProperty("four", "$$");

		Pair<String, Set<String>> q = new ImmutablePair<>(
				cmdString, 
				Sets.newHashSet(cmdString.split(" ")));
		Map<String, Object> json = RecordConverter.stringToMap(sinkRecord.toString());

		String result = CommandGenerator.from(q).preparedStatement(json);
	    assertThat(result, is(equalTo("$$ null FIELD POINT %% @@")));
	}

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
		Map<String, Object> json = RecordConverter.stringToMap(sinkRecord.toString());

		String result = CommandGenerator.from(q).compile(json).toCommandString();
	    assertThat(result, is(equalTo("fooid is to be sub foosub and foofoo fooed")));
	}
	
	@Test
	void missingField() {
		final String cmdString = "event.id is to be event.sub";

		JsonObject sinkRecord = new JsonObject();
		sinkRecord.addProperty("id", "fooid");

		Pair<String, Set<String>> q = new ImmutablePair<>(
				cmdString, 
				Sets.newHashSet(cmdString.split(" ")));
		Map<String, Object> json = RecordConverter.stringToMap(sinkRecord.toString());

		Assertions.assertThrows(DataException.class, () -> {
			CommandGenerator.from(q).compile(json);
		});
	}

	@Test
	void nesting() {
		final String cmdString = "foo event.id POINT event.nested.foo event.nested.bar";

		JsonObject nestedRecord = new JsonObject();
		nestedRecord.addProperty("foo", "some foo");
		nestedRecord.addProperty("bar", "some bar");

		JsonObject sinkRecord = new JsonObject();
		sinkRecord.addProperty("id", "fooid");
		sinkRecord.add("nested", nestedRecord);
		
		Pair<String, Set<String>> q = new ImmutablePair<>(
				cmdString, 
				Sets.newHashSet(cmdString.split(" ")));
		Map<String, Object> json = RecordConverter.stringToMap(sinkRecord.toString());

		String result = CommandGenerator.from(q).preparedStatement(json);
		assertThat(result, is(equalTo("foo fooid POINT some foo some bar")));
	}

}
