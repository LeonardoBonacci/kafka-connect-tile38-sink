package guru.bonacci.kafka.connect.tile38.commands;

import static com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper.write;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;

import guru.bonacci.kafka.connect.tile38.writer.RecordConverter;
import guru.bonacci.kafka.connect.tile38.writer.Tile38Record;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;

public class CommandGeneratorTests {

	@Test
	void preparedStatement() {
		final String cmdString = "something event.id is to be sub event.sub and event.foo event.nest.ed";

		JsonObject sinkRecord = new JsonObject();
		sinkRecord.addProperty("id", "fooid");
		sinkRecord.addProperty("sub", "foosub");
		sinkRecord.addProperty("foo", "foofoo");

		JsonObject nestedRecord = new JsonObject();
		nestedRecord.addProperty("ed", "fooed");
		sinkRecord.add("nest", nestedRecord);

		CommandWrapper cmd = CommandWrapper.from(cmdString); 
		Map<String, Object> json = new RecordConverter().jsonStringToMap(sinkRecord.toString());

		String result = CommandGenerator.from(cmd).preparedStatement(json);
	    assertThat(result, is(equalTo("something fooid is to be sub foosub and foofoo fooed")));
	}

	@Test
	void prepareInvalidStatements() {
		final String cmdString = "thekey event.four event.one FIELD POINT event.two event.three";

		JsonObject sinkRecord = new JsonObject();
		sinkRecord.addProperty("one", "null");
		sinkRecord.addProperty("two", "%%");
		sinkRecord.addProperty("three", "@@");
		sinkRecord.addProperty("four", "$$");

		CommandWrapper cmd = CommandWrapper.from(cmdString); 
		Map<String, Object> json = new RecordConverter().jsonStringToMap(sinkRecord.toString());

		String result = CommandGenerator.from(cmd).preparedStatement(json);
	    assertThat(result, is(equalTo("thekey $$ null FIELD POINT %% @@")));
	}

	@Test
	void compileToSET() {
		final String cmdString = "bla event.id is to be sub nest.event.foo and nest.event.bar more";

		Schema nestedSchema = SchemaBuilder.struct()
				.field("foo", Schema.STRING_SCHEMA)
				.field("bar", Schema.STRING_SCHEMA).build();
		Schema schema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA)
				.field("nested", nestedSchema);

		Struct nestedValue = new Struct(nestedSchema).put("foo", "some foo").put("bar", "some bar");
		Struct value = new Struct(schema).put("id", "some id").put("nested", nestedValue);

		SinkRecord rec = write("unused", Schema.STRING_SCHEMA, "id", schema, value);

		Tile38Record internalRecord = new RecordConverter().convert(rec);

		Pair<CommandType, CommandArgs<String, String>> result = CommandGenerator.from(
				CommandWrapper.from(cmdString)).compile(internalRecord);

	    assertThat(result.getLeft(), is(equalTo(CommandType.SET)));
	    assertThat(result.getRight().toCommandString(), is(equalTo("bla some id is to be sub nest.event.foo and nest.event.bar more")));
	}

	@Test
	void tombstoneToDELETE() {
		final String cmdString = "bla event.id is to be sub nest.event.foo and nest.event.bar more";

		SinkRecord rec = new SinkRecord(
	            "unused",
	            1,
	            Schema.STRING_SCHEMA,
	            "thekey",
	            null,
	            null,
	            91283741L,
	            1530286549123L,
	            TimestampType.CREATE_TIME
	        );


		Tile38Record internalRecord = new RecordConverter().convert(rec);

		Pair<CommandType, CommandArgs<String, String>> result = CommandGenerator.from(
				CommandWrapper.from(cmdString)).compile(internalRecord);

	    assertThat(result.getLeft(), is(equalTo(CommandType.DEL)));
	    assertThat(result.getRight().toCommandString(), is(equalTo("bla thekey")));
	}

	@Test
	void missingField() {
		final String cmdString = "qqqq event.id is to be event.sub";

		Schema schema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA);
		Struct value = new Struct(schema).put("id", "some id");

		SinkRecord rec = write("unused", Schema.STRING_SCHEMA, "id", schema, value);

		Tile38Record internalRecord = new RecordConverter().convert(rec);

		
		Assertions.assertThrows(DataException.class, () -> {
			CommandGenerator.from(CommandWrapper.from(cmdString)).compile(internalRecord);
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
		
		CommandWrapper cmd = CommandWrapper.from(cmdString); 
		Map<String, Object> json = new RecordConverter().jsonStringToMap(sinkRecord.toString());

		String result = CommandGenerator.from(cmd).preparedStatement(json);
		assertThat(result, is(equalTo("foo fooid POINT some foo some bar")));
	}

}
