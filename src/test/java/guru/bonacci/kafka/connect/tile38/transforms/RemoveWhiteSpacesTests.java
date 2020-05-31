package guru.bonacci.kafka.connect.tile38.transforms;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

public class RemoveWhiteSpacesTests {

	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void untouchedString() {
		final String id = "fooid";
		final String other = "something else";
		
		Schema schema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).field("other", Schema.STRING_SCHEMA).build();
		Struct value = new Struct(schema).put("id", id).put("other", other);

		final SinkRecord sinkRecord = new SinkRecord("some-topic", 1, null, null, schema, value, 0);

		RemoveWhiteSpaces<SinkRecord> transformer = new RemoveWhiteSpaces<>();
		transformer.configure(ImmutableMap.of("field", "id"));
		SinkRecord transformedRecord = transformer.apply(sinkRecord);
		transformer.close();

		Map<String, Object> trValues = (Map)transformedRecord.value();
		assertThat(trValues.get("id"), is(equalTo(value.get("id"))));
		assertThat(trValues.get("other"), is(equalTo(value.get("other"))));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void transformStringInSchemaLess() {
		final String id = " foo id ";
		final String other = "something else";
		
		final Map<String, Object> value = ImmutableMap.of("id", id, "other", other);		

		final SinkRecord sinkRecord = new SinkRecord("some-topic", 1, null, null, null, value, 0);

		RemoveWhiteSpaces<SinkRecord> transformer = new RemoveWhiteSpaces<>();
		transformer.configure(ImmutableMap.of("field", "id"));
		SinkRecord transformedRecord = transformer.apply(sinkRecord);
		transformer.close();

		Map<String, Object> trValues = (Map)transformedRecord.value();
		assertThat(trValues.get("id"), is(equalTo("fooid")));
		assertThat(trValues.get("other"), is(equalTo(value.get("other"))));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void transformNestedInSchemaLess() {
		final String id = " foo id ";
		final String other = "something else";

		final Map<String, Object> nested = ImmutableMap.of("id", id);		
		final Map<String, Object> value = ImmutableMap.of("other", other, "nest", nested);		

		final SinkRecord sinkRecord = new SinkRecord("some-topic", 1, null, null, null, value, 0);

		RemoveWhiteSpaces<SinkRecord> transformer = new RemoveWhiteSpaces<>();
		transformer.configure(ImmutableMap.of("field", "nest.id"));
		SinkRecord transformedRecord = transformer.apply(sinkRecord);
		transformer.close();

		Map<String, Object> trValues = (Map)transformedRecord.value();
		Map<String, Object> trNested = (Map)trValues.get("nest");
		assertThat(trNested.get("id"), is(equalTo("fooid")));
		assertThat(trValues.get("other"), is(equalTo(value.get("other"))));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void transformStringInStruct() {
		final String id = " foo id ";
		final String other = "something else";
		
		Schema schema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).field("other", Schema.STRING_SCHEMA).build();
		Struct value = new Struct(schema).put("id", id).put("other", other);

		final SinkRecord sinkRecord = new SinkRecord("some-topic", 1, null, null, schema, value, 0);

		RemoveWhiteSpaces<SinkRecord> transformer = new RemoveWhiteSpaces<>();
		transformer.configure(ImmutableMap.of("field", "id"));
		SinkRecord transformedRecord = transformer.apply(sinkRecord);
		transformer.close();

		Map<String, Object> trValues = (Map)transformedRecord.value();
		assertThat(trValues.get("id"), is(equalTo("fooid")));
		assertThat(trValues.get("other"), is(equalTo(value.get("other"))));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void transformDouble() {
		final String id = "fooid";
		final Float other = Float.valueOf(0.21f);
		
		Schema schema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).field("other", Schema.FLOAT32_SCHEMA).build();
		Struct value = new Struct(schema).put("id", id).put("other", other);

		final SinkRecord sinkRecord = new SinkRecord("some-topic", 1, null, null, schema, value, 0);

		RemoveWhiteSpaces<SinkRecord> transformer = new RemoveWhiteSpaces<>();
		transformer.configure(ImmutableMap.of("field", "other"));
		SinkRecord transformedRecord = transformer.apply(sinkRecord);
		transformer.close();

		Map<String, Object> trValues = (Map)transformedRecord.value();
		assertThat(trValues.get("id"), is(equalTo("fooid")));
		assertThat(trValues.get("other"), is(equalTo(value.get("other").toString())));
	}

	@Test
	public void tombstone() {
		final Map<String, Object> key = new HashMap<>();
		key.put("Id", 1234);

		final Map<String, Object> value = null;

		final SinkRecord sinkRecord = new SinkRecord("some-topic", 1, null, key, null, value, 0);

		RemoveWhiteSpaces<SinkRecord> transformer = new RemoveWhiteSpaces<>();
		SinkRecord transformedRecord = transformer.apply(sinkRecord);
		transformer.close();

		assertThat(transformedRecord, is(equalTo(sinkRecord)));
	}
}