/**
 * 	Copyright 2020 Jeffrey van Helden (aabcehmu@mailfence.com)
 *	
 *	Licensed under the Apache License, Version 2.0 (the "License");
 *	you may not use this file except in compliance with the License.
 *	You may obtain a copy of the License at
 *	
 *	    http://www.apache.org/licenses/LICENSE-2.0
 *	
 *	Unless required by applicable law or agreed to in writing, software
 *	distributed under the License is distributed on an "AS IS" BASIS,
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *	See the License for the specific language governing permissions and
 *	limitations under the License.
 */
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
		final String topic = "some-topic";
		final String id = "fooid";
		final String other = "something else";
		
		Schema schema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).field("other", Schema.STRING_SCHEMA).build();
		Struct value = new Struct(schema).put("id", id).put("other", other);

		final SinkRecord sinkRecord = new SinkRecord(topic, 1, null, null, schema, value, 0);

		RemoveWhiteSpaces<SinkRecord> transformer = new RemoveWhiteSpaces<>();
		transformer.configure(ImmutableMap.of("field", "id", "topic", topic));
		SinkRecord transformedRecord = transformer.apply(sinkRecord);
		transformer.close();

		Map<String, Object> trValues = (Map)transformedRecord.value();
		assertThat(trValues.get("id"), is(equalTo(value.get("id"))));
		assertThat(trValues.get("other"), is(equalTo(value.get("other"))));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void transformStringInSchemaLess() {
		final String topic = "some-topic";
		final String id = " foo id ";
		final String other = "something else";
		
		final Map<String, Object> value = ImmutableMap.of("id", id, "other", other);		

		final SinkRecord sinkRecord = new SinkRecord(topic, 1, null, null, null, value, 0);

		RemoveWhiteSpaces<SinkRecord> transformer = new RemoveWhiteSpaces<>();
		transformer.configure(ImmutableMap.of("field", "id", "topic", topic));
		SinkRecord transformedRecord = transformer.apply(sinkRecord);
		transformer.close();

		Map<String, Object> trValues = (Map)transformedRecord.value();
		assertThat(trValues.get("id"), is(equalTo("fooid")));
		assertThat(trValues.get("other"), is(equalTo(value.get("other"))));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void transformNestedInSchemaLess() {
		final String topic = "some-topic";
		final String id = " foo id ";
		final String other = "something else";

		final Map<String, Object> nested = ImmutableMap.of("id", id);		
		final Map<String, Object> value = ImmutableMap.of("other", other, "nest", nested);		

		final SinkRecord sinkRecord = new SinkRecord(topic, 1, null, null, null, value, 0);

		RemoveWhiteSpaces<SinkRecord> transformer = new RemoveWhiteSpaces<>();
		transformer.configure(ImmutableMap.of("field", "nest.id", "topic", topic));
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
		final String topic = "some-topic";
		final String id = " foo id ";
		final String other = "something else";
		
		Schema schema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).field("other", Schema.STRING_SCHEMA).build();
		Struct value = new Struct(schema).put("id", id).put("other", other);

		final SinkRecord sinkRecord = new SinkRecord(topic, 1, null, null, schema, value, 0);

		RemoveWhiteSpaces<SinkRecord> transformer = new RemoveWhiteSpaces<>();
		transformer.configure(ImmutableMap.of("field", "id", "topic", topic));
		SinkRecord transformedRecord = transformer.apply(sinkRecord);
		transformer.close();

		Map<String, Object> trValues = (Map)transformedRecord.value();
		assertThat(trValues.get("id"), is(equalTo("fooid")));
		assertThat(trValues.get("other"), is(equalTo(value.get("other"))));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void transformDouble() {
		final String topic = "some-topic";
		final String id = "fooid";
		final Float other = Float.valueOf(0.21f);
		
		Schema schema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).field("other", Schema.FLOAT32_SCHEMA).build();
		Struct value = new Struct(schema).put("id", id).put("other", other);

		final SinkRecord sinkRecord = new SinkRecord(topic, 1, null, null, schema, value, 0);

		RemoveWhiteSpaces<SinkRecord> transformer = new RemoveWhiteSpaces<>();
		transformer.configure(ImmutableMap.of("field", "other", "topic", topic));
		SinkRecord transformedRecord = transformer.apply(sinkRecord);
		transformer.close();

		Map<String, Object> trValues = (Map)transformedRecord.value();
		assertThat(trValues.get("id"), is(equalTo("fooid")));
		assertThat(trValues.get("other"), is(equalTo(value.get("other").toString())));
	}

	@Test
	public void tombstone() {
		final String topic = "some-topic";
		final Map<String, Object> key = new HashMap<>();
		key.put("Id", 1234);

		final Map<String, Object> value = null;

		final SinkRecord sinkRecord = new SinkRecord(topic, 1, null, key, null, value, 0);

		RemoveWhiteSpaces<SinkRecord> transformer = new RemoveWhiteSpaces<>();
		transformer.configure(ImmutableMap.of("field", "other", "topic", topic));
		SinkRecord transformedRecord = transformer.apply(sinkRecord);
		transformer.close();

		assertThat(transformedRecord, is(equalTo(sinkRecord)));
	}
}