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
package guru.bonacci.kafka.connect.tile38.writer;

import static com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper.write;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.Map;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.hamcrest.core.IsNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class RecordConverterTests {

	@Test
	void convert() {
		Schema schema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA)
				.field("foo", Schema.STRING_SCHEMA).field("bar", Schema.STRING_SCHEMA).build();
		Struct value = new Struct(schema).put("id", "some id").put("foo", "some foo").put("bar", "some bar");

		SinkRecord rec = write("unused", Schema.STRING_SCHEMA, "id", schema, value);
		final Struct recStruct = (Struct)rec.value();
		
		Tile38Record intRec = new RecordConverter().convert(rec);
		final Map<String, Object> intRecMap = intRec.getValue();

		intRecMap.keySet().forEach(key -> {
			assertThat(intRecMap.get(key), is(equalTo(recStruct.getString(key))));
		});
	}

	@SuppressWarnings("unchecked")
	@Test
	void convertNested() {
		Schema nestedSchema = SchemaBuilder.struct()
				.field("foo", Schema.STRING_SCHEMA)
				.field("bar", Schema.STRING_SCHEMA).build();
		Schema schema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA)
				.field("nested", nestedSchema);

		Struct nestedValue = new Struct(nestedSchema).put("foo", "some foo").put("bar", "some bar");
		Struct value = new Struct(schema).put("id", "some id").put("nested", nestedValue);

		SinkRecord rec = write("unused", Schema.STRING_SCHEMA, "id", schema, value);
		final Struct recStruct = (Struct)rec.value();
		
		Tile38Record intRec = new RecordConverter().convert(rec);
		final Map<String, Object> intRecMap = intRec.getValue();

		assertThat(intRecMap.get("id"), is(equalTo(recStruct.getString("id"))));

		final Map<String, Object> nestedMap = (Map<String, Object>)intRecMap.get("nested");
		final Struct nestedStruct = (Struct)recStruct.get("nested");
		assertThat(nestedMap, is(aMapWithSize(2)));
		assertThat(nestedMap.get("foo"), is(equalTo(nestedStruct.getString("foo"))));
		assertThat(nestedMap.get("bar"), is(equalTo(nestedStruct.getString("bar"))));
	}

	@Test
	void convertNullKey() {
		Schema schema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA)
				.field("foo", Schema.STRING_SCHEMA).field("bar", Schema.STRING_SCHEMA).build();
		Struct value = new Struct(schema).put("id", "some id").put("foo", "some foo").put("bar", "some bar");

		SinkRecord rec = new SinkRecord(
	            "unused",
	            1,
	            Schema.STRING_SCHEMA,
	            null,
	            value.schema(),
	            value,
	            91283741L,
	            1530286549123L,
	            TimestampType.CREATE_TIME
	        );


		Tile38Record intRec = new RecordConverter().convert(rec);
		assertThat(intRec.getId(), is(IsNull.nullValue()));
	}

	@Test // Will this ever happen?
	void invalidSinkRecord() {
		Schema schema = SchemaBuilder.struct()
				.field("foo", Schema.STRING_SCHEMA).field("bar", Schema.STRING_SCHEMA).build();
		Struct value = new Struct(schema).put("foo", "some foo").put("foo", "more bar");

		SinkRecord rec = write("unused", Schema.STRING_SCHEMA, "foo", schema, value);

		Assertions.assertThrows(DataException.class, () -> {
			new RecordConverter().convert(rec);
		});
	}
}
