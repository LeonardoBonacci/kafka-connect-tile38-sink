package guru.bonacci.kafka.connect.tile38;

import static com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper.write;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class DataConverterTests {

	@Test
	void convert() {
		Schema schema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA)
				.field("foo", Schema.STRING_SCHEMA).field("bar", Schema.STRING_SCHEMA).build();
		Struct value = new Struct(schema).put("id", "some id").put("foo", "some foo").put("bar", "some bar");

		SinkRecord rec = write("unused", Schema.STRING_SCHEMA, "id", schema, value);
		final Struct recStruct = (Struct)rec.value();
		
		InternalSinkRecord intRec = DataConverter.toInternalSinkRecord(rec);
		final Map<String, String> intRecMap = intRec.getValue();

		intRecMap.keySet().forEach(key -> {
			assertThat(intRecMap.get(key), is(recStruct.getString(key)));
		});
	}

	@Test // Will this ever happen?
	void invalidSinkRecord() {
		Schema schema = SchemaBuilder.struct()
				.field("foo", Schema.STRING_SCHEMA).field("bar", Schema.STRING_SCHEMA).build();
		Struct value = new Struct(schema).put("foo", "some foo").put("foo", "more bar");

		SinkRecord rec = write("unused", Schema.STRING_SCHEMA, "foo", schema, value);

		Assertions.assertThrows(DataException.class, () -> {
			DataConverter.toInternalSinkRecord(rec);
		});
	}
}
