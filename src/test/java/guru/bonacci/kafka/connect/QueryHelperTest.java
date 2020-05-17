package guru.bonacci.kafka.connect;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryHelperTest {

	@BeforeAll
	static void setup() {
	    log.info("@BeforeAll - executes once before all test methods in this class");
	}
	
	@Test
	void preparedStatement() {
		final String query = "event.id is to be sub event.sub and event.foo event.nest.ed";

		JsonObject sinkRecord = new JsonObject();
		sinkRecord.addProperty("id", "fooid");
		sinkRecord.addProperty("sub", "foosub");
		sinkRecord.addProperty("foo", "foofoo");

		JsonObject nestRecord = new JsonObject();
		nestRecord.addProperty("ed", "fooed");
		sinkRecord.add("nest", nestRecord);

		String q = new QueryHelper(query, Arrays.asList(query.split(" ")), sinkRecord).preparedStatement();
	    assertThat(q, is("fooid is to be sub foosub and foofoo fooed"));
	}

	@Test
	void commandArgs() {
		final String query = "event.id is to be sub event.sub and event.foo event.nest.ed";

		JsonObject sinkRecord = new JsonObject();
		sinkRecord.addProperty("id", "fooid");
		sinkRecord.addProperty("sub", "foosub");
		sinkRecord.addProperty("foo", "foofoo");

		JsonObject nestRecord = new JsonObject();
		nestRecord.addProperty("ed", "fooed");
		sinkRecord.add("nest", nestRecord);

		String q = new QueryHelper(query, Arrays.asList(query.split(" ")), sinkRecord).generateCommand().toCommandString();
	    assertThat(q, is("fooid is to be sub foosub and foofoo fooed"));
	}

}
