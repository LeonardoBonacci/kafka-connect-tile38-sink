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

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonMap;

import java.util.Map;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RecordConverter {

	private static final Converter JSON_CONVERTER;
	private static final ObjectMapper MAPPER;
	
	static {
		JSON_CONVERTER = new JsonConverter();
		JSON_CONVERTER.configure(singletonMap("schemas.enable", "false"), false);
		
		MAPPER = new ObjectMapper();
	}

	
	public final Tile38Record convert(SinkRecord sinkRecord) {
		return new Tile38Record(
					sinkRecord.topic(), 
					convertKey(sinkRecord), 
					convertValue(sinkRecord));
	}

	/**
	 * Expecting a String key
	 */
	private String convertKey(SinkRecord record) {
		final Object key = record.key();
		return key != null ? key.toString() : null; 
	}

	public Map<String, Object> convertValue(SinkRecord record) {
		 // Tombstone records don't need to be converted
		if (record.value() == null) {
			return null;
		}

		return jsonStringToMap(getValue(record));
	}

	// visible for testing
	@SuppressWarnings("unchecked")
	public Map<String, Object> jsonStringToMap(String jsonString) {
		if (jsonString == null) {
			return null;
		}

		try {
			return MAPPER.readValue(jsonString, Map.class);
		} catch (JsonProcessingException e) {
			throw new DataException(format("Problems parsing json %s", jsonString), e);
		}
	}

	private String getValue(SinkRecord record) {
		 // Tombstone records don't need to be converted
		if (record.value() == null) {
			return null;
		}

		byte[] rawJsonPayload = JSON_CONVERTER.fromConnectData(record.topic(), 
				record.valueSchema(), 
				record.value());
		return new String(rawJsonPayload, UTF_8);
	}
}
