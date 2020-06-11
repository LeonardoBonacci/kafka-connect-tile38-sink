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
package guru.bonacci.kafka.connect.tile38;

public class Constants {

	private static final String TOKEN = "event";
	private static final String SEPARATOR = ".";
	public static final String TOKERATOR = TOKEN + SEPARATOR;

	public static final String SET_TERM = "SET";
	
	public static final String COMMAND_PREFIX = "tile38.topic.";
	public static final String EXPIRE_SUFFIX = ".expire";
}
