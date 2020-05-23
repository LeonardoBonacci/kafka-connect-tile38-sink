package guru.bonacci.kafka.connect.tile38;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class Version {

	
	public static String getVersion() {
		try {
			return Version.class.getPackage().getImplementationVersion();
		} catch (Exception ex) {
			log.error("Exception thrown while getting error", ex);
			return "0.0.0.0";
		}
	}
}
