package guru.bonacci.kafka.connect;

class Version {

	
	public static String getVersion() {
		try {
			return Version.class.getPackage().getImplementationVersion();
		} catch (Exception ex) {
			return "0.0.0.0";
		}
	}
}
