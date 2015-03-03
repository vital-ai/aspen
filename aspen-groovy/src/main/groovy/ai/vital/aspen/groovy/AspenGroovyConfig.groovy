package ai.vital.aspen.groovy

/**
 * A singleton containing aspen groovy runtime configuration.
 * Default settings match single-jar runtime environment with embedded resource.
 *  
 * @author Derek
 *
 */
class AspenGroovyConfig {

	private static AspenGroovyConfig singleton
	
	public static AspenGroovyConfig get() {
		if(singleton == null) {
			synchronized (AspenGroovyConfig.class) {
				if(singleton == null) {
					singleton = new AspenGroovyConfig()
				}
			}
		}
		return singleton
	}
	
	private AspenGroovyConfig(){}
	
	/**
	 * By default all resources are loaded from classpath /resources/ directory
	 */
	boolean loadResourcesFromClasspath = true
	
}
