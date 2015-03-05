package ai.vital.aspen.groovy

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.typesafe.config.Config
import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigFactory

/**
 * A singleton containing aspen groovy runtime configuration.
 * Default settings match single-jar runtime environment with embedded resource.
 *  
 * @author Derek
 *
 */
class AspenGroovyConfig {

	private static AspenGroovyConfig singleton
	
	private final static org.slf4j.Logger log = LoggerFactory.getLogger(AspenGroovyConfig.class)
	
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
	
	private AspenGroovyConfig(){
		
		String path = "/resources/sample-aspen-groovy.config" 
		
		log.debug("Initilizing Aspen-Groovy config singleton from classpath resource ${path}")
		
		InputStream stream = null
		
		try {
			stream = AspenGroovyConfig.class.getResourceAsStream(path)
			if(stream != null) {
				this.configure(stream)
			} else {
				log.warn("aspen config not found in classpath: ${path}")
			}
		} catch(Exception e) {
			log.error("Error when configuring aspen groovy from classpath resource: ${path} - ${e.localizedMessage}")
		} finally {
			IOUtils.closeQuietly(stream)
		}
		
	}
	
	/**
	 * By default all resources are loaded from classpath /resources/ directory
	 */
	boolean loadResourcesFromClasspath = true
	
	/**
	 * Loads settings from given HOCON inputStream
	 * @param inputStream
	 */
	public void configure(InputStream hoconInputStream) {
		
		Config cfg = ConfigFactory.parseReader(new InputStreamReader(hoconInputStream, 'UTF-8')) 
		
		try {
			this.loadResourcesFromClasspath = cfg.getBoolean("loadResourcesFromClasspath")
			log.info("loadResourcesFromClasspath: {}", this.loadResourcesFromClasspath)
		} catch(ConfigException.Missing ex) {
			log.warn("Missing loadResourcesFromClasspath")
		}
		
	}
	
	public void reset() {
		loadResourcesFromClasspath = true
	}
	
}
