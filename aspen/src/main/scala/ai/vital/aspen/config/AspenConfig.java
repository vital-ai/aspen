package ai.vital.aspen.config;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

import ai.vital.vitalsigns.VitalSigns;

/**
 * provides access to aspen config object
 * @author Derek
 *
 */
public class AspenConfig {

	private static AspenConfig singleton;
	
	private final static Logger log = LoggerFactory.getLogger(AspenConfig.class);
	
	private String jobServerURL;
	
	private String datesetsLocation;
	
	public String getJobServerURL() {
		return jobServerURL;
	}

	public void setJobServerURL(String jobServerURL) {
		this.jobServerURL = jobServerURL;
	}
	
	public String getDatesetsLocation() {
		return datesetsLocation;
	}

	public void setDatesetsLocation(String datesetsLocation) {
		this.datesetsLocation = datesetsLocation;
	}

	public static AspenConfig get() {
		
		if(singleton == null) {
			
			synchronized (AspenConfig.class) {
				
				if(singleton == null) {
					
					try {
						
						singleton = createInstance();
						
					} catch(Exception e) {
						
						log.warn("Couldn't access aspen config file, using default aspen config instead");
						singleton = new AspenConfig();
						
					}
					
				}
				
			}
			
		}
		
		return singleton;
		
	}

	static AspenConfig createInstance() throws Exception {

		String vitalHome = System.getenv(VitalSigns.VITAL_HOME);
		
		if(vitalHome == null ||vitalHome.isEmpty()) throw new Exception("No " + VitalSigns.VITAL_HOME + " environment variable");
		
		File configFile = new File(vitalHome, "vital-config/aspen/aspen.config");
		
		if(!configFile.exists()) throw new Exception("Aspen config file not found: " + configFile.getAbsolutePath());
		
		if(!configFile.isFile()) throw new Exception("Aspen config path is not a file: " + configFile.getAbsolutePath());
		
		Config cfg = ConfigFactory.parseFile(configFile);
		
		AspenConfig a = new AspenConfig();

		try {
			a.jobServerURL = cfg.getString("jobserverURL");
		} catch(ConfigException.Missing ex) {
			log.warn("No jobserverURL config param");
		}
		
		try {
			a.datesetsLocation = cfg.getString("datesetsLocation");
		} catch(ConfigException.Missing ex) {
			log.warn("No datesetsLocation config param");
		}
		
		return a;
	}
	
}
