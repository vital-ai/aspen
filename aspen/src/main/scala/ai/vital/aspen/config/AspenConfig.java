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
	
	private String datasetsLocation;
	
	//location for temp remote files, such as when using local files and processing them remotely seamlessly
	private String tempRemoteLocation;
	
	public String getJobServerURL() {
		return jobServerURL;
	}

	public void setJobServerURL(String jobServerURL) {
		this.jobServerURL = jobServerURL;
	}
	
	public String getDatasetsLocation() {
		return datasetsLocation;
	}

	public void setDatasetsLocation(String datasetsLocation) {
		this.datasetsLocation = datasetsLocation;
	}

	public String getTempRemoteLocation() {
		return tempRemoteLocation;
	}

	public void setTempRemoteLocation(String tempRemoteLocation) {
		this.tempRemoteLocation = tempRemoteLocation;
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
			a.datasetsLocation = cfg.getString("datasetsLocation");
		} catch(ConfigException.Missing ex) {
			log.warn("No datasetsLocation config param");
		}
		
		try {
			a.tempRemoteLocation = cfg.getString("tempRemoteLocation");
		} catch(ConfigException.Missing ex) {
			log.warn("No tempRemoteLocation config param");
		}
		
		return a;
	}
	
}
