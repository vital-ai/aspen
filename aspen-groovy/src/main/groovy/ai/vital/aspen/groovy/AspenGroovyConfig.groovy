package ai.vital.aspen.groovy

import javax.naming.ConfigurationException;

import java.io.InputStream
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import ai.vital.aspen.groovy.nlp.models.OpenNLPModel;

import com.typesafe.config.Config
import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValue;

import java.util.Map
import java.util.HashMap
import java.util.Map.Entry

/**
 * A singleton containing aspen groovy runtime configuration.
 * Default settings match single-jar runtime environment with embedded resource.
 *  
 * @author Derek
 *
 */
class AspenGroovyConfig {

	public final static String FS_S3N_AWSACCESSKEYID = "fs.s3n.awsAccessKeyId";
	
	public final static String FS_S3N_AWSSECRETACCESSKEY = "fs.s3n.awsSecretAccessKey";
	
	private static AspenGroovyConfig singleton
	
	private final static org.slf4j.Logger log = LoggerFactory.getLogger(AspenGroovyConfig.class)
	
	//empty hadoop configuration
	private Configuration hadoopConfiguration = new Configuration(false)
	
	private String awsAccessKeyId
	
	private String awsSecretAccessKey
	
	private List<String> hadoopXmlFiles = []
	
	
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

		for(String t : OpenNLPModel.validTypes) {
			modelType2Class.put(t, OpenNLPModel.class.canonicalName)
		}
				
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
	public boolean loadResourcesFromClasspath = true
	
	/**
	 * Required when loadResourcesFromClasspath==false, default "./resources"
	 */
	public String resourcesDir = "./resources"
	
	
	//hdfs:///datasets/--name--/--name--.seq
	public String datesetsLocation = "hdfs://127.0.0.1:8020/datasets/"
	
	
	//by default 
	public Map<String, String> modelType2Class = new HashMap<String, String>();
	
	
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
			log.warn("Missing loadResourcesFromClasspath - using default true")
			this.loadResourcesFromClasspath = true
		}
		
		try {
			this.resourcesDir = cfg.getString("resourcesDir") 
		} catch(ConfigException.Missing ex) {
			log.warn("Missing resourceDir - using default \"./resources\"")
			this.resourcesDir = "./resources"
		}
		
		try {
			
			Config modelsMapping = cfg.getConfig('models-mapping')
			
			for(Entry<String, ConfigValue> e : modelsMapping.entrySet() ) {
				
				this.modelType2Class.put(e.getKey(), (String) e.getValue().unwrapped())
				
				
			}
			
			
			
		} catch(ConfigException.Missing ex) {
			log.warn("No models-mapping config section, only default mappings will be used")
		}
		
		try {
			
			this.datesetsLocation = cfg.getString("datesetsLocation")
			
		} catch(ConfigException.Missing ex) {
			datesetsLocation = "hdfs://127.0.0.1:8020/datasets/"
			log.warn("Missing datesetsLocation - using default ${this.datesetsLocation}")
		}
		
	}
	
	public void reset() {
		loadResourcesFromClasspath = true
		resourcesDir = "./resources"
	}
	
	
	public void setHadoopConfigXML( List<String> hadoopXMLFiles) {
		List currentFiles = this.hadoopXmlFiles
		this.hadoopXmlFiles = hadoopXMLFiles != null ? hadoopXMLFiles : []
		try {
			reloadHadoopConfig()
		} catch(Exception e) {
			//restore
			this.hadoopXmlFiles = currentFiles
			throw e
		}
	}
	
	private reloadHadoopConfig() {
		
		Configuration newConfiguration = new Configuration(false)
		//s3 properties are set as final params
		
		String awsCredentials = """\
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>fs.s3n.awsAccessKeyId</name>
    <value>${awsAccessKeyId ? awsAccessKeyId : ''}</value>
	<final>true</final>
  </property>
  
  <property>
    <name>fs.s3n.awsSecretAccessKey</name>
    <value>${awsSecretAccessKey ? awsSecretAccessKey : ''}</value>
    <final>true</final>
  </property>

</configuration>
"""
		
		newConfiguration.addResource(new ByteArrayInputStream(awsCredentials.getBytes(StandardCharsets.UTF_8)))
		for(String xml : hadoopXmlFiles) {
			newConfiguration.addResource(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)))
		}
		
		hadoopConfiguration.clear()
		
		hadoopConfiguration = newConfiguration
		
	}
	
	public void setHadoopConfigXML( String hadoopXMLFile) {
		this.setHadoopConfigXML([hadoopXMLFile])
	}
	
	public void setAWSAccessCredentials(String key, secret) {

		this.awsAccessKeyId = key
		this.awsSecretAccessKey = secret
		
		this.reloadHadoopConfig()
		//this
		
	}
	
	public Configuration getHadoopConfiguration() {
		return this.hadoopConfiguration
	}
	
}
