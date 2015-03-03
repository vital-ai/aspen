package ai.vital.aspen.groovy.nlp.m3rd

import java.io.File;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.aspen.groovy.AspenGroovyConfig;
import edu.cmu.minorthird.text.AnnotatorLoader;

class VitalClasspathAnnotatorLoader extends AnnotatorLoader {

	private final static Logger log = LoggerFactory.getLogger(VitalClasspathAnnotatorLoader.class);
	
	private String mixupDirectoryPath;
	
	public VitalClasspathAnnotatorLoader(String mixupDirectoryPath) {
		super();
		if(!mixupDirectoryPath.endsWith("/")) {
			this.mixupDirectoryPath = mixupDirectoryPath + '/'
		} else {
			this.mixupDirectoryPath = mixupDirectoryPath;
		}
	}

	@Override
	public Class<?> findClassResource(String className) {
		try{
			return VitalAnnotatorLoader.class.getClassLoader().loadClass(className);
		}
		catch(ClassNotFoundException e){
			return null;
		}
	}

	@Override
	public InputStream findFileResource(String file) {

		String path = mixupDirectoryPath + file
		
		log.debug("Looking for file classpath resource: {}...", path);
		
		return AspenGroovyConfig.class.getResourceAsStream(path)
		
	}

	
}
