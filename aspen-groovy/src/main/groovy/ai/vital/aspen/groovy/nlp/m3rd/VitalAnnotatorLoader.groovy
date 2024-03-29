package ai.vital.aspen.groovy.nlp.m3rd;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.cmu.minorthird.text.AnnotatorLoader;

public class VitalAnnotatorLoader extends AnnotatorLoader {

	private final static Logger log = LoggerFactory.getLogger(VitalAnnotatorLoader.class);
	
	private File mixupDirectory;
	
	public VitalAnnotatorLoader(File mixupDirectory) {
		super();
		this.mixupDirectory = mixupDirectory;
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

		log.debug("Looking for file resource: {}...", file);
		
		
		File mixupFile = new File(mixupDirectory, file);
		
		if(mixupFile.exists()) {
			try {
				return new FileInputStream(mixupFile);
			} catch (FileNotFoundException e) {
				log.error(e.getLocalizedMessage(), e);
			}
		}
		
		return null;
	}

}
