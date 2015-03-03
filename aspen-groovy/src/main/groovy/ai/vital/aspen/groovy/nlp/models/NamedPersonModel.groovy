package ai.vital.aspen.groovy.nlp.models;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamedPersonModel {

	private final static Logger log = LoggerFactory.getLogger(NamedPersonModel.class);
	
	private static NameFinderME singleton;
	
	private NamedPersonModel() {		
	}

	public static void init(InputStream inputStream)  {
		
		if(singleton == null) {
			
			synchronized(NamedPersonModel.class) {
				
				if(singleton == null) {
					
					long start = System.currentTimeMillis();
					
					try {
						singleton = new NameFinderME(new TokenNameFinderModel(inputStream));
					} catch (IOException e) {
						log.error(e.getLocalizedMessage(), e);
						throw new Exception(e);
					}
					
					long stop = System.currentTimeMillis();
					
					log.info("Person Name loaded, {}ms", stop - start);
					
				}
				
			}
			
		} else {
			log.warn("Person Name finder already initialized!");
		}
		
	}
	
	public static NameFinderME getNameFinder()  {
		if(singleton == null) throw new Exception("Name finder not initialized!");
		return singleton;
	}
	
	public static void purge() {
		singleton = null
	}
	
}
