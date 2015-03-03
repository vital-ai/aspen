package ai.vital.aspen.groovy.nlp.models;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class POSTaggerModel {

	private final static Logger log = LoggerFactory.getLogger(POSTaggerModel.class);
	
	private static POSTaggerME singleton;
	
	private POSTaggerModel() {		
	}

	public static void init(InputStream inputStream)  {
		
		if(singleton == null) {
			
			synchronized(POSTaggerModel.class) {
				
				if(singleton == null) {
					
					long start = System.currentTimeMillis();
					
					try {
						singleton = new POSTaggerME(new POSModel(inputStream));
					} catch (IOException e) {
						log.error(e.getLocalizedMessage(), e);
						throw new Exception(e);
					}
					
					long stop = System.currentTimeMillis();
					
					log.info("POS tagger model loaded, {}ms", stop - start);
					
				}
				
			}
			
		} else {
			log.warn("POS tagger model already initialized!");
		}
		
	}
	
	public static POSTaggerME getTagger()  {
		if(singleton == null) throw new Exception("POS Tagger not initialized!");
		return singleton;
	}
	
	public static void purge() {
		singleton = null
	}
	
}
