package ai.vital.aspen.groovy.nlp.models;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import opennlp.tools.chunker.ChunkerME;
import opennlp.tools.chunker.ChunkerModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ChunkerModelWrapper {

	private final static Logger log = LoggerFactory.getLogger(ChunkerModelWrapper.class);
	
	private static ChunkerME singleton;
	
	private ChunkerModelWrapper() {		
	}

	public static void init(InputStream inputStream)  {
		
		if(singleton == null) {
			
			synchronized(ChunkerModelWrapper.class) {
				
				if(singleton == null) {
					
					log.info("Initializing Chunker model from stream...");
					
					long start = System.currentTimeMillis();
					
					try {
						singleton = new ChunkerME(new ChunkerModel(inputStream));
					} catch (IOException e) {
						log.error(e.getLocalizedMessage(), e);
						throw new Exception(e);
					}
					
					long stop = System.currentTimeMillis();
					
					log.info("Chunker model loaded, {}ms", stop - start);
					
				}
				
			}
			
		} else {
			log.warn("Chunker model already initialized!");
		}
		
	}
	
	public static ChunkerME getChunker()  {
		if(singleton == null) throw new Exception("Chunker model not initialized!");
		return singleton;
	}

	public static void purge() {
		singleton = null
	}	
}
