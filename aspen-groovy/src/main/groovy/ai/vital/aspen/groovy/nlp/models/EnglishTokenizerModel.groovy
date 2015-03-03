package ai.vital.aspen.groovy.nlp.models

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.nio.ch.IOUtil;
import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.opennlp.classifier.Classifier;

class EnglishTokenizerModel {

	
	private final static Logger log = LoggerFactory.getLogger(EnglishTokenizerModel.class);
	
	private static Classifier singleton;
	
	private EnglishTokenizerModel() {
	}

	public static void init(InputStream inputStream) throws Exception {
		
		if(singleton == null) {
			
			synchronized(EnglishTokenizerModel.class) {
				
				if(singleton == null) {
					
					long start = System.currentTimeMillis();
					
					singleton = new Classifier();
					singleton.init(inputStream);
					
					long stop = System.currentTimeMillis();
					
					log.info("English Tokenizer Model classifier loaded, {}ms", stop - start);
					
				}
				
			}
			
		} else {
			log.warn("English Tokenizer model classifier already initialized!");
		}
		
	}
	
	public static Classifier get() throws Exception {
		if(singleton == null) throw new Exception("English Tokenizer classifier not initialized!");
		return singleton;
	}
	
	
	public static void purge() {
		singleton = null
	}
	
	
}
