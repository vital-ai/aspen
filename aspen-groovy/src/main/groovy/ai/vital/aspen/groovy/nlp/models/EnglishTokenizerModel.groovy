package ai.vital.aspen.groovy.nlp.models

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.opennlp.classifier.Classifier;

class EnglishTokenizerModel {

	
	private final static Logger log = LoggerFactory.getLogger(EnglishTokenizerModel.class);
	
	private static Classifier singleton;
	
	private EnglishTokenizerModel() {
	}

	public static void init(File modelFile) {
		
		if(singleton == null) {
			
			synchronized(EnglishTokenizerModel.class) {
				
				if(singleton == null) {
					
					log.info("Initializing EnglishTokenizer classifier from file: {}", modelFile.getAbsolutePath());
					
					long start = System.currentTimeMillis();
					
					try {
						singleton = new Classifier();
						singleton.init(new FileInputStream(modelFile));
					} catch (IOException e) {
						log.error(e.getLocalizedMessage(), e);
						throw new Exception(e);
					}
					
					long stop = System.currentTimeMillis();
					
					log.info("English Tokenizer Model classifier loaded, {}ms", stop - start);
					
				}
				
			}
			
		} else {
			log.warn("English Tokenizer model classifier already initialized!");
		}
		
	}
	
	public static Classifier get()  {
		if(singleton == null) throw new Exception("English Tokenizer classifier not initialized!");
		return singleton;
	}
	
	
	
	
	
}
