package ai.vital.aspen.groovy.nlp.models;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SentenceDetectorModel {

	private final static Logger log = LoggerFactory.getLogger(SentenceDetectorModel.class);
	
	private static SentenceDetectorME singleton;
	
	private SentenceDetectorModel() {		
	}

	public static void init(InputStream inputStream)  {
		
		if(singleton == null) {
			
			synchronized(SentenceDetectorModel.class) {
				
				if(singleton == null) {
					
					SentenceModel model = null;
					
					long start = System.currentTimeMillis();
					
					model = new SentenceModel(inputStream);
					
					singleton = new SentenceDetectorME(model);
					
					long stop = System.currentTimeMillis();
					
					log.info("Sentences detector initialized, {}ms", (stop-start));
					
				}
				
			}
			
		} else {
			log.warn("Sentence detector model already initialized!");
		}
		
	}
	
	public static SentenceDetectorME getDetector()  {
		if(singleton == null) throw new Exception("Sentence detector model not initialized!");
		return singleton;
	}
	
	public static void purge() {
		singleton = null
	}
	
}
