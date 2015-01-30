/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.models;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.opennlp.classifier.Classifier;

public class SentimentClassifier {

	private final static Logger log = LoggerFactory.getLogger(SentimentClassifier.class);
	
	private static Classifier singleton;
	
	private SentimentClassifier() {		
	}

	public static void init(File modelFile) {
		
		if(singleton == null) {
			
			synchronized(SentimentClassifier.class) {
				
				if(singleton == null) {
					
					log.info("Initializing Sentiment classifier from file: {}", modelFile.getAbsolutePath());
					
					long start = System.currentTimeMillis();
					
					try {
						singleton = new Classifier();
						singleton.init(new FileInputStream(modelFile));
					} catch (IOException e) {
						log.error(e.getLocalizedMessage(), e);
						throw new Exception(e);
					}
					
					long stop = System.currentTimeMillis();
					
					log.info("Sentiment classifier loaded, {}ms", stop - start);
					
				}
				
			}
			
		} else {
			log.warn("Sentiment classifier already initialized!");
		}
		
	}
	
	public static Classifier get()  {
		if(singleton == null) throw new Exception("Sentiment classifier not initialized!");
		return singleton;
	}
	
}
