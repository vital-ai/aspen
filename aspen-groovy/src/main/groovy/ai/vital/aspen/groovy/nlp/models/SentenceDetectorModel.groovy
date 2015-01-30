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

import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SentenceDetectorModel {

	private final static Logger log = LoggerFactory.getLogger(SentenceDetectorModel.class);
	
	private static SentenceDetectorME singleton;
	
	private SentenceDetectorModel() {		
	}

	public static void init(File modelFile)  {
		
		if(singleton == null) {
			
			synchronized(SentenceDetectorModel.class) {
				
				if(singleton == null) {
					
					log.info("Initializing sentence detector model from file: {}", modelFile.getAbsolutePath());
					
					SentenceModel model = null;
					
					long start = System.currentTimeMillis();
					
					try {
						model = new SentenceModel(new FileInputStream(modelFile));
					} catch (IOException e) {
						log.error("Error when loading sentence model file: " + e.getLocalizedMessage());
						throw new Exception(e);
					}
					
					
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
	
}
