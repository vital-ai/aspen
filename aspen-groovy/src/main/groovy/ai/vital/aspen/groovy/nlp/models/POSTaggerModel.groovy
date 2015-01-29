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

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.workflow.StepInitializationException;

public class POSTaggerModel {

	private final static Logger log = LoggerFactory.getLogger(POSTaggerModel.class);
	
	private static POSTaggerME singleton;
	
	private POSTaggerModel() {		
	}

	public static void init(File modelFile) throws StepInitializationException {
		
		if(singleton == null) {
			
			synchronized(POSTaggerModel.class) {
				
				if(singleton == null) {
					
					log.info("Initializing POS tagger model from file: {}", modelFile.getAbsolutePath());
					
					long start = System.currentTimeMillis();
					
					try {
						singleton = new POSTaggerME(new POSModel(new FileInputStream(modelFile)));
					} catch (IOException e) {
						log.error(e.getLocalizedMessage(), e);
						throw new StepInitializationException(e);
					}
					
					long stop = System.currentTimeMillis();
					
					log.info("POS tagger model loaded, {}ms", stop - start);
					
				}
				
			}
			
		} else {
			log.warn("POS tagger model already initialized!");
		}
		
	}
	
	public static POSTaggerME getTagger() throws StepInitializationException {
		if(singleton == null) throw new StepInitializationException("POS Tagger not initialized!");
		return singleton;
	}
	
}
