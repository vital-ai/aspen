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

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.workflow.StepInitializationException;

public class NamedPersonModel {

	private final static Logger log = LoggerFactory.getLogger(NamedPersonModel.class);
	
	private static NameFinderME singleton;
	
	private NamedPersonModel() {		
	}

	public static void init(File modelFile) throws StepInitializationException {
		
		if(singleton == null) {
			
			synchronized(NamedPersonModel.class) {
				
				if(singleton == null) {
					
					log.info("Initializing Person Name model from file: {}", modelFile.getAbsolutePath());
					
					long start = System.currentTimeMillis();
					
					try {
						singleton = new NameFinderME(new TokenNameFinderModel(new FileInputStream(modelFile)));
					} catch (IOException e) {
						log.error(e.getLocalizedMessage(), e);
						throw new StepInitializationException(e);
					}
					
					long stop = System.currentTimeMillis();
					
					log.info("Person Name loaded, {}ms", stop - start);
					
				}
				
			}
			
		} else {
			log.warn("Person Name finder already initialized!");
		}
		
	}
	
	public static NameFinderME getNameFinder() throws StepInitializationException {
		if(singleton == null) throw new StepInitializationException("Name finder not initialized!");
		return singleton;
	}
	
}
