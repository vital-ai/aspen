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

import opennlp.tools.chunker.ChunkerME;
import opennlp.tools.chunker.ChunkerModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.workflow.StepInitializationException;

public class ChunkerModelWrapper {

	private final static Logger log = LoggerFactory.getLogger(ChunkerModelWrapper.class);
	
	private static ChunkerME singleton;
	
	private ChunkerModelWrapper() {		
	}

	public static void init(File modelFile) throws StepInitializationException {
		
		if(singleton == null) {
			
			synchronized(ChunkerModelWrapper.class) {
				
				if(singleton == null) {
					
					log.info("Initializing Chunkder model from file: {}", modelFile.getAbsolutePath());
					
					long start = System.currentTimeMillis();
					
					try {
						singleton = new ChunkerME(new ChunkerModel(new FileInputStream(modelFile)));
					} catch (IOException e) {
						log.error(e.getLocalizedMessage(), e);
						throw new StepInitializationException(e);
					}
					
					long stop = System.currentTimeMillis();
					
					log.info("Chunker model loaded, {}ms", stop - start);
					
				}
				
			}
			
		} else {
			log.warn("Sentence detector model already initialized!");
		}
		
	}
	
	public static ChunkerME getChunker() throws StepInitializationException {
		if(singleton == null) throw new StepInitializationException("Chunker model not initialized!");
		return singleton;
	}
	
}