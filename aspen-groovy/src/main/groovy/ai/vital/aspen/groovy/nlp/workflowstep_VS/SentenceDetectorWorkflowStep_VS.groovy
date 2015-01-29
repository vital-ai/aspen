/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.workflowstep_VS;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import opennlp.tools.sentdetect.SentenceDetectorME;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.domain.Document;
import ai.vital.domain.Edge_hasSentence;
import ai.vital.domain.Sentence;
import ai.vital.domain.TextBlock;
import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.aspen.groovy.nlp.config.OpenNLPConfig;
import ai.vital.aspen.groovy.nlp.model.EdgeUtils;
import ai.vital.aspen.groovy.nlp.models.SentenceDetectorModel;
import ai.vital.flow.server.ontology.VitalOntology;
import ai.vital.vitalsigns.model.container.Payload;
import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepV2Impl


import com.hp.hpl.jena.rdf.model.Model;

public class SentenceDetectorWorkflowStep_VS extends WorkflowStepV2Impl<NLPServerConfig> {

	public final static StepName SENTENCEDETECTOR_VS = new StepName("sentencedetector_vs");
	
	private final static Logger log = LoggerFactory.getLogger(SentenceDetectorWorkflowStep_VS.class);
	
	private SentenceDetectorME detector;
	
	@Override
	public void init(NLPServerConfig config) throws StepInitializationException {
		super.init(config);
		
		OpenNLPConfig openNLP = config.getOpenNLP();
		
		File modelFile = new File(openNLP.getModelsDir(), openNLP.getSentencesModel());
		
		log.info("Initializing sentences model from file: {} ...", modelFile.getAbsolutePath());
		
		SentenceDetectorModel.init(modelFile);
		
		long start = System.currentTimeMillis();
		
		detector = SentenceDetectorModel.getDetector();
		
		long stop = System.currentTimeMillis();
	
		log.info("Sentences detector obtained, {}ms", (stop-start));
		
	}

	@Override
	public String getName() {
		return SENTENCEDETECTOR_VS.getName();
	}

	@Override
	public void processPayload(Payload payload)
			throws ai.vital.workflow.IWorkflowStep.WorkflowHaltException,
			ai.vital.workflow.IWorkflowStep.ProcessflowHaltException,
			Exception {

		Map<String, Object> context = ai.vital.flow.server.utils.JSONUtils.getContextMap(payload);
		
		boolean singleSentencePerBlock = Boolean.TRUE.equals(context.get("singleSentencePerBlock"));		
		
		for( Document doc : payload.iterator(Document.class) ) {

			String uri = doc.getURI();
			
			log.info("Processing doc: {}", uri);

			int startIndex = 1;
			
			for( TextBlock block : doc.getTextBlocks() ) {
			
				startIndex = processTextBloc(payload, doc, block, startIndex, singleSentencePerBlock);

			}
		
		}
		
	}

	public int processTextBloc(Payload payload, Document doc, TextBlock block, int index, boolean singleSentencePerBlock) {
		
		String text = block.text;
		
		String[] sentArray = null;
		
		if(singleSentencePerBlock) {
			sentArray = [text];
		} else {
			sentArray = detector.sentDetect(text);
		}
		
		int low_index = 0;
		
		int high_index = 0;
		
		int cursor = 0;
		
		List<Sentence> sentences = new ArrayList<Sentence>();
		
		for(String s : sentArray ) {
			
			//whitespace bug
			s = s.trim();
	            
            low_index = text.indexOf(s, cursor);
	            
            if(low_index < 0) {
            	throw new RuntimeException("Sentence not found! The sentence detector must have changed the sentence.");
            }

            high_index = low_index + ( s.length() );

            Sentence sentObj = new Sentence();
            
            sentObj.startPosition = low_index;
            sentObj.endPosition = high_index;
            sentObj.sentenceNumber = index;
            sentObj.setURI(doc.getURI() + "#sentence_" + index++);
			
			sentences.add(sentObj);
            
			cursor = high_index;
			
		}
		
//		block.setSentences(sentences);
		payload.putGraphObjects(sentences);
		payload.putGraphObjects(EdgeUtils.createEdges(block, sentences, Edge_hasSentence, VitalOntology.Edge_hasSentenceURIBase));
		
		return index;
		
	}

}
