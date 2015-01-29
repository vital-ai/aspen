/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.workflowstep;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import opennlp.tools.sentdetect.SentenceDetectorME;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.aspen.groovy.nlp.config.OpenNLPConfig;
import ai.vital.aspen.groovy.nlp.domain.Document;
import ai.vital.aspen.groovy.nlp.domain.Sentence;
import ai.vital.aspen.groovy.nlp.domain.TextBlock;
import ai.vital.aspen.groovy.nlp.domain.rdf.DocumentExtractor;
import ai.vital.aspen.groovy.nlp.models.SentenceDetectorModel;
import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.IWorkflowStep.ProcessflowHaltException
import ai.vital.workflow.IWorkflowStep.WorkflowHaltException
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepImpl;

import com.hp.hpl.jena.rdf.model.Model;

public class SentenceDetectorWorkflowStep extends WorkflowStepImpl<NLPServerConfig> {

	public final static StepName SENTENCEDETECTOR = new StepName("sentencedetector");
	
	private final static Logger log = LoggerFactory.getLogger(SentenceDetectorWorkflowStep.class);
	
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
		return SENTENCEDETECTOR.getName();
	}

	@Override
	public Model processModel(Model model, Map<String, Serializable> context)
			throws WorkflowHaltException, ProcessflowHaltException, Exception {

		List<Document> docs = DocumentExtractor.readDocuments(model);
		
		for( Document doc : docs ) {
			
			String uri = doc.getUri();
			
			log.debug("Processing doc: {}", uri);

			int startIndex = 1;
			
			for( TextBlock block : doc.getTextBlocks() ) {
			
				startIndex = processTextBloc(doc, block, startIndex);

			}
		
			DocumentExtractor.updateDoc(model, doc);
			
		}
		
		return model;
	}

	public int processTextBloc(Document doc, TextBlock block, int index) {
		
		String text = block.getText();
		
		String[] sentArray = detector.sentDetect(text);
		
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
            
            sentObj.setStart(low_index);
            sentObj.setEnd(high_index);
            sentObj.setSentenceNumber(index);
            sentObj.setUri(doc.getUri() + "#sentence_" + index++);
			sentences.add(sentObj);
            
			cursor = high_index;
			
		}
		
		block.setSentences(sentences);
		
		return index;
		
	}

}
