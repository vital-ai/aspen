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

import opennlp.tools.postag.POSTaggerME;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;

import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.aspen.groovy.nlp.config.OpenNLPConfig;
import ai.vital.aspen.groovy.nlp.domain.Document;
import ai.vital.aspen.groovy.nlp.domain.PosTag;
import ai.vital.aspen.groovy.nlp.domain.Sentence;
import ai.vital.aspen.groovy.nlp.domain.TextBlock;
import ai.vital.aspen.groovy.nlp.domain.Token;
import ai.vital.aspen.groovy.nlp.domain.rdf.DocumentExtractor;
import ai.vital.aspen.groovy.nlp.models.POSTaggerModel;
import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepImpl;

public class PosTaggerWorkflowStep extends WorkflowStepImpl<NLPServerConfig> {

	public final static StepName POSTAGGER = new StepName("postagger");
	
	private final static Logger log = LoggerFactory.getLogger(PosTaggerWorkflowStep.class);
	
	private POSTaggerME posTagger;
	
	@Override
	public void init(NLPServerConfig config) throws StepInitializationException {
	
		OpenNLPConfig openNLP = config.getOpenNLP();
		File modelFile = new File(openNLP.getModelsDir(), openNLP.getPosTaggerModel());
		
		log.info("Initializing POS tagger model from file: {}", modelFile.getAbsolutePath());
		
		long start = System.currentTimeMillis();
		
		POSTaggerModel.init(modelFile);
		
		posTagger = POSTaggerModel.getTagger();
		
		long stop = System.currentTimeMillis();
		
		log.info("POS tagger obtained, {}ms", stop - start);
		
		super.init(config);
	}
	
	@Override
	public String getName() {
		return POSTAGGER.getName();
	}

	@Override
	public Model processModel(Model model, Map<String, Serializable> context)
			throws ai.vital.workflow.IWorkflowStep.WorkflowHaltException,
			ai.vital.workflow.IWorkflowStep.ProcessflowHaltException,
			Exception {

		List<Document> docs = DocumentExtractor.readDocuments(model);
		
		for( Document doc : docs ) {
			
			String docUri = doc.getUri();
			
			log.debug("Processing document {} ...", docUri);
			
			for(TextBlock b : doc.getTextBlocks()) {
				
				for(Sentence s : b.getSentences()) {

					List<PosTag> tags = new ArrayList<PosTag>();
					
					List<Token> tokens = s.getTokens();
					
					String[] sentenceA = new String[tokens.size()];
					
					for(int i = 0 ; i < tokens.size(); i++) {
						sentenceA[i] = tokens.get(i).getText();
					}
					
					String[] posTags = posTagger.tag(sentenceA);
					
					double[] probs = posTagger.probs();
					
					for(int i = 0 ; i < posTags.length; i++) {
						PosTag pt = new PosTag();
						pt.setConfidence(probs[i]);
						pt.setTag(posTags[i]);
						tags.add(pt);
						pt.setUri(s.getUri() + "_posTag_" + tags.size());
					}
					
					s.setPosTags(tags);
				}
				
			}
			
			DocumentExtractor.updateDoc(model, doc);
			
		}
		
		return model;
	}

}
