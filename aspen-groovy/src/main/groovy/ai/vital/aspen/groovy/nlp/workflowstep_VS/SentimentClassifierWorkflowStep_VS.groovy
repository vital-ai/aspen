/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.workflowstep_VS;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.domain.Category;
import ai.vital.domain.Document;
import ai.vital.domain.Edge_hasCategory;
import ai.vital.domain.Sentence;
import ai.vital.domain.TextBlock;
import ai.vital.domain.Token;
import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.aspen.groovy.nlp.config.OpenNLPConfig;
import ai.vital.aspen.groovy.nlp.model.EdgeUtils;
import ai.vital.aspen.groovy.nlp.model.TokenUtils;
import ai.vital.aspen.groovy.nlp.models.SentimentClassifier;
import ai.vital.flow.server.ontology.VitalOntology;
import ai.vital.opennlp.classifier.Classification;
import ai.vital.opennlp.classifier.Classifier;
import ai.vital.vitalsigns.model.container.Payload;
import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepV2Impl;

import com.hp.hpl.jena.rdf.model.Model;

public class SentimentClassifierWorkflowStep_VS extends WorkflowStepV2Impl<NLPServerConfig> {

	public final static StepName SENTIMENTCLASSIFIER_VS = new StepName("sentimentclassifier_vs");
	
	private final static Logger log = LoggerFactory.getLogger(SentimentClassifierWorkflowStep_VS.class);
	
	private Classifier sentimentClassifier;
	
	@Override
	public String getName() {
		return SENTIMENTCLASSIFIER_VS.getName();
	}
	
	@Override
	public void init(NLPServerConfig config) throws StepInitializationException {
		super.init(config);
		
		OpenNLPConfig openNLP = config.getOpenNLP();
		File sentimentModelFile = new File(openNLP.getModelsDir(), openNLP.getSentimentModel());
		
		log.info("Initializing named person model from file: {} ...", sentimentModelFile.getAbsolutePath());
		
		SentimentClassifier.init(sentimentModelFile);
		
		sentimentClassifier = SentimentClassifier.get();
		
	}

	@Override
	public void processPayload(Payload payload)
			throws ai.vital.workflow.IWorkflowStep.WorkflowHaltException,
			ai.vital.workflow.IWorkflowStep.ProcessflowHaltException,
			Exception {

		for( Document doc : payload.iterator(Document.class) ) {
		
			String uri = doc.getURI();
			
			log.info("Processing doc {}", uri);
			
			int sentencesCount = 0;
			
			Map<String, Double> results = new HashMap<String, Double>();
			
			List tbs = doc.getTextBlocks();
			
			for(TextBlock b : tbs) {
				
				List sentences = b.getSentences();
				
				for(Sentence sentence : sentences) {
			
					sentencesCount++;
					
					List<Token> tokens = TokenUtils.getTokens(sentence); //sentence.getTokens();
					
					String[] sentenceA = new String[tokens.size()];
					
					for(int i = 0 ; i < tokens.size(); i++) {
						//lowercased!
						sentenceA[i] = tokens.get(i).tokenText.toLowerCase();
					}
					
					Classification[] categorizeDetailed = sentimentClassifier.categorizeDetailed(sentenceA);

					for(Classification c : categorizeDetailed) {
						Double totalScore = results.get(c.category);
						if(totalScore == null) {
							totalScore = c.score;
						} else {
							totalScore += c.score;
						}
						results.put(c.category, totalScore);
					}
					
				}
			
			}
			
			String bestCategory = null;
			Double bestScore = null;
					
			
			for(Entry<String, Double> e : results.entrySet()) {
				if(bestCategory == null || bestScore < e.getValue()) {
					bestCategory = e.getKey();
					bestScore = e.getValue();
				}
			}
			
			if(bestCategory != null) {

				Category c = new Category();
				c.name = "sentiment/" + bestCategory.toLowerCase();
				c.score = (float) ( bestScore.floatValue() / (float)sentencesCount ); 
				c.setURI(uri + "#SentimentCategory");
//				doc.getCategories().add(c);
		
				payload.putGraphObjects(Arrays.asList(c));
				payload.putGraphObjects(EdgeUtils.createEdges(doc, Arrays.asList(c), Edge_hasCategory.class, VitalOntology.Edge_hasCategoryURIBase));
						
			}
			
		}
		
	}

}