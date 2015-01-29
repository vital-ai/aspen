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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.aspen.groovy.nlp.config.OpenNLPConfig;
import ai.vital.aspen.groovy.nlp.domain.Category;
import ai.vital.aspen.groovy.nlp.domain.Document;
import ai.vital.aspen.groovy.nlp.domain.Sentence;
import ai.vital.aspen.groovy.nlp.domain.TextBlock;
import ai.vital.aspen.groovy.nlp.domain.Token;
import ai.vital.aspen.groovy.nlp.domain.rdf.DocumentExtractor;
import ai.vital.aspen.groovy.nlp.models.SentimentClassifier;
import ai.vital.opennlp.classifier.Classification;
import ai.vital.opennlp.classifier.Classifier;
import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepImpl;

import com.hp.hpl.jena.rdf.model.Model;

public class SentimentClassifierWorkflowStep extends WorkflowStepImpl<NLPServerConfig> {

	public final static StepName SENTIMENTCLASSIFIER = new StepName("sentimentclassifier");
	
	private final static Logger log = LoggerFactory.getLogger(SentimentClassifierWorkflowStep.class);
	
	private Classifier sentimentClassifier;
	
	@Override
	public String getName() {
		return SENTIMENTCLASSIFIER.getName();
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
	public Model processModel(Model model, Map<String, Serializable> context)
			throws ai.vital.workflow.IWorkflowStep.WorkflowHaltException,
			ai.vital.workflow.IWorkflowStep.ProcessflowHaltException,
			Exception {

		List<Document> docs = DocumentExtractor.readDocuments(model);
		
		for(Document doc : docs) {
			
			String docUri = doc.getUri();
			
			log.info("Processing document with URI: " + docUri);
			
			int sentencesCount = 0;
			
			Map<String, Double> results = new HashMap<String, Double>();
			
			for(TextBlock b : doc.getTextBlocks()) {
				
				for(Sentence sentence : b.getSentences()) {
			
					sentencesCount++;
					
					List<Token> tokens = sentence.getTokens();
					
					String[] sentenceA = new String[tokens.size()];
					
					for(int i = 0 ; i < tokens.size(); i++) {
						//lowercased!
						sentenceA[i] = tokens.get(i).getText().toLowerCase();
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
				c.setName("sentiment/" + bestCategory.toLowerCase());
				c.setScore(bestScore.floatValue() / (float)sentencesCount);
				c.setUri(docUri + "#SentimentCategory");
				doc.getCategories().add(c);
				
			}
			
			DocumentExtractor.updateDoc(model, doc);
			
		}
		
		return model;
	}

}
