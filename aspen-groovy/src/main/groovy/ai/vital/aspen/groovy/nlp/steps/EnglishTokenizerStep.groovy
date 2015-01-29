package ai.vital.aspen.groovy.nlp.steps


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import java.util.Map.Entry;

import ai.vital.domain.Category;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;

import com.hp.hpl.jena.rdf.model.Model;

import ai.vital.aspen.groovy.nlp.domain.Document;
import ai.vital.aspen.groovy.nlp.domain.Sentence;
import ai.vital.aspen.groovy.nlp.domain.TextBlock;
import ai.vital.aspen.groovy.nlp.domain.Token;
import ai.vital.aspen.groovy.nlp.domain.rdf.DocumentExtractor;
import ai.vital.opennlp.classifier.Classification
import ai.vital.opennlp.classifier.Classifier;
import ai.vital.vitalsigns.model.container.Payload;
import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.IWorkflowStep.ProcessflowHaltException
import ai.vital.workflow.IWorkflowStep.WorkflowHaltException
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepImpl;

import ai.vital.aspen.groovy.nlp.models.EnglishTokenizerModel



//TODO modify this to output tokens
// this was copied from sentiment classifier


class EnglishTokenizerStep {
	
	public final static StepName ENGLISHTOKENIZER = new StepName("englishtokenizer");
	
	private final static Logger log = LoggerFactory.getLogger(EnglishTokenizerStep.class);
	
	private Classifier englishtokenClassifier;
	
	public String getName() {
		return ENGLISHTOKENIZER.getName();
	}
	
	
	public void init()  {
		
		File englishtokenizerModelFile = new File("resources/models/", "en-token.bin");
		
		log.info("Initializing English Tokenizer model from file: {} ...", englishtokenizerModelFile.getAbsolutePath());
		
		englishtokenClassifier.init(englishtokenizerModelFile);
		
		englishtokenClassifier = EnglishTokenizerModel.get();
		
	}

	
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
					
					Classification[] categorizeDetailed = englishtokenClassifier.categorizeDetailed(sentenceA);

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
