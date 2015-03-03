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




import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;

import com.hp.hpl.jena.rdf.model.Model;

import ai.vital.domain.Document;
import ai.vital.domain.Sentence;
import ai.vital.domain.TextBlock;
import ai.vital.domain.Token;
import ai.vital.opennlp.classifier.Classification
import ai.vital.opennlp.classifier.Classifier;
import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.vitalsigns.model.VITAL_Container;
import ai.vital.aspen.groovy.nlp.models.EnglishTokenizerModel
import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.ontology.VitalOntology



//TODO modify this to output tokens
// this was copied from sentiment classifier


class EnglishTokenizerStep {
	
	public final static String ENGLISHTOKENIZER = "englishtokenizer";
	
	private final static Logger log = LoggerFactory.getLogger(EnglishTokenizerStep.class);
	
	private Classifier englishtokenClassifier;
	
	public String getName() {
		return ENGLISHTOKENIZER;
	}
	
	
	public void init()  {
		
		try {
			englishtokenClassifier = EnglishTokenizerModel.get()
		} catch(Exception e) {
		}
		
		if(englishtokenClassifier == null) {
			
			InputStream inputStream = null
			
			try {
				
				if( AspenGroovyConfig.get().loadResourcesFromClasspath) {
					
					String path = "/resources/models/en-token.bin"
					
					log.info("Initializing English Tokenizer model from classpath: {} ...", path);
					
					inputStream = AspenGroovyConfig.class.getResourceAsStream(path)
					
				} else {
				
					File englishtokenizerModelFile = new File("resources/models/", "en-token.bin");
					
					log.info("Initializing English Tokenizer model from file: {} ...", englishtokenizerModelFile.getAbsolutePath());
					
					inputStream = new FileInputStream(englishtokenizerModelFile)
					
				}
				
				EnglishTokenizerModel.init(inputStream);
				
				
			} finally {
				IOUtils.closeQuietly(inputStream)
			}
			
			englishtokenClassifier = EnglishTokenizerModel.get();
		}

		
	}

	
	public void processPayload(VITAL_Container payload)
			throws 
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
