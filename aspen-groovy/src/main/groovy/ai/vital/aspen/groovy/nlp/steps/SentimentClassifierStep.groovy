package ai.vital.aspen.groovy.nlp.steps



import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.domain.Category;
import ai.vital.domain.Document;
import ai.vital.domain.Edge_hasCategory;
import ai.vital.domain.Sentence;
import ai.vital.domain.TextBlock;
import ai.vital.domain.Token;
import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.nlp.model.EdgeUtils;
import ai.vital.aspen.groovy.nlp.model.TokenUtils;
import ai.vital.aspen.groovy.nlp.models.SentimentClassifier;
import ai.vital.opennlp.classifier.Classification;
import ai.vital.opennlp.classifier.Classifier;
import ai.vital.vitalsigns.model.VITAL_Container;

import com.hp.hpl.jena.rdf.model.Model;

import ai.vital.aspen.groovy.ontology.VitalOntology


class SentimentClassifierStep {
	
	public final static String SENTIMENTCLASSIFIER_VS = "sentimentclassifier_vs";
	
	private final static Logger log = LoggerFactory.getLogger(SentimentClassifierStep.class);
	
	private Classifier sentimentClassifier;
	
	
	public String getName() {
		return SENTIMENTCLASSIFIER_VS.getName();
	}
	
	
	public void init()  {
		
		try {
			sentimentClassifier = SentimentClassifier.get()
		} catch(Exception e) {}
		
		if(sentimentClassifier == null) {
			
			InputStream inputStream = null
			
			try {
				
				if( AspenGroovyConfig.get().loadResourcesFromClasspath ) {
					
					String path = "/resources/models/en-sentiment.bin"
					
					log.info("Initializing named person model from path: {} ...", path);
					
					inputStream = AspenGroovyConfig.class.getResourceAsStream(path)
					
					if(inputStream == null) throw new Exception("model file not found: ${path}")
					
				} else {
				
					String resDir = AspenGroovyConfig.get().resourcesDir
					if(!resDir) throw new RuntimeException("resourcesDir not set")
				
					File sentimentModelFile = new File(new File(resDir, "models"), "en-sentiment.bin");
					
					log.info("Initializing named person model from file: {} ...", sentimentModelFile.getAbsolutePath());
					
					if(!sentimentModelFile.exists()) throw new Exception("model file not found: ${sentimentModelFile.absolutePath}")
					
					inputStream = new FileInputStream(sentimentModelFile)
				}
				
				SentimentClassifier.init(inputStream);
				
				sentimentClassifier = SentimentClassifier.get();
				
			} finally {
				IOUtils.closeQuietly(inputStream)
			}
			
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
