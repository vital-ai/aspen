package ai.vital.aspen.groovy.nlp.steps

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.step.AbstractStep

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;

import ai.vital.domain.Document;
import ai.vital.domain.Edge_hasPosTag;
import ai.vital.domain.PosTag;
import ai.vital.domain.Sentence;
import ai.vital.domain.TextBlock;
import ai.vital.domain.Token;
import ai.vital.aspen.groovy.nlp.model.EdgeUtils;
import ai.vital.aspen.groovy.nlp.models.POSTaggerModel;
import ai.vital.vitalsigns.global.GlobalHashTable;
import ai.vital.vitalsigns.model.container.Payload;
import ai.vital.aspen.groovy.ontology.VitalOntology


class PosTaggerStep extends AbstractStep {

	public final static String POSTAGGER_VS = "postagger_vs";
	
	private final static Logger log = LoggerFactory.getLogger(PosTaggerStep.class);
	
	private POSTaggerME posTagger;
	
	private DecimalFormat confidenceFormat = new DecimalFormat('0.000');
	
	public void init() {
	
		try {
			posTagger = POSTaggerModel.getTagger()
		} catch(Exception e) {}
		
		if(posTagger == null) {
			
			InputStream inputStream = null
			
			try {
				
				if( AspenGroovyConfig.get().loadResourcesFromClasspath ) {
					
					String path = "/resources/models/en-pos-maxent.bin"
					
					log.info("Initializing POS tagger model from classpath: {}", path);
					
					inputStream = AspenGroovyConfig.class.getResourceAsStream(path)
					
					if(inputStream == null) throw new RuntimeException("Model file not found: ${path}")
					
				} else {
				
					File modelFile = new File("resources/models", "en-pos-maxent.bin");
					
					log.info("Initializing POS tagger model from file: {}", modelFile.getAbsolutePath());
				
					if(!modelFile.exists()) throw new RuntimeException("Model file not found: ${modelFile.absolutePath}")
					
					inputStream = new FileInputStream(modelFile) 
						
				}
				
				POSTaggerModel.init(inputStream);
				
				posTagger = POSTaggerModel.getTagger();
				
			} finally {
				IOUtils.closeQuietly(inputStream)
			}
			
		}
		
		
		
	}
	
	public String getName() {
		return POSTAGGER_VS;
	}

	public void processDocument(Document doc) {

			String docUri = doc.getURI();
			
			log.debug("Processing document {} ...", docUri);
			
			int tbindex = 1;
			
			List tbs = doc.getTextBlocks();
			
			for(TextBlock b : tbs) {
				
				long start = System.currentTimeMillis();
				List sentences = b.getSentences();
				long stop = System.currentTimeMillis();
				
				log.info("Processing tb: " + tbindex++ + " of " + tbs.size() + " with " + sentences.size() + " sentences, " + (stop-start) + "ms - HASH SIZE: " + GlobalHashTable.get().size());
				
				int sentenceIndex = 1;
				for(Sentence s : sentences) {

//					List<PosTag> tags = new ArrayList<PosTag>();
					
//					List<Token> tokens = s.getTokens();
//					String[] sentenceA = new String[tokens.size()];
//					for(int i = 0 ; i < tokens.size(); i++) {
//						sentenceA[i] = tokens.get(i).tokenText;
//					}
					
					String[] sentenceA =  s.tokensTextString.split(' ');
					
					
					String[] posTags = posTagger.tag(sentenceA);
					
					StringBuilder posTagsValues = new StringBuilder();
					StringBuilder posTagsConfidence = new StringBuilder();
					
					double[] probs = posTagger.probs();
					
					for(int i = 0 ; i < posTags.length; i++) {
//						PosTag pt = new PosTag();
//						pt.confidence = probs[i];
//						pt.tagValue = posTags[i];
//						tags.add(pt);
//						pt.setURI(s.getURI() + "_posTag_" + tags.size());
						
						if(i > 0) {
							posTagsValues.append(' ');
							posTagsConfidence.append(' ');
						}
						
						posTagsValues.append(posTags[i]);
						posTagsConfidence.append(confidenceFormat.format(probs[i]));
						
					}
					
					s.posTagsValuesString = posTagsValues.toString();
					s.posTagsConfidenceString = posTagsConfidence.toString();
		
					
//					payload.putGraphObjects(tags);
//					payload.putGraphObjects(EdgeUtils.createEdges(s, tags, Edge_hasPosTag, VitalOntology.Edge_hasPosTagURIBase));
//					s.setPosTags(tags);
				}
				
			}
		
	}
	
	
	
	
	
}
