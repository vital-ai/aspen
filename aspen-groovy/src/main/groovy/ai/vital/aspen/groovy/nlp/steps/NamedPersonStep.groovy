package ai.vital.aspen.groovy.nlp.steps

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.util.Span;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// changed from namedperson


import com.vitalai.domain.nlp.Document;
import com.vitalai.domain.nlp.Edge_hasEntity;
import com.vitalai.domain.nlp.Edge_hasEntityInstance;
import com.vitalai.domain.nlp.Edge_hasSentenceEntityInstance;
import com.vitalai.domain.nlp.Entity;
import com.vitalai.domain.nlp.EntityInstance;
import com.vitalai.domain.nlp.Sentence;
import com.vitalai.domain.nlp.TextBlock;
import com.vitalai.domain.nlp.Token;
import ai.vital.vitalsigns.model.VITAL_Container;
import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.nlp.model.DocumentUtils;
import ai.vital.aspen.groovy.nlp.model.EdgeUtils;
import ai.vital.aspen.groovy.nlp.model.TokenUtils;
import ai.vital.aspen.groovy.nlp.models.NamedPersonModel;
import ai.vital.aspen.groovy.ontology.VitalOntology


class NamedPersonStep {
	
	public final static String NAMEDPERSONTAGGER_VS = "namedpersontagger_vs";
	
	private final static Logger log = LoggerFactory.getLogger(NamedPersonStep.class);
	
	private NameFinderME nameFinder;
	
	
	public String getName() {
		return NAMEDPERSONTAGGER_VS;
	}
	
	
	public void init()  {

		try {
			nameFinder = NamedPersonModel.getNameFinder()
		} catch(Exception e) {
		}
				
		
		if(nameFinder == null) {
			
			InputStream inputStream = null
			
			try {
			
				if( AspenGroovyConfig.get().loadResourcesFromClasspath ) {
					
					String path = "/resources/models/en-ner-person.bin"
					
					log.info("Initializing named person model from classpath: {}", path);
					
					inputStream = AspenGroovyConfig.class.getResourceAsStream(path)
					
					if(inputStream == null) throw new RuntimeException("Model file not found: ${path}")
					
				} else {
				
					String resDir = AspenGroovyConfig.get().resourcesDir
					if(!resDir) throw new RuntimeException("resourcesDir not set")
				
					File namedPersonModelFile = new File(new File(resDir, "models"), "en-ner-person.bin")
					log.info("Initializing named person model from file: {}", namedPersonModelFile.getAbsolutePath());
					
					if(!namedPersonModelFile.exists()) {
						throw new RuntimeException("Model file not found: ${namedPersonModelFile.absolutePath}")
					}
					
					inputStream = new FileInputStream(namedPersonModelFile)
					
				}
			
				NamedPersonModel.init(inputStream);
				
				nameFinder = NamedPersonModel.getNameFinder()
				
			} finally {
			
				IOUtils.closeQuietly(inputStream)
			
			}
			
						
		}
		
			
		
		
		
		nameFinder = NamedPersonModel.getNameFinder();
		
	}

	public void processDocument(Document doc, List results)
			throws 
			Exception {

			String docUri = doc.getURI();
			
			log.info("Processing document {} ...", docUri);
			
			List<Entity> namedPersons = new ArrayList<Entity>();
			
			int blockOffset = 0;
			
			for(TextBlock b : doc.getTextBlocks()) {
				
				String blockText = b.text;
				
				List sentences = b.getSentences();
				
				for(Sentence sentence : sentences) {
			
					int sentenceOffset = sentence.startPosition.rawValue();
					
					String sentenceText = blockText.substring(sentence.startPosition.rawValue(), sentence.endPosition.rawValue());
					
					List<Token> tokens = TokenUtils.getTokens(sentence); //sentence.getTokens();
					
					String[] sentenceA = new String[tokens.size()];
					
					for(int i = 0 ; i < tokens.size(); i++) {
						sentenceA[i] = tokens.get(i).tokenText.toString();
					}
					
					Span[] names = nameFinder.find(sentenceA);
					
					for(Span n : names) {
						
						Token firstToken = tokens.get(n.getStart());
						Token lastToken = tokens.get(n.getEnd() -1 );

						Entity np = new Entity();
						np.setURI(docUri + "#NamedPerson_" + namedPersons.size());
						
						String substring = sentenceText.substring(firstToken.startPosition.rawValue(), lastToken.endPosition.rawValue());
						np.name = substring;
						np.extractSource = "OpenNLP";
						np.category = 'Person';

						EntityInstance entityInstance = new EntityInstance();
						entityInstance.setURI(np.getURI() + "instance_0");
						entityInstance.exactString = substring;
						entityInstance.lengthInSentence = substring.length();
						entityInstance.offsetInSentence = firstToken.startPosition.rawValue();
						entityInstance.offset = DocumentUtils.translateBlocksContentOffsetToBodyOffset(doc, blockOffset + sentenceOffset + firstToken.startPosition.rawValue());

//							doc.translateBlocksContentOffsetToBodyOffset(blockOffset + firstToken.getStart()));
						entityInstance.length = DocumentUtils.translateBlocksContentOffsetToBodyOffset(doc, blockOffset + sentenceOffset + lastToken.endPosition.rawValue() - entityInstance.offset.rawValue());
						
						
						Set<String> spanTypes = new HashSet<String>()
						spanTypes.add("Person")
					
						//entityInstance.spanType = spanTypes
						//todo: was this supposed to be a list?
						entityInstance.spanType = "Person"
						
						
						
						List<EntityInstance> instances = new ArrayList<EntityInstance>();
						instances.add(entityInstance);
						
//						np.setEntityInstances(instances);
//
//						sentence.getEntityInstances().add(entityInstance);
//
//						doc.getEntities().add(np);
						
						results.addAll(Arrays.asList(np, entityInstance));
						results.addAll(EdgeUtils.createEdges(doc, Arrays.asList(np), Edge_hasEntity.class, VitalOntology.Edge_hasEntityURIBase));
						results.addAll(EdgeUtils.createEdges(np, Arrays.asList(entityInstance), Edge_hasEntityInstance.class, VitalOntology.Edge_hasEntityInstanceURIBase));
						results.addAll(EdgeUtils.createEdges(sentence, Arrays.asList(entityInstance), Edge_hasSentenceEntityInstance.class, VitalOntology.Edge_hasSentenceEntityInstanceURIBase));
						
						namedPersons.add(np);
						
					}
					
					
				}
				blockOffset += (blockText.length() + 1);
			
			}
			
		
	}
	
}
