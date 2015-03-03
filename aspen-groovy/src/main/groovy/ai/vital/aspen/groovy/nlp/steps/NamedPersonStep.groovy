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



import ai.vital.domain.Person;



import ai.vital.domain.Document;
import ai.vital.domain.Edge_hasEntity;
import ai.vital.domain.Edge_hasEntityInstance;
import ai.vital.domain.Edge_hasSentenceEntityInstance;
import ai.vital.domain.Entity;
import ai.vital.domain.EntityInstance;
import ai.vital.domain.Sentence;
import ai.vital.domain.TextBlock;
import ai.vital.domain.Token;
import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.nlp.model.DocumentUtils;
import ai.vital.aspen.groovy.nlp.model.EdgeUtils;
import ai.vital.aspen.groovy.nlp.model.TokenUtils;
import ai.vital.aspen.groovy.nlp.models.NamedPersonModel;
import ai.vital.vitalsigns.model.container.Payload;
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
					
					String path = "resources/models/en-ner-person.bin"
					
					log.info("Initializing named person model from classpath: {}", path);
					
					inputStream = AspenGroovyConfig.class.getResourceAsStream(path)
					
					if(inputStream == null) throw new RuntimeException("Model file not found: ${path}")
					
				} else {
				
					File namedPersonModelFile = new File("resources/models/", "en-ner-person.bin")
					log.info("Initializing named person model from file: {}", namedPersonModelFile.getAbsolutePath());
					
					if(!namedPersonModelFile.exists()) {
						throw new RuntimeException("Model file not found: ${namedPersonModelFile.absolutePath}")
					}
					
					
				}
			
				NamedPersonModel.init(inputStream);
				
				nameFinder = NamedPersonModel.getNameFinder()
				
			} finally {
			
				IOUtils.closeQuietly(inputStream)
			
			}
			
						
		}
		
			
		
		
		
		nameFinder = NamedPersonModel.getNameFinder();
		
	}

	public void processPayload(Payload payload)
			throws 
			Exception {

		for( Document doc : payload.iterator(Document.class) ) {
			
			String docUri = doc.getURI();
			
			log.info("Processing document {} ...", docUri);
			
			List<Person> namedPersons = new ArrayList<Person>();
			
			int blockOffset = 0;
			
			for(TextBlock b : doc.getTextBlocks()) {
				
				String blockText = b.text;
				
				List sentences = b.getSentences();
				
				for(Sentence sentence : sentences) {
			
					int sentenceOffset = sentence.startPosition;
					
					String sentenceText = blockText.substring(sentence.startPosition, sentence.endPosition);
					
					List<Token> tokens = TokenUtils.getTokens(sentence); //sentence.getTokens();
					
					String[] sentenceA = new String[tokens.size()];
					
					for(int i = 0 ; i < tokens.size(); i++) {
						sentenceA[i] = tokens.get(i).tokenText;
					}
					
					Span[] names = nameFinder.find(sentenceA);
					
					for(Span n : names) {
						
						Token firstToken = tokens.get(n.getStart());
						Token lastToken = tokens.get(n.getEnd() -1 );

						Person np = new Person();
						np.setURI(docUri + "#NamedPerson_" + namedPersons.size());
						
						String substring = sentenceText.substring(firstToken.startPosition, lastToken.endPosition);
						np.name = substring;
						np.extractSource = "OpenNLP";
						np.category = NamedPerson.class.getSimpleName();

						EntityInstance entityInstance = new EntityInstance();
						entityInstance.setURI(np.getURI() + "instance_0");
						entityInstance.exactString = substring;
						entityInstance.lengthInSentence = substring.length();
						entityInstance.offsetInSentence = firstToken.startPosition;
						entityInstance.offset = DocumentUtils.translateBlocksContentOffsetToBodyOffset(doc, blockOffset + sentenceOffset + firstToken.startPosition);

//							doc.translateBlocksContentOffsetToBodyOffset(blockOffset + firstToken.getStart()));
						entityInstance.length = DocumentUtils.translateBlocksContentOffsetToBodyOffset(doc, blockOffset + sentenceOffset + lastToken.endPosition - entityInstance.offset);
						
						List<EntityInstance> instances = new ArrayList<EntityInstance>();
						instances.add(entityInstance);
						
//						np.setEntityInstances(instances);
//
//						sentence.getEntityInstances().add(entityInstance);
//
//						doc.getEntities().add(np);
						
						payload.putGraphObjects(Arrays.asList(np, entityInstance));
						payload.putGraphObjects(EdgeUtils.createEdges(doc, Arrays.asList(np), Edge_hasEntity.class, VitalOntology.Edge_hasEntityURIBase));
						payload.putGraphObjects(EdgeUtils.createEdges(np, Arrays.asList(entityInstance), Edge_hasEntityInstance.class, VitalOntology.Edge_hasEntityInstanceURIBase));
						payload.putGraphObjects(EdgeUtils.createEdges(sentence, Arrays.asList(entityInstance), Edge_hasSentenceEntityInstance.class, VitalOntology.Edge_hasSentenceEntityInstanceURIBase));
						
						namedPersons.add(np);
						
					}
					
					
				}
				blockOffset += (blockText.length() + 1);
			
			}
			
		}
		
	}
	
	

}
