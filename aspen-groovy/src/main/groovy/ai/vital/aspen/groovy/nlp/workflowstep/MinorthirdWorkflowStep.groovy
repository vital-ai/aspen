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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.aspen.groovy.nlp.config.MinorthirdConfig;
import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.aspen.groovy.nlp.domain.Document;
import ai.vital.aspen.groovy.nlp.domain.Entity;
import ai.vital.aspen.groovy.nlp.domain.EntityInstance;
import ai.vital.aspen.groovy.nlp.domain.NounPhrase;
import ai.vital.aspen.groovy.nlp.domain.PosTag;
import ai.vital.aspen.groovy.nlp.domain.Sentence;
import ai.vital.aspen.groovy.nlp.domain.TextBlock;
import ai.vital.aspen.groovy.nlp.domain.Token;
import ai.vital.aspen.groovy.nlp.domain.VerbPhrase;
import ai.vital.aspen.groovy.nlp.domain.rdf.DocumentExtractor;
import ai.vital.aspen.groovy.nlp.m3rd.DocumentTokenizer;
import ai.vital.aspen.groovy.nlp.m3rd.PosAnnotator;
import ai.vital.aspen.groovy.nlp.m3rd.VitalAnnotatorLoader;
import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepImpl;

import com.hp.hpl.jena.rdf.model.Model;

import edu.cmu.minorthird.text.BasicTextBase;
import edu.cmu.minorthird.text.BasicTextLabels;
import edu.cmu.minorthird.text.Span;
import edu.cmu.minorthird.text.mixup.MixupInterpreter;
import edu.cmu.minorthird.text.mixup.MixupProgram;

public class MinorthirdWorkflowStep extends WorkflowStepImpl<NLPServerConfig> {


	private static final String PACKAGE_PREFIX = "package_";

	private static final String NOUN_PHRASE = "NP";
	
	private static final String VERB_PHRASE = "VP";
	
	private static final String SENTENCE_NUMBER = "sentenceNumber";

	private static final String TEXT_BLOCK = "textBlock";
	
	private static final String SENTENCE = "sentence";

	public final static StepName MINORTHIRD = new StepName("minorthird");
	
	private final static Logger log = LoggerFactory.getLogger(MinorthirdWorkflowStep.class);

	private static final String NS_QUALIFIER = ":";
	
	private static final String PROPERTY_QUALIFIER = "#";
	

	private MixupInterpreter interpreter;
	
	private VitalAnnotatorLoader annotatorLoader;
	
	private String docID = "doc01";
	
	private Set<String> inputTypes;
	
	private Set<String> inputProps;
	
	@Override
	public void init(NLPServerConfig config) throws StepInitializationException {
		super.init(config);
		
		long start = System.currentTimeMillis();
		
		log.info("Initializing minorthird workflow step...");
		
		inputTypes = new HashSet<String>(Arrays.asList(TEXT_BLOCK, SENTENCE, NOUN_PHRASE, VERB_PHRASE));
		inputProps = new HashSet<String>(Arrays.asList(SENTENCE_NUMBER));
		MinorthirdConfig cfg = config.getMinorthird();
		
		File mixupFile = new File(cfg.getMixupDir(), cfg.getMainMixupFile());
		
		File mixupDir = new File(cfg.getMixupDir());
		
		log.info("Mixup dir: {}", mixupDir.getAbsolutePath());
		
		annotatorLoader = new VitalAnnotatorLoader(mixupDir);
		
		log.info("Setting up interpreter with main mixup file: {}", mixupFile.getAbsolutePath());
		
		try {
			interpreter = new MixupInterpreter(new MixupProgram(mixupFile));
		} catch (Exception e) {
			log.error(e.getLocalizedMessage(), e);
			throw new StepInitializationException(e);
		}
		log.info("Minorthird step initialized properly, {}ms", System.currentTimeMillis() - start);
	
		
		
	}
	
	@Override
	public String getName() {
		return MINORTHIRD.getName();
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
			
			String content = doc.getTextBlocksContent();

			BasicTextBase textBase = new BasicTextBase(new DocumentTokenizer(doc));
			
			//full text goes here, but the tokenizer references back to the original doc
			
			textBase.loadDocument(docID, content);

			BasicTextLabels labels = new BasicTextLabels(textBase);
			
			labels.setAnnotatorLoader(annotatorLoader);
			
			PosAnnotator posAnnotator = new PosAnnotator(doc);

			posAnnotator.annotate(labels);
			
			copyAllOtherProperties(doc, labels);
			
			interpreter.eval(labels);
			

			Map<String, String> prefix2Package = new HashMap<String, String>();
			
			//collect all package prefixes
			for(String spanProperty : labels.getSpanProperties() ) {
				
				if(spanProperty.startsWith(PACKAGE_PREFIX)) {
					
					for( Iterator<Span> spansWithProperty = labels.getSpansWithProperty(spanProperty); spansWithProperty.hasNext(); ) {
						
						Span s = spansWithProperty.next();
						
						String prefix = spanProperty.substring(PACKAGE_PREFIX.length());
						
						String packageName = labels.getProperty(s, spanProperty);
						
						packageName = aposFilter(packageName);
						
						String currentValue = prefix2Package.get(prefix);
						if(currentValue != null && !currentValue.equals(packageName)) {
							throw new RuntimeException("Inconsistent package prefix usage across mixup - prefix: " + prefix + " oldV: " + currentValue + " newV: " + packageName);
						}
						
						prefix2Package.put(prefix, packageName);
						
					}
					
				}
				
				
			}
			
			Set<String> types = labels.getTypes();
			
			List<EntityLocation> entityLocations = new ArrayList<EntityLocation>();
			
			//first iteration to collect entity types, and create instances
			for(String type : types) {
				
				//filter out unwanted types
				if(inputTypes.contains(type)) continue;
				if(type.contains(PROPERTY_QUALIFIER)) continue;
				
				String clsName = resolveClass(prefix2Package, aposFilter(type));
				
				for(Span span : labels.getTypeSet(type, docID)) {
					
					//try to create an entity from span type
					Entity entity = createEntityFromType(clsName);
					
					if(entity == null) continue;
					
					int loChar = span.getLoChar();
					int hiChar = span.getHiChar();
					
					Object[] translateBlocksContentOffsetToSentenceOffset = doc.translateBlocksContentOffsetToSentenceOffset(loChar);
					
					Sentence sentence = (Sentence) translateBlocksContentOffsetToSentenceOffset[0];
					
					int sentenceOffset = ((Integer) translateBlocksContentOffsetToSentenceOffset[1]).intValue();
					
					int offset = doc.translateBlocksContentOffsetToBodyOffset(loChar);
					
					Integer end = doc.translateBlocksContentOffsetToBodyOffset(hiChar);

					if(end == null) {
						continue;
					}
					
					entity.setCategory(entity.getClass().getSimpleName());
					//don't set name property entity.setName(content.substring(loChar, hiChar));
					entity.setExtractSource("M3RD");
					entity.setRelevance(1F);
					String randomAlphanumeric = RandomStringUtils.randomAlphanumeric(16);
					entity.setUri(doc.getUri() + "#Entity_" + randomAlphanumeric);
					List<EntityInstance> entityInstances = new ArrayList<EntityInstance>();
					
					EntityInstance ei = new EntityInstance();
					ei.setUri(doc.getUri() + "#EntityInstance_" + randomAlphanumeric);
					ei.setExactString(content.substring(loChar, hiChar));
					ei.setLength(end-offset);
					ei.setLengthInSentence(hiChar - loChar);
					ei.setOffsetInSentence(sentenceOffset);
					ei.setOffset(offset);

					entityInstances.add(ei);
					
					entity.setEntityInstances(entityInstances);
					
					doc.getEntities().add(entity);
					
					sentence.getEntityInstances().add(ei);
					
					entityLocations.add(new EntityLocation(entity, loChar, hiChar));
					
				}
				
			}
			
			for(String type: types) {
				
				//filter out unwanted types
				if(inputTypes.contains(type)) continue;

				//second iteration to set properties
				if(!type.contains(PROPERTY_QUALIFIER)) continue;
				
				
				for(Span span : labels.getTypeSet(type, docID)) {

					String clsName = resolveClass(prefix2Package, aposFilter(type));
					
					Entity entity = findEnclosingEntity(entityLocations, clsName, span);
						
					if(entity != null) {
							
						String propertyName = clsName.substring(clsName.indexOf(PROPERTY_QUALIFIER) + PROPERTY_QUALIFIER.length());
							
						//JAVA HACK
						if(propertyName.equals("name")) {
							entity.setName(content.substring(span.getLoChar(), span.getHiChar()));
						} else {
							System.out.println("Unhandled entity name: " + propertyName);
						}
							
					}
					
				}
					
			}
			
			DocumentExtractor.updateDoc(model, doc);
			
		}
		
		return model;
	}

	private String resolveClass(Map<String, String> prefix2Package,
			String clsName) {

		int indexOfNSQualifier = clsName.indexOf(NS_QUALIFIER);
		
		if(indexOfNSQualifier >= 0 ) {
			
			String prefix = clsName.substring(0, indexOfNSQualifier);
			
			String packageName = prefix2Package.get(prefix);
			
			if(packageName == null) {
				
				throw new RuntimeException("No package registered for prefix: " + prefix);
				
			}
			
			return packageName + '.' + clsName.substring(indexOfNSQualifier + NS_QUALIFIER.length());
			
		}
		
		return clsName;
	}

	private String aposFilter(String input) {
		if(input.startsWith("'")) input = input.substring(1);
		if(input.endsWith("'")) input = input.substring(0, input.length()-1);
		return input;
	}

	private Entity findEnclosingEntity(List<EntityLocation> entityLocations, String clsName, Span span) {

		int loChar = span.getLoChar();
		int hiChar = span.getHiChar();
		
		int split = clsName.indexOf(PROPERTY_QUALIFIER);
		
		String className = clsName.substring(0, split);
		
		for(EntityLocation location : entityLocations) {
			
			if( location.loChar <= loChar  && hiChar <= location.hiChar ) {
				
				Entity entity = location.entity;
				if(entity.getClass().getCanonicalName().equals(className)) {
					return entity;
				}
				
			}
			
			
		}
		
		return null;
	}

	private Entity createEntityFromType(String cls) {

		
		try {
			
			Class<? extends Entity> clsObj = (Class<? extends Entity>) Class.forName(cls);
			
			//JAVA HACK
			Entity newInstance = new Entity();
			
			if(newInstance instanceof Entity) {
				return newInstance;
			}
			
		} catch (Exception e) {
			
		}
		
		return null;
	}

	private void copyAllOtherProperties(Document doc, BasicTextLabels labels) {

		int blockOffset = 0;
		
		Span docSpan = labels.getTextBase().documentSpan(docID);
		
		Map<String, Entity> instanceToParent = new HashMap<String, Entity>();
		
		for( Entity entity : doc.getEntities() ) {
			for(EntityInstance ei : entity.getEntityInstances()) {
				instanceToParent.put(ei.getUri(), entity);
			}
		}
		
		for( TextBlock b : doc.getTextBlocks() ) {
			
			Span blockSpan = docSpan.charIndexSubSpan(blockOffset, b.getText().length());
			
			labels.addToType(blockSpan,TEXT_BLOCK);
			
			for( Sentence s : b.getSentences() ) {
				
				Span sentenceSpan = docSpan.charIndexSubSpan(blockOffset + s.getStart(), blockOffset + s.getEnd());
				
				labels.addToType(sentenceSpan, SENTENCE);
				labels.setProperty(sentenceSpan, SENTENCE_NUMBER, "" + s.getSentenceNumber());

				for(EntityInstance ei : s.getEntityInstances()) {
					
					int offsetInSentence = ei.getOffsetInSentence();
					int lengthInSentence = ei.getLengthInSentence();
					Entity e = instanceToParent.get(ei.getUri());
					
					Span entitySpan = docSpan.charIndexSubSpan(blockOffset + s.getStart() + offsetInSentence, blockOffset + s.getStart() + offsetInSentence + lengthInSentence);
					
					String inputType = e.getExtractSource() + ":" + e.getCategory();
					
					inputTypes.add(inputType);
					
					labels.addToType(entitySpan, inputType);
					
				}
				
				for( PosTag t : s.getPosTags()) {
					inputTypes.add(t.getTag());
				}
				
				List<Token> tokens = s.getTokens();
				
				//chunker - compatible with np.mixup
				
				for( NounPhrase np : s.getNounPhrases() ) {
					
					Integer startTokenIndex = np.getStartTokenIndex();
					
					Integer endTokenIndex = np.getEndTokenIndex();
					
					int start = tokens.get(startTokenIndex).getStart();
					
					int end = tokens.get(endTokenIndex).getEnd();

					Span span = docSpan.charIndexSubSpan(blockOffset + s.getStart() + start, blockOffset + s.getStart() + end);

					//add token types?
					
					labels.addToType(span, NOUN_PHRASE);
					
					
				}
				
				
				for( VerbPhrase vp : s.getVerbPhrases() ) {
					
					Integer startTokenIndex = vp.getStartTokenIndex();
					
					Integer endTokenIndex = vp.getEndTokenIndex();
					
					int start = tokens.get(startTokenIndex).getStart();
					
					int end = tokens.get(endTokenIndex).getEnd();
					
					Span span = docSpan.charIndexSubSpan(blockOffset + s.getStart() + start, blockOffset + s.getStart() + end);
					
					labels.addToType(span, VERB_PHRASE);
					
					
				}
				
			}
			
			blockOffset += (b.getText().length() + 1);
			
		}
		
		labels.setAnnotatedBy("npchunks");
		
		
	}

	private static class EntityLocation {
		public Entity entity;
		
		public int loChar;
		
		public int hiChar;

		public EntityLocation(Entity entity, int loChar, int hiChar) {
			super();
			this.entity = entity;
			this.loChar = loChar;
			this.hiChar = hiChar;
		}
		
		
	}
	
}
