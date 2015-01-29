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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;
import org.codehaus.jackson.map.ObjectMapper
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;


import edu.cmu.minorthird.text.BasicTextBase;
import edu.cmu.minorthird.text.BasicTextLabels;
import edu.cmu.minorthird.text.CharAnnotation;
import edu.cmu.minorthird.text.POSTagger;
import edu.cmu.minorthird.text.Span;
import edu.cmu.minorthird.text.TextToken;
import edu.cmu.minorthird.text.Tokenizer;
import edu.cmu.minorthird.text.mixup.Mixup.ParseException;
import edu.cmu.minorthird.text.mixup.MixupInterpreter;
import edu.cmu.minorthird.text.mixup.MixupProgram;

import ai.vital.domain.Document;
import ai.vital.domain.Edge_hasEntity;
import ai.vital.domain.Edge_hasEntityInstance;
import ai.vital.domain.Edge_hasSentenceEntityInstance;
import ai.vital.domain.Entity;
import ai.vital.domain.EntityInstance;
import ai.vital.domain.NounPhrase;
import ai.vital.domain.PosTag;
import ai.vital.domain.Sentence;
import ai.vital.domain.TextBlock;
import ai.vital.domain.Token;
import ai.vital.domain.VerbPhrase;
import ai.vital.aspen.groovy.nlp.config.MinorthirdConfig;
import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.aspen.groovy.nlp.m3rd.DocumentTokenizer_VS;
import ai.vital.aspen.groovy.nlp.m3rd.PosAnnotator_VS;
import ai.vital.aspen.groovy.nlp.m3rd.VitalAnnotatorLoader;
import ai.vital.aspen.groovy.nlp.model.DocumentUtils;
import ai.vital.aspen.groovy.nlp.model.EdgeUtils;
import ai.vital.aspen.groovy.nlp.model.PosTagsUtils;
import ai.vital.aspen.groovy.nlp.model.TokenUtils;
import ai.vital.flow.server.ontology.VitalOntology;
import ai.vital.aspen.groovy.nlp.workflowstep.MinorthirdWorkflowStep.EntityLocation;
import ai.vital.flow.server.utils.JSONUtils;
import ai.vital.vitalsigns.model.container.Payload;
import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepV2Impl;

public class MinorthirdWorkflowStep_VS extends WorkflowStepV2Impl<NLPServerConfig> {

	public static boolean DEBUG = false;
	
	private static final String PACKAGE_PREFIX = "package_";

	private static final String NOUN_PHRASE = "NP";
	
	private static final String VERB_PHRASE = "VP";
	
	private static final String SENTENCE_NUMBER = "sentenceNumber";

	private static final String TEXT_BLOCK = "textBlock";
	
	private static final String SENTENCE = "sentence";

	public final static StepName MINORTHIRD_VS = new StepName("minorthird_vs");
	
	private final static Logger log = LoggerFactory.getLogger(MinorthirdWorkflowStep_VS.class);

	private static final String NS_QUALIFIER = ":";
	
	private static final String PROPERTY_QUALIFIER = "#";
	
	
	private MixupInterpreter interpreter;
	
	private VitalAnnotatorLoader annotatorLoader;
	
	private String docID = "doc01";
	
	private Set<String> inputTypes;
	
	private Set<String> inputProps;
	
	private ObjectMapper mapper = new ObjectMapper();
	
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
		
		try {
			reloadMainMixup();
		} catch (Exception e) {
			log.error(e.getLocalizedMessage(), e);
			throw new StepInitializationException(e);
		}
		
		log.info("Minorthird step initialized properly, {}ms", System.currentTimeMillis() - start);
	
		
		
	}
	
	private void reloadMainMixup() throws Exception {

		File mixupFile = new File(config.getMinorthird().getMixupDir(), config.getMinorthird().getMainMixupFile());
				
		log.info("Setting up interpreter with main mixup file: {}", mixupFile.getAbsolutePath());
		interpreter = new MixupInterpreter(new MixupProgram(mixupFile));
		
	}
	
	@Override
	public String getName() {
		return MINORTHIRD_VS.getName();
	}

	@Override
	public void processPayload(Payload payload)
			throws ai.vital.workflow.IWorkflowStep.WorkflowHaltException,
			ai.vital.workflow.IWorkflowStep.ProcessflowHaltException,
			Exception {

		Map<String, Object> context = JSONUtils.getContextMap(payload);
		
		if(context.get("refreshMixup") == true) {
			
			log.info("Received refreshMixup request - refreshing...");
			try {
				reloadMainMixup();
				log.info("Refresh success");
			} catch(Exception e) {
				log.error(e.getLocalizedMessage(), e);
			}
			return;
			
		}
		
		String mixupFileName = context.get("mixupFileName");
		
		
		MixupInterpreter _interpreter = null;
		
		if(mixupFileName) {
			
			_interpreter = null;
			
			File mixupFile = new File(config.getMinorthird().getMixupDir(), mixupFileName);
			
			if(!mixupFile.exists()) throw new Exception("Mixup file: ${mixupFileName} not found.");
			
			_interpreter = new MixupInterpreter(new MixupProgram(mixupFile));
			
		} else {
		
			_interpreter = interpreter
		
		}
		
		
		for( Document doc : payload.iterator(Document.class) ) {
			
			String docUri = doc.getURI();
			
			log.info("Processing document {} ...", docUri);
			
			String content = DocumentUtils.getTextBlocksContent(doc);

			long start = System.currentTimeMillis();
			
			BasicTextBase textBase = new BasicTextBase(new DocumentTokenizer_VS(doc));
			
			long stop = System.currentTimeMillis();
			
			log.info("Text base created, {}ms", stop-start);
			
			//full text goes here, but the tokenizer references back to the original doc
			
			start = System.currentTimeMillis();
			textBase.loadDocument(docID, content);
			stop = System.currentTimeMillis();
			log.info("Document loading time into textbase: {}ms", stop-start);

			BasicTextLabels labels = new BasicTextLabels(textBase);
			
			labels.setAnnotatorLoader(annotatorLoader);
			
			PosAnnotator_VS posAnnotator = new PosAnnotator_VS(doc);

			start = System.currentTimeMillis();
			posAnnotator.annotate(labels);
			stop= System.currentTimeMillis();
			log.info("M3RD POS annotator time: {}ms", stop-start);
			
			start = System.currentTimeMillis();
			copyAllOtherProperties(doc, labels);
			stop = System.currentTimeMillis();
			log.info("All properties copied in time: {}ms", stop-start);
			
			
			start = System.currentTimeMillis();
			_interpreter.eval(labels);
			stop = System.currentTimeMillis();
			log.info("Mixup evaluation time: {}ms", stop-start);
			
			start = System.currentTimeMillis();
			
			
			Map<String, String> prefix2Package = new HashMap<String, String>();
			
			Set<String> types = labels.getTypes();
			
			//collect all package prefixes
			//bad method
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
			
			for(String type : types) {
				
				String t = aposFilter(type);
				
				if(t.startsWith(PACKAGE_PREFIX)) {
					
					String _def = t.substring(PACKAGE_PREFIX.length());
					
					int indexOfQ = _def.indexOf(NS_QUALIFIER);
					
					if( indexOfQ < 1 ) continue;
					
					String prefix = _def.substring(0, indexOfQ);
					
					String packageName = _def.substring(indexOfQ + 1);
					
					String currentValue = prefix2Package.get(prefix);
					if(currentValue != null && !currentValue.equals(packageName)) {
						throw new RuntimeException("Inconsistent package prefix usage across mixup - prefix: " + prefix + " oldV: " + currentValue + " newV: " + packageName);
					}
		
					prefix2Package.put(prefix, packageName);
					
				}
				
			}
			
			
			
			List<EntityLocation> entityLocations = new ArrayList<EntityLocation>();
			
			//first iteration to collect entity types, and create instances
			for(String type : types) {
				
				//filter out unwanted types
				if(inputTypes.contains(type)) continue;
				
				if(type.contains(PROPERTY_QUALIFIER)) continue;
				
				if(aposFilter(type).startsWith(PACKAGE_PREFIX)) continue;	
				
				String clsName = resolveClass(prefix2Package, aposFilter(type));
				
				List<Span> spans = new ArrayList<Span>(labels.getTypeSet(type, docID));
				
				spans = filterOutInnerSpans(spans);
				
				for(Span span : spans) {
					
					//try to create an entity from span type
					Entity entity = createEntityFromType(clsName);
					
					if(entity == null) continue;
					
					int loChar = -1;
					int hiChar = -1;
					try {
						loChar = span.getLoChar();
						hiChar = span.getHiChar();
					} catch(Exception e1) {
						log.error(e1.getLocalizedMessage(), e1);
						continue;
					}
					
					if(loChar >= hiChar) {
						continue;
					}
					
					Object[] translateBlocksContentOffsetToSentenceOffset = DocumentUtils.translateBlocksContentOffsetToSentenceOffset(doc, loChar);
					
					Sentence sentence = (Sentence) translateBlocksContentOffsetToSentenceOffset[0];
					
					int sentenceOffset = ((Integer) translateBlocksContentOffsetToSentenceOffset[1]).intValue();
					
					int offset = DocumentUtils.translateBlocksContentOffsetToBodyOffset(doc, loChar);
					
					Integer end = DocumentUtils.translateBlocksContentOffsetToBodyOffset(doc, hiChar);

					if(end == null) {
						continue;
					}
					
					entity.category = entity.getClass().getSimpleName();
					//entity.name = content.substring(loChar, hiChar);
					entity.extractSource = "M3RD";
					entity.relevance = 1F;
					String randomAlphanumeric = RandomStringUtils.randomAlphanumeric(16);
					entity.setURI(doc.getURI() + "#Entity_" + randomAlphanumeric);
					
//					List<EntityInstance> entityInstances = new ArrayList<EntityInstance>();
					
					EntityInstance ei = new EntityInstance();
					ei.setURI(doc.getURI() + "#EntityInstance_" + randomAlphanumeric);
					ei.exactString = content.substring(loChar, hiChar);
					ei.length = end-offset;
					ei.lengthInSentence = hiChar - loChar;
					ei.offsetInSentence = sentenceOffset;
					ei.offset = offset;

//					entityInstances.add(ei);
//					
//					entity.setEntityInstances(entityInstances);
//					
//					doc.getEntities().add(entity);
//					
//					sentence.getEntityInstances().add(ei);
					
					payload.putGraphObjects(Arrays.asList(entity, ei));
					
					//links!
					payload.putGraphObjects(EdgeUtils.createEdges(doc, Arrays.asList(entity), Edge_hasEntity, VitalOntology.Edge_hasEntityURIBase));
					payload.putGraphObjects(EdgeUtils.createEdges(entity, Arrays.asList(ei), Edge_hasEntityInstance, VitalOntology.Edge_hasEntityInstanceURIBase));
					payload.putGraphObjects(EdgeUtils.createEdges(sentence, Arrays.asList(ei), Edge_hasSentenceEntityInstance, VitalOntology.Edge_hasSentenceEntityInstanceURIBase));
				
					
					entityLocations.add(new EntityLocation(entity, loChar, hiChar));
						
				}
				
			}
			
			if(DEBUG) {
				
				println("token props:");
				for(String tp : labels.getTokenProperties()) {
					
					Span docSpan = labels.getTextBase().documentSpan(docID);
					
					for(int i = 0 ; i < docSpan.size(); i++) {
						
						edu.cmu.minorthird.text.Token t = docSpan.getToken(i);
						
						String p = labels.getProperty(t, tp);
						if(p) {
							println("${tp} ${t} ${p}");
						}
					}
					
				}
				println("\nspan types:");
				for( String type : types) {
				
					for(Span span : labels.getTypeSet(type, docID)) {
						println("${type} ${span}");
					}
						
				}
				
				println("\nspan props:");
				for(String spanProp : labels.getSpanProperties()) {
			
					for(Span span : labels.getSpansWithProperty(spanProp, docID)) {
						
						String propertyValue = labels.getProperty(span, spanProp);
						
						println("${spanProp} ${span} ${propertyValue}");
						
					}
							
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
							
						entity[propertyName] = content.substring(span.getLoChar(), span.getHiChar());
							
					}
					
				}

			}
			
			for(String spanProp : labels.getSpanProperties()) {
				
				//second iteration to set properties
				if(!spanProp.contains(PROPERTY_QUALIFIER)) continue;
				
				for(Span span : labels.getSpansWithProperty(spanProp, docID)) {
					
					String clsName = resolveClass(prefix2Package, aposFilter(spanProp));
					
					Entity entity = findEnclosingEntity(entityLocations, clsName, span);
					
					if(entity != null) {
						
						String propertyName = clsName.substring(clsName.indexOf(PROPERTY_QUALIFIER) + PROPERTY_QUALIFIER.length());

						String propertyValue = labels.getProperty(span, spanProp);
												
						entity[propertyName] = propertyValue;
						
					}
					
				}
				
			}
			
			stop = System.currentTimeMillis();
			log.info("Entities assignment time: {}ms", stop-start);
			
		}
		
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
				
		Class<? extends Entity> clsObj = null;
						
		try {
							
			clsObj = (Class<? extends Entity>) Class.forName(cls);
							
			//JAVA HACK
			Entity newInstance = clsObj.newInstance();
							
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
				instanceToParent.put(ei.getURI(), entity);
			}
		}
		
		for( TextBlock b : doc.getTextBlocks() ) {
			
			int textLength = b.text.length();
			
			Integer textBlockLength = b.textBlockLength;
			
			Span blockSpan = docSpan.charIndexSubSpan(blockOffset, textLength);
			
			labels.addToType(blockSpan,TEXT_BLOCK);
			
			for( Sentence s : b.getSentences() ) {
				
				Span sentenceSpan = docSpan.charIndexSubSpan(blockOffset + s.startPosition, blockOffset + s.endPosition);
				
				labels.addToType(sentenceSpan, SENTENCE);
				labels.setProperty(sentenceSpan, SENTENCE_NUMBER, "" + s.sentenceNumber);

				for(EntityInstance ei : s.getSentenceEntityInstances()) {
					
					int offsetInSentence = ei.offsetInSentence;
					int lengthInSentence = ei.lengthInSentence;
					Entity e = instanceToParent.get(ei.getURI());
					
					Span entitySpan = docSpan.charIndexSubSpan(blockOffset + s.startPosition + offsetInSentence, blockOffset + s.startPosition + offsetInSentence + lengthInSentence);
					
					String inputType = e.extractSource + ":" + e.category;
					
					inputTypes.add(inputType);
					
					labels.addToType(entitySpan, inputType);
					
				}
				
				List<PosTag> posTags = PosTagsUtils.getPosTags(s);
				
				for( PosTag t : posTags) {
					inputTypes.add(t.tagValue);
				}
				
				List<Token> tokens = TokenUtils.getTokens(s);
				
				for( NounPhrase np : s.getNounPhrases() ) {
					
					Integer startTokenIndex = np.startTokenIndex;
					
					Integer endTokenIndex = np.endTokenIndex;
					
					int start = tokens.get(startTokenIndex).startPosition;
					
					int end = tokens.get(endTokenIndex).endPosition;

					Span span = docSpan.charIndexSubSpan(blockOffset + s.startPosition + start, blockOffset + s.startPosition + end);

					labels.addToType(span, NOUN_PHRASE);
					
					
				}
				
				
				for( VerbPhrase vp : s.getVerbPhrases() ) {
					
					Integer startTokenIndex = vp.startTokenIndex;
					
					Integer endTokenIndex = vp.endTokenIndex;
					
					int start = tokens.get(startTokenIndex).startPosition;
					
					int end = tokens.get(endTokenIndex).endPosition;
					
					Span span = docSpan.charIndexSubSpan(blockOffset + s.startPosition + start, blockOffset + s.startPosition + end);
					
					labels.addToType(span, VERB_PHRASE);
					
					
				}
				
			}
			
			blockOffset += (b.text.length() + 1);
			
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
	
	private List<Span> filterOutInnerSpans(List<Span> spans) {
		
		//sort them by occurrence

		List<Span> filtered = new ArrayList<Span>();
		
		for(int i = 0 ; i < spans.size(); i ++) {
			
			Span inputSpan = spans.get(i);
			
			int loChar = -1;
			int hiChar = -1;
			try {
				loChar = inputSpan.getLoChar();
				hiChar = inputSpan.getHiChar();
			} catch(Exception e1) {
				log.error(e1.getLocalizedMessage(), e1);
				continue;
			}

			
			boolean passed = true;
			
			for(int j = 0; j < spans.size(); j++) {
				
				if(i == j) continue;
				
				Span s = spans.get(j);

				int loChar2 = -1;
				int hiChar2 = -1;
				try {
					loChar2 = s.getLoChar();
					hiChar2 = s.getHiChar();
				} catch(Exception e1) {
					log.error(e1.getLocalizedMessage(), e1);
					continue;
				}
								
				if(loChar2 <= loChar && hiChar <= hiChar2) {
					passed = false;
					break;
				}
				
			}
			
			if(passed) {
				filtered.add(inputSpan);
			}
			
		}
				
		
		return filtered;
		
	} 
}
