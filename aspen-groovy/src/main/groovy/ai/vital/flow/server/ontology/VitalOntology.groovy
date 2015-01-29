/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.flow.server.ontology;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class VitalOntology {

	public final static String NS = "http://vital.ai/ontology/vital.owl#";
	
	public final static String CORE_NS = "http://vital.ai/ontology/vital-core.owl#";
	
	
	
	public final static String EDGE_NS = "http://vital.ai/nlp/edge/";
	
	//NODES
	public final static Resource Abbreviation = ResourceFactory.createResource(NS + "Abbreviation");
	
	public final static Resource AbbreviationInstance = ResourceFactory.createResource(NS + "AbbreviationInstance");
	
	public final static Resource Annotation = ResourceFactory.createResource(NS + "Annotation");
	
	public final static Resource Document = ResourceFactory.createResource(NS + "Document");
	
	public final static Resource Entity = ResourceFactory.createResource(NS + "Entity");
	
	public final static Resource EntityInstance = ResourceFactory.createResource(NS + "EntityInstance");
	
	public final static String EntityInstanceURIBase = "http://vital.ai/nlp/entity-instance/";
	
	public final static Resource NounPhrase = ResourceFactory.createResource(NS + "NounPhrase");
	
	public final static Resource NormalizedEntity = ResourceFactory.createResource(NS + "NormalizedEntity");
	
	public final static Resource NormalizedTopic = ResourceFactory.createResource(NS + "NormalizedTopic");
	
	public final static Resource PosTag = ResourceFactory.createResource(NS + "PosTag");
	
	public final static Resource VerbPhrase = ResourceFactory.createResource(NS + "VerbPhrase");
	
	public final static Resource Sentence = ResourceFactory.createResource(NS + "Sentence");
	
	public final static Resource TagElement = ResourceFactory.createResource(NS + "TagElement");
	
	public static final Resource TextBlock = ResourceFactory.createResource(NS + "TextBlock");
	
	public final static Resource Token = ResourceFactory.createResource(NS + "Token");
	
	public final static Resource Topic = ResourceFactory.createResource(NS + "Topic");
	
	public static final Resource Category = ResourceFactory.createResource(NS + "Category");
	
	
	//EDGES
	public final static Resource Edge_hasNormalizedEntity = ResourceFactory.createResource(NS + "Edge_hasNormalizedEntity");
	public static final String Edge_hasNormalizedEntityURIBase = EDGE_NS + "hasNormalizedEntity/";
	
	public final static Resource Edge_hasNormalizedTopic = ResourceFactory.createResource(NS + "Edge_hasNormalizedTopic");
	public static final String Edge_hasNormalizedTopicURIBase = EDGE_NS + "hasNormalizedTopic/";
	
	public static final Resource Edge_hasEntityInstance = ResourceFactory.createResource(NS + "Edge_hasEntityInstance");
	public static final String Edge_hasEntityInstanceURIBase = EDGE_NS + "hasEntityInstance/";
	
	public static final Resource Edge_hasEntity = ResourceFactory.createResource(NS + "Edge_hasEntity");
	public static final String Edge_hasEntityURIBase = EDGE_NS + "hasEntity/";
	
	public static final Resource Edge_hasTopic = ResourceFactory.createResource(NS + "Edge_hasTopic");
	public static final String Edge_hasTopicURIBase = EDGE_NS + "hasTopic/";
	
	public static final Resource Edge_hasTextBlock = ResourceFactory.createResource(NS + "Edge_hasTextBlock");
	public static final String Edge_hasTextBlockURIBase = EDGE_NS + "hasTextBlock/";
	
	public static final Resource Edge_hasContent = ResourceFactory.createResource(NS + "Edge_hasContent");
	public static final String Edge_hasContentURIBase = EDGE_NS + "hasContent/";
	
	public static final Resource Edge_hasSentence = ResourceFactory.createResource(NS + "Edge_hasSentence");
	public static final String Edge_hasSentenceURIBase = EDGE_NS + "hasSentence/";

	public static final Resource Edge_hasTagElement = ResourceFactory.createResource(NS + "Edge_hasTagElement");
	public static final String Edge_hasTagElementURIBase = EDGE_NS + "hasTagElement/";
	
	public static final Resource Edge_hasNounPhrase = ResourceFactory.createResource(NS + "Edge_hasNounPhrase");
	public static final String Edge_hasNounPhraseURIBase = EDGE_NS + "hasNounPhrase/";
	
	public static final Resource Edge_hasPosTag = ResourceFactory.createResource(NS + "Edge_hasPosTag");
	public static final String Edge_hasPosTagURIBase = EDGE_NS + "hasPosTag/";
	
	public static final Resource Edge_hasToken = ResourceFactory.createResource(NS + "Edge_hasToken");
	public static final String Edge_hasTokenURIBase = EDGE_NS + "hasToken/";
	
	public static final Resource Edge_hasVerbPhrase = ResourceFactory.createResource(NS + "Edge_hasVerbPhrase");
	public static final String Edge_hasVerbPhraseURIBase = EDGE_NS + "hasVerbPhrase/";
	
	public static final Resource Edge_hasSentenceEntityInstance = ResourceFactory.createResource(NS + "Edge_hasSentenceEntityInstance");
	public static final String Edge_hasSentenceEntityInstanceURIBase = EDGE_NS + "SentenceEntityInstance/";
	
	public static final Resource Edge_hasAbbreviation = ResourceFactory.createResource(NS + "Edge_hasAbbreviation");
	public static final String Edge_hasAbbreviationURIBase = EDGE_NS + "hasAbbreviation/";
	
	public static final Resource Edge_hasAbbreviationInstance = ResourceFactory.createResource(NS + "Edge_hasAbbreviationInstance");
	public static final String Edge_hasAbbreviationInstanceURIBase = EDGE_NS + "hasAbbreviationInstance/";
	
	public static final Resource Edge_hasSentenceAbbreviationInstance = ResourceFactory.createResource(NS + "Edge_hasSentenceAbbreviationInstance");
	public static final String Edge_hasSentenceAbbreviationInstanceURIBase = EDGE_NS + "hasSentenceAbbreviationInstance/";
	
	public static final Resource Edge_hasAnnotation = ResourceFactory.createResource(NS + "Edge_hasAnnotation");
	public static final String Edge_hasAnnotationURIBase = EDGE_NS + "hasAnnotation/";
	
	public static final Resource Edge_hasCategory = ResourceFactory.createResource(NS + "Edge_hasCategory");
	public static final String Edge_hasCategoryURIBase = EDGE_NS + "hasCategory/";
	
	//PROPERTIES

	public final static Property hasName = ResourceFactory.createProperty(NS + "hasName");

	public final static Property hasNormalizedName = ResourceFactory.createProperty(NS + "hasNormalizedName");

	public final static Property hasExtractSource = ResourceFactory.createProperty(NS + "hasExtractSource");

	public final static Property hasCategory = ResourceFactory.createProperty(NS + "hasCategory");

	public final static Property hasRelevance = ResourceFactory.createProperty(NS + "hasRelevance");

	public final static Property hasText = ResourceFactory.createProperty(NS + "hasText");

	public final static Property hasEdgeSource = ResourceFactory.createProperty(CORE_NS + "hasEdgeSource");

	public final static Property hasEdgeDestination = ResourceFactory.createProperty(CORE_NS + "hasEdgeDestination");

	public final static Property hasClassifierName = ResourceFactory.createProperty(NS + "hasClassifierName");
	
	//public final static Property hasCategoryName = ResourceFactory.createProperty(NS + "hasCategoryName");

	public final static Property hasScore = ResourceFactory.createProperty(NS + "hasScore");

	public final static Property hasOffset = ResourceFactory.createProperty(NS + "hasOffset");
	
	public final static Property hasLength = ResourceFactory.createProperty(NS + "hasLength");

	public final static Property hasListIndex = ResourceFactory.createProperty(CORE_NS + "hasListIndex");

	public final static Property hasExactString = ResourceFactory.createProperty(NS + "hasExactString");
	
	
	public final static Property hasShortname = ResourceFactory.createProperty(NS + "hasShortname");
	
	public final static Property hasTicker = ResourceFactory.createProperty(NS + "hasTicker");
	
	public final static Property hasSymbol = ResourceFactory.createProperty(NS + "hasSymbol");

	public final static Property hasBody = ResourceFactory.createProperty(NS + "hasBody");
	
	public final static Property hasTitle = ResourceFactory.createProperty(NS + "hasTitle");

	public final static Property hasExtractedTitle = ResourceFactory.createProperty(NS + "hasExtractedTitle");

	public final static Property hasExtractedText = ResourceFactory.createProperty(NS + "hasExtractedText");

	public final static Property hasEndTokenIndex = ResourceFactory.createProperty(NS + "hasEndTokenIndex");
	
	public final static Property hasStartTokenIndex = ResourceFactory.createProperty(NS + "hasStartTokenIndex");

	public final static Property hasTextBlockLength = ResourceFactory.createProperty(NS + "hasTextBlockLength");

	public final static Property hasTextBlockOffset = ResourceFactory.createProperty(NS + "hasTextBlockOffset");

	public final static Property isClosingTag = ResourceFactory.createProperty(NS + "isClosingTag");

	public final static Property isOpeningTag = ResourceFactory.createProperty(NS + "isOpeningTag");

	public final static Property hasEndPosition = ResourceFactory.createProperty(NS + "hasEndPosition");
	
	public final static Property hasStartPosition = ResourceFactory.createProperty(NS + "hasStartPosition");

	public final static Property isStandaloneTag = ResourceFactory.createProperty(NS + "isStandaloneTag");

	public final static Property hasTagValue = ResourceFactory.createProperty(NS + "hasTagValue");

	public final static Property hasTransformationVector = ResourceFactory.createProperty(NS + "hasTransformationVector");

	public final static Property hasSentenceNumber = ResourceFactory.createProperty(NS + "hasSentenceNumber");

	public final static Property hasConfidence = ResourceFactory.createProperty(NS + "hasConfidence");

	public final static Property hasTokenText = ResourceFactory.createProperty(NS + "hasTokenText");

	public final static Property hasLengthInSentence = ResourceFactory.createProperty(NS + "hasLengthInSentence");

	public final static Property hasOffsetInSentence = ResourceFactory.createProperty(NS + "hasOffsetInSentence");

	public final static Property hasLongForm = ResourceFactory.createProperty(NS + "hasLongForm");
	
	public final static Property hasLongFormStart = ResourceFactory.createProperty(NS + "hasLongFormStart");
	
	public final static Property hasLongFormEnd = ResourceFactory.createProperty(NS + "hasLongFormEnd");
	
	public final static Property hasShortForm = ResourceFactory.createProperty(NS + "hasShortForm");
	
	public final static Property hasShortFormStart = ResourceFactory.createProperty(NS + "hasShortFormStart");
	
	public final static Property hasShortFormEnd = ResourceFactory.createProperty(NS + "hasShortFormEnd");
	
	public final static Property hasAnnotationName = ResourceFactory.createProperty(NS + "hasAnnotationName");
	
	public final static Property hasAnnotationValue = ResourceFactory.createProperty(NS + "hasAnnotationValue");

	public final static Property hasUrl = ResourceFactory.createProperty(NS + "hasUrl");

	

	

	

	
}
