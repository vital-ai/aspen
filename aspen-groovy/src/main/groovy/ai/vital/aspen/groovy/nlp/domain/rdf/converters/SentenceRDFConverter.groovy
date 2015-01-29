/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.domain.rdf.converters;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

import ai.vital.aspen.groovy.nlp.domain.AbbreviationInstance;
import ai.vital.aspen.groovy.nlp.domain.EntityInstance;
import ai.vital.aspen.groovy.nlp.domain.NounPhrase;
import ai.vital.aspen.groovy.nlp.domain.PosTag;
import ai.vital.aspen.groovy.nlp.domain.Sentence;
import ai.vital.aspen.groovy.nlp.domain.Token;
import ai.vital.aspen.groovy.nlp.domain.VerbPhrase;
import ai.vital.aspen.groovy.nlp.domain.rdf.AbstractRDFConverter;
import ai.vital.flow.server.ontology.VitalOntology;

public class SentenceRDFConverter extends AbstractRDFConverter<Sentence> {

	@Override
	public Resource getRDFType() {
		return VitalOntology.Sentence;
	}
	
	@Override
	public void delete(String uri, Model m) {
		
		deleteListPropertyAsEdges(m, m.getResource(uri), VitalOntology.Edge_hasNounPhrase, AbstractRDFConverter.converters.get(NounPhrase.class));
		deleteListPropertyAsEdges(m, m.getResource(uri), VitalOntology.Edge_hasPosTag, AbstractRDFConverter.converters.get(PosTag.class));
		deleteListPropertyAsEdges(m, m.getResource(uri), VitalOntology.Edge_hasToken, AbstractRDFConverter.converters.get(Token.class));
		deleteListPropertyAsEdges(m, m.getResource(uri), VitalOntology.Edge_hasVerbPhrase, AbstractRDFConverter.converters.get(VerbPhrase.class));
		
		//keep the destinations
		deleteListPropertyAsEdges(m, m.getResource(uri), VitalOntology.Edge_hasSentenceEntityInstance, null);
		deleteListPropertyAsEdges(m, m.getResource(uri), VitalOntology.Edge_hasSentenceAbbreviationInstance, null);
		
		super.delete(uri, m);
	}

	@Override
	public Sentence fromRDF(Model m, Resource r) {
		
		Sentence s = new Sentence();
		s.setUri(r.getURI());
		s.setAbbreviationInstances(getListPropertyFromEdges(AbbreviationInstance.class, m, r, VitalOntology.Edge_hasSentenceAbbreviationInstance));
		s.setEnd(getIntegerLiteral(r, VitalOntology.hasEndPosition, null));
		s.setEntityInstances(getListPropertyFromEdges(EntityInstance.class, m, r, VitalOntology.Edge_hasSentenceEntityInstance));
		s.setNounPhrases(getListPropertyFromEdges(NounPhrase.class, m, r, VitalOntology.Edge_hasNounPhrase));
		s.setPosTags(getListPropertyFromEdges(PosTag.class, m, r, VitalOntology.Edge_hasPosTag));
		s.setSentenceNumber(getIntegerLiteral(r, VitalOntology.hasSentenceNumber, null));
		s.setStart(getIntegerLiteral(r, VitalOntology.hasStartPosition, null));
		s.setTokens(getListPropertyFromEdges(Token.class, m, r, VitalOntology.Edge_hasToken));
		s.setVerbPhrases(getListPropertyFromEdges(VerbPhrase.class, m, r, VitalOntology.Edge_hasVerbPhrase));
		return s;
	}

	@Override
	protected void fill(Sentence s, Model m, Resource r) {

		addListPropertyAsEdges(m, r, VitalOntology.Edge_hasSentenceAbbreviationInstance, VitalOntology.Edge_hasAbbreviationInstanceURIBase, s.getAbbreviationInstances());
		addIntegerLiteral(r, VitalOntology.hasEndPosition, s.getEnd());
		addListPropertyAsEdges(m, r, VitalOntology.Edge_hasSentenceEntityInstance, VitalOntology.Edge_hasSentenceEntityInstanceURIBase, s.getEntityInstances());
		addListPropertyAsEdges(m, r, VitalOntology.Edge_hasNounPhrase, VitalOntology.Edge_hasNounPhraseURIBase, s.getNounPhrases());
		addListPropertyAsEdges(m, r, VitalOntology.Edge_hasPosTag, VitalOntology.Edge_hasPosTagURIBase, s.getPosTags());
		addIntegerLiteral(r, VitalOntology.hasSentenceNumber, s.getSentenceNumber());
		addIntegerLiteral(r, VitalOntology.hasStartPosition, s.getStart());
		addListPropertyAsEdges(m, r, VitalOntology.Edge_hasToken, VitalOntology.Edge_hasTokenURIBase, s.getTokens());
		addListPropertyAsEdges(m, r, VitalOntology.Edge_hasVerbPhrase, VitalOntology.Edge_hasVerbPhraseURIBase, s.getVerbPhrases());

	}

}
