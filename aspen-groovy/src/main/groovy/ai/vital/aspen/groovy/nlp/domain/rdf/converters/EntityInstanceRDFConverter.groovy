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

import ai.vital.aspen.groovy.nlp.domain.EntityInstance;
import ai.vital.aspen.groovy.nlp.domain.rdf.AbstractRDFConverter;
import ai.vital.flow.server.ontology.VitalOntology;

public class EntityInstanceRDFConverter extends
		AbstractRDFConverter<EntityInstance> {

	@Override
	public Resource getRDFType() {
		return VitalOntology.EntityInstance;
	}

	@Override
	public EntityInstance fromRDF(Model model, Resource s) {
		EntityInstance ei = new EntityInstance();
		ei.setUri(s.getURI());
		ei.setExactString(getStringLiteral(s, VitalOntology.hasExactString));
		ei.setLength(getIntegerLiteral(s, VitalOntology.hasLength, null));
		ei.setOffset(getIntegerLiteral(s, VitalOntology.hasOffset, null));
		ei.setLengthInSentence(getIntegerLiteral(s, VitalOntology.hasLengthInSentence, null));
		ei.setOffsetInSentence(getIntegerLiteral(s, VitalOntology.hasOffsetInSentence, null));
		return ei;
	}

	@Override
	protected void fill(EntityInstance ei, Model m, Resource r) {

		addStringLiteral(r, VitalOntology.hasExactString, ei.getExactString());
		addIntegerLiteral(r, VitalOntology.hasLength, ei.getLength());
		addIntegerLiteral(r, VitalOntology.hasOffset, ei.getOffset());
		addIntegerLiteral(r, VitalOntology.hasLengthInSentence, ei.getLengthInSentence());
		addIntegerLiteral(r, VitalOntology.hasOffsetInSentence, ei.getOffsetInSentence());
	}

}
