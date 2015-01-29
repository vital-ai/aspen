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

import ai.vital.aspen.groovy.nlp.domain.TagElement;
import ai.vital.aspen.groovy.nlp.domain.rdf.AbstractRDFConverter;
import ai.vital.flow.server.ontology.VitalOntology;

public class TagElementRDFConverter extends AbstractRDFConverter<TagElement> {

	@Override
	public Resource getRDFType() {
		return VitalOntology.TagElement;
	}

	@Override
	public TagElement fromRDF(Model m, Resource r) {
		
		TagElement te = new TagElement();
		te.setUri(r.getURI());
		te.setClosing(getBooleanLiteral(r, VitalOntology.isClosingTag, false));
		te.setEnd(getIntegerLiteral(r, VitalOntology.hasEndPosition, null));
		te.setOpening(getBooleanLiteral(r, VitalOntology.isOpeningTag, false));
		te.setStandalone(getBooleanLiteral(r, VitalOntology.isStandaloneTag, false));
		te.setStart(getIntegerLiteral(r, VitalOntology.hasStartPosition, null));
		te.setTag(getStringLiteral(r, VitalOntology.hasTagValue));
		
		
		return te;
	}

	@Override
	protected void fill(TagElement te, Model m, Resource r) {

		addBooleanLiteral(r, VitalOntology.isClosingTag, te.getClosing());
		addIntegerLiteral(r, VitalOntology.hasEndPosition, te.getEnd());
		addBooleanLiteral(r, VitalOntology.isOpeningTag, te.getOpening());
		addBooleanLiteral(r, VitalOntology.isStandaloneTag, te.getStandalone());
		addIntegerLiteral(r, VitalOntology.hasStartPosition, te.getStart());
		addStringLiteral(r, VitalOntology.hasTagValue, te.getTag());

	}

}
