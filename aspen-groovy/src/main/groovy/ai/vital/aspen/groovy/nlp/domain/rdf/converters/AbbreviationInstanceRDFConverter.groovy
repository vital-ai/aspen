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
import ai.vital.aspen.groovy.nlp.domain.rdf.AbstractRDFConverter;
import ai.vital.flow.server.ontology.VitalOntology;

public class AbbreviationInstanceRDFConverter extends
		AbstractRDFConverter<AbbreviationInstance> {

	@Override
	public Resource getRDFType() {
		return VitalOntology.AbbreviationInstance;
	}

	@Override
	public AbbreviationInstance fromRDF(Model m, Resource r) {
		
		AbbreviationInstance a = new AbbreviationInstance();
		a.setUri(r.getURI());
		a.setLongForm(getStringLiteral(r, VitalOntology.hasLongForm));
		a.setLongFormEnd(getIntegerLiteral(r, VitalOntology.hasLongFormEnd, null));
		a.setLongFormStart(getIntegerLiteral(r, VitalOntology.hasLongFormStart, null));
		a.setShortForm(getStringLiteral(r, VitalOntology.hasShortForm));
		a.setShortFormEnd(getIntegerLiteral(r, VitalOntology.hasShortFormEnd, null));
		a.setShortFormStart(getIntegerLiteral(r, VitalOntology.hasShortFormStart, null));
		return a;
	}

	@Override
	protected void fill(AbbreviationInstance a, Model m, Resource r) {

		addStringLiteral(r, VitalOntology.hasLongForm, a.getLongForm());
		addIntegerLiteral(r, VitalOntology.hasLongFormEnd, a.getLongFormEnd());
		addIntegerLiteral(r, VitalOntology.hasLongFormStart, a.getLongFormStart());

		addStringLiteral(r, VitalOntology.hasShortForm, a.getShortForm());
		addIntegerLiteral(r, VitalOntology.hasShortFormEnd, a.getShortFormEnd());
		addIntegerLiteral(r, VitalOntology.hasShortFormStart, a.getShortFormStart());
		
	}

}
