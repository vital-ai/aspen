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

import ai.vital.aspen.groovy.nlp.domain.NormalizedEntity;
import ai.vital.aspen.groovy.nlp.domain.rdf.AbstractRDFConverter;
import ai.vital.flow.server.ontology.VitalOntology;

public class NormalizedEntityRDFConverter extends AbstractRDFConverter<NormalizedEntity> {

	@Override
	public Resource getRDFType() {
		return VitalOntology.NormalizedEntity;
	}

	@Override
	public NormalizedEntity fromRDF(Model model, Resource s) {
		NormalizedEntity ne = new NormalizedEntity();
		ne.setUri(s.getURI());
		ne.setName(getStringLiteral(s, VitalOntology.hasName));
		ne.setScore(getFloatLiteral(s, VitalOntology.hasScore, null));
		ne.setShortname(getStringLiteral(s, VitalOntology.hasShortname));
		ne.setSymbol(getStringLiteral(s, VitalOntology.hasSymbol));
		ne.setTicker(getStringLiteral(s, VitalOntology.hasTicker));
		return ne;
	}

	@Override
	protected void fill(NormalizedEntity ne, Model m, Resource r) {

		addStringLiteral(r, VitalOntology.hasName, ne.getName());
		addFloatLiteral(r, VitalOntology.hasScore, ne.getScore());
		addStringLiteral(r, VitalOntology.hasShortname, ne.getShortname());
		addStringLiteral(r, VitalOntology.hasSymbol, ne.getSymbol());
		addStringLiteral(r, VitalOntology.hasTicker, ne.getTicker());
	}


}
