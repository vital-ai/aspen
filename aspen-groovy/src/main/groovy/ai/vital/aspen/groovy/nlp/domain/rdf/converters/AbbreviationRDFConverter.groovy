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

import ai.vital.aspen.groovy.nlp.domain.Abbreviation;
import ai.vital.aspen.groovy.nlp.domain.AbbreviationInstance;
import ai.vital.aspen.groovy.nlp.domain.rdf.AbstractRDFConverter;
import ai.vital.flow.server.ontology.VitalOntology;

public class AbbreviationRDFConverter extends
		AbstractRDFConverter<Abbreviation> {

	@Override
	public Resource getRDFType() {
		return VitalOntology.Abbreviation;
	}

	@Override
	public Abbreviation fromRDF(Model m, Resource r) {
		
		Abbreviation a = new Abbreviation();
		a.setUri(r.getURI());
		a.setAbbreviationInstances(getListPropertyFromEdges(AbbreviationInstance.class, m, r, VitalOntology.Edge_hasAbbreviationInstance));
		a.setLongForm(getStringLiteral(r, VitalOntology.hasLongForm));
		a.setShortForm(getStringLiteral(r, VitalOntology.hasShortForm));
		return a;
	}

	@Override
	protected void fill(Abbreviation a, Model m, Resource r) {

		addListPropertyAsEdges(m, r, VitalOntology.Edge_hasAbbreviationInstance, VitalOntology.Edge_hasAbbreviationInstanceURIBase, a.getAbbreviationInstances());
		addStringLiteral(r, VitalOntology.hasLongForm, a.getLongForm());
		addStringLiteral(r, VitalOntology.hasShortForm, a.getShortForm());
		
	}

}
