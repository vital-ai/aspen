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

import ai.vital.aspen.groovy.nlp.domain.Category;
import ai.vital.aspen.groovy.nlp.domain.rdf.AbstractRDFConverter;
import ai.vital.flow.server.ontology.VitalOntology;

public class CategoryRDFConverter extends AbstractRDFConverter<Category> {

	@Override
	public Resource getRDFType() {
		return VitalOntology.Category;
	}

	@Override
	public Category fromRDF(Model m, Resource r) {
		Category c = new Category();
		c.setUri(r.getURI());
		c.setName(getStringLiteral(r, VitalOntology.hasName));
		c.setScore(getFloatLiteral(r, VitalOntology.hasScore, null));
		return c;
	}

	@Override
	protected void fill(Category c, Model m, Resource r) {

		addStringLiteral(r, VitalOntology.hasName, c.getName());
		addFloatLiteral(r, VitalOntology.hasScore, c.getScore());

	}

}
