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

import ai.vital.aspen.groovy.nlp.domain.VerbPhrase;
import ai.vital.aspen.groovy.nlp.domain.rdf.AbstractRDFConverter;
import ai.vital.flow.server.ontology.VitalOntology;

public class VerbPhraseRDFConverter extends AbstractRDFConverter<VerbPhrase> {

	@Override
	public Resource getRDFType() {
		return VitalOntology.VerbPhrase;
	}

	@Override
	public VerbPhrase fromRDF(Model m, Resource s) {

		VerbPhrase vp = new VerbPhrase();
		vp.setUri(s.getURI());
		vp.setEndTokenIndex(getIntegerLiteral(s, VitalOntology.hasEndTokenIndex, null));
		vp.setStartTokenIndex(getIntegerLiteral(s, VitalOntology.hasStartTokenIndex, null));
		return vp;
	}

	@Override
	protected void fill(VerbPhrase vp, Model m, Resource r) {

		addIntegerLiteral(r, VitalOntology.hasEndTokenIndex, vp.getEndTokenIndex());
		addIntegerLiteral(r, VitalOntology.hasStartTokenIndex, vp.getStartTokenIndex());

	}

}
