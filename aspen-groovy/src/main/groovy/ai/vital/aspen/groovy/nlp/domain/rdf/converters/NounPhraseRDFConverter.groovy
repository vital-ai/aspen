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

import ai.vital.aspen.groovy.nlp.domain.NounPhrase;
import ai.vital.aspen.groovy.nlp.domain.rdf.AbstractRDFConverter;
import ai.vital.flow.server.ontology.VitalOntology;

public class NounPhraseRDFConverter extends AbstractRDFConverter<NounPhrase> {

	@Override
	public Resource getRDFType() {
		return VitalOntology.NounPhrase;
	}

	@Override
	public NounPhrase fromRDF(Model m, Resource s) {

		NounPhrase np = new NounPhrase();
		np.setUri(s.getURI());
		np.setEndTokenIndex(getIntegerLiteral(s, VitalOntology.hasEndTokenIndex, null));
		np.setStartTokenIndex(getIntegerLiteral(s, VitalOntology.hasStartTokenIndex, null));
		return np;
	}

	@Override
	protected void fill(NounPhrase vp, Model m, Resource r) {

		addIntegerLiteral(r, VitalOntology.hasEndTokenIndex, vp.getEndTokenIndex());
		addIntegerLiteral(r, VitalOntology.hasStartTokenIndex, vp.getStartTokenIndex());

	}

}
