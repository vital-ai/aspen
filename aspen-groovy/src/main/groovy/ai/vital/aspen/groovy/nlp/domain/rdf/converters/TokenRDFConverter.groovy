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

import ai.vital.aspen.groovy.nlp.domain.Token;
import ai.vital.aspen.groovy.nlp.domain.rdf.AbstractRDFConverter;
import ai.vital.flow.server.ontology.VitalOntology;

public class TokenRDFConverter extends AbstractRDFConverter<Token> {

	@Override
	public Resource getRDFType() {
		return VitalOntology.Token;
	}

	@Override
	public Token fromRDF(Model m, Resource r) {
		Token t = new Token();
		t.setUri(r.getURI());
		t.setEnd(getIntegerLiteral(r, VitalOntology.hasEndPosition, null));
		t.setStart(getIntegerLiteral(r, VitalOntology.hasStartPosition, null));
		t.setText(getStringLiteral(r, VitalOntology.hasTokenText));
		return t;
	}

	@Override
	protected void fill(Token t, Model m, Resource r) {

		addIntegerLiteral(r, VitalOntology.hasEndPosition, t.getEnd());
		addIntegerLiteral(r, VitalOntology.hasStartPosition, t.getStart());
		addStringLiteral(r, VitalOntology.hasTokenText, t.getText());
		
	}

}
