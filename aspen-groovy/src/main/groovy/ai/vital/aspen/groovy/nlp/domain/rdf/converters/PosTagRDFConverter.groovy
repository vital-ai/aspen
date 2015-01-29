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

import ai.vital.aspen.groovy.nlp.domain.PosTag;
import ai.vital.aspen.groovy.nlp.domain.rdf.AbstractRDFConverter;
import ai.vital.flow.server.ontology.VitalOntology;

public class PosTagRDFConverter extends AbstractRDFConverter<PosTag> {

	@Override
	public Resource getRDFType() {
		return VitalOntology.PosTag;
	}

	@Override
	public PosTag fromRDF(Model m, Resource r) {
		PosTag t = new PosTag();
		t.setUri(r.getURI());
		t.setConfidence(getDoubleLiteral(r, VitalOntology.hasConfidence, null));
		t.setTag(getStringLiteral(r, VitalOntology.hasTagValue));
		return t;
	}

	@Override
	protected void fill(PosTag p, Model m, Resource r) {
		addDoubleLiteral(r, VitalOntology.hasConfidence, p.getConfidence());
		addStringLiteral(r, VitalOntology.hasTagValue, p.getTag());
	}

}
