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

import ai.vital.aspen.groovy.nlp.domain.NormalizedTopic;
import ai.vital.aspen.groovy.nlp.domain.rdf.AbstractRDFConverter;
import ai.vital.flow.server.ontology.VitalOntology;

public class NormalizedTopicRDFConverter extends
		AbstractRDFConverter<NormalizedTopic> {

	@Override
	public Resource getRDFType() {
		return VitalOntology.NormalizedTopic;
	}

	@Override
	public NormalizedTopic fromRDF(Model m, Resource r) {
		NormalizedTopic nt = new NormalizedTopic();
		nt.setUri(r.getURI());
		return nt;
	}

	@Override
	protected void fill(NormalizedTopic object, Model model, Resource resource) {

	}

}
