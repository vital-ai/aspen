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
import ai.vital.aspen.groovy.nlp.domain.Topic;
import ai.vital.aspen.groovy.nlp.domain.rdf.AbstractRDFConverter;
import ai.vital.flow.server.ontology.VitalOntology;

public class TopicRDFConverter extends AbstractRDFConverter<Topic> {

	@Override
	public void delete(String uri, Model model) {

		deleteListPropertyAsEdges(model, model.getResource(uri), VitalOntology.Edge_hasNormalizedTopic, AbstractRDFConverter.converters.get(NormalizedTopic.class));
		
		super.delete(uri, model);
	}
	
	@Override
	public Resource getRDFType() {
		return VitalOntology.Topic;
	}

	@Override
	public Topic fromRDF(Model m, Resource r) {
		Topic t = new Topic();
		t.setUri(r.getURI());
		t.setClassifierName(getStringLiteral(r, VitalOntology.hasClassifierName));
		t.setName(getStringLiteral(r, VitalOntology.hasName));
		t.setNormalizedTopics(getListPropertyFromEdges(NormalizedTopic.class, m, r, VitalOntology.Edge_hasNormalizedTopic));
		t.setScore(getFloatLiteral(r, VitalOntology.hasScore, null));
		return t;
	}

	@Override
	protected void fill(Topic t, Model m, Resource r) {

		addStringLiteral(r, VitalOntology.hasClassifierName, t.getClassifierName());
		addStringLiteral(r, VitalOntology.hasName, t.getName());
		addListPropertyAsEdges(m, r, VitalOntology.Edge_hasNormalizedTopic, VitalOntology.Edge_hasNormalizedTopicURIBase, t.getNormalizedTopics());
		addFloatLiteral(r, VitalOntology.hasScore, t.getScore());

	}

}
