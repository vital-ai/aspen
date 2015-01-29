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

import ai.vital.aspen.groovy.nlp.domain.Entity;
import ai.vital.aspen.groovy.nlp.domain.EntityInstance;
import ai.vital.aspen.groovy.nlp.domain.NormalizedEntity;
import ai.vital.aspen.groovy.nlp.domain.rdf.AbstractRDFConverter;
import ai.vital.flow.server.ontology.VitalOntology;

public class EntityRDFConverter extends AbstractRDFConverter<Entity> {

	@Override
	public void delete(String uri, Model model) {
	
		deleteListPropertyAsEdges(model, model.getResource(uri), VitalOntology.Edge_hasEntityInstance, AbstractRDFConverter.converters.get(EntityInstance.class));
		
		deleteListPropertyAsEdges(model, model.getResource(uri), VitalOntology.Edge_hasNormalizedEntity, AbstractRDFConverter.converters.get(NormalizedEntity.class));
		
		super.delete(uri, model);
	}
	
	@Override
	public Resource getRDFType() {
		return VitalOntology.Entity;
	}

	@Override
	public Entity fromRDF(Model m, Resource s) {
		Entity e = new Entity();
		fillObject(e, m, s);
		return e;
	}

	public void fillObject(Entity e, Model m, Resource s) {
		e.setUri(s.getURI());
		e.setCategory(getStringLiteral(s, VitalOntology.hasCategory));
		e.setEntityInstances(getListPropertyFromEdges(EntityInstance.class, m, s, VitalOntology.Edge_hasEntityInstance));
		e.setExtractSource(getStringLiteral(s, VitalOntology.hasExtractSource));
		e.setName(getStringLiteral(s, VitalOntology.hasName));
		e.setNormalizedEntities(getListPropertyFromEdges(NormalizedEntity.class, m, s, VitalOntology.Edge_hasNormalizedEntity));
		e.setRelevance(getFloatLiteral(s, VitalOntology.hasRelevance, null));		
	}
	
	@Override
	protected void fill(Entity e, Model m, Resource r) {
		fillResource(e, m, r);
	}

	public void fillResource(Entity e, Model m, Resource r) {
		addStringLiteral(r, VitalOntology.hasCategory, e.getCategory());
		addListPropertyAsEdges(m, r, VitalOntology.Edge_hasEntityInstance, VitalOntology.Edge_hasEntityInstanceURIBase, e.getEntityInstances());
		addStringLiteral(r, VitalOntology.hasExtractSource, e.getExtractSource());
		addStringLiteral(r, VitalOntology.hasName, e.getName());
		addListPropertyAsEdges(m, r, VitalOntology.Edge_hasNormalizedEntity, VitalOntology.Edge_hasNormalizedEntityURIBase, e.getNormalizedEntities());
		addFloatLiteral(r, VitalOntology.hasRelevance, e.getRelevance());		
	}
	
}
