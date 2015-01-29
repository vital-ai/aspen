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
import ai.vital.aspen.groovy.nlp.domain.Annotation;
import ai.vital.aspen.groovy.nlp.domain.Category;
import ai.vital.aspen.groovy.nlp.domain.Document;
import ai.vital.aspen.groovy.nlp.domain.Entity;
import ai.vital.aspen.groovy.nlp.domain.TextBlock;
import ai.vital.aspen.groovy.nlp.domain.Topic;
import ai.vital.aspen.groovy.nlp.domain.rdf.AbstractRDFConverter;
import ai.vital.flow.server.ontology.VitalOntology;

public class DocumentRDFConverter extends AbstractRDFConverter<Document>{

	@Override
	public Resource getRDFType() {
		return VitalOntology.Document;
	}
	
	@Override
	public void delete(String uri, Model model) {

		deleteListPropertyAsEdges(model, model.getResource(uri), VitalOntology.Edge_hasAbbreviation, AbbreviationRDFConverter.converters.get(Abbreviation.class));
		
		deleteListPropertyAsEdges(model, model.getResource(uri), VitalOntology.Edge_hasEntity, AbstractRDFConverter.converters.get(Entity.class));
		
		deleteListPropertyAsEdges(model, model.getResource(uri), VitalOntology.Edge_hasTopic, AbstractRDFConverter.converters.get(Topic.class));
		
		deleteListPropertyAsEdges(model, model.getResource(uri), VitalOntology.Edge_hasTextBlock, AbstractRDFConverter.converters.get(TextBlock.class));
		
		deleteListPropertyAsEdges(model, model.getResource(uri), VitalOntology.Edge_hasAnnotation, AbstractRDFConverter.converters.get(Annotation.class));
		
		deleteListPropertyAsEdges(model, model.getResource(uri), VitalOntology.Edge_hasCategory, AbstractRDFConverter.converters.get(Category.class));
		
		super.delete(uri, model);
	}



	@Override
	public Document fromRDF(Model m, Resource s) {

		Document d = new Document();
		
		fillDocumentObject(d, m, s);
		
		return d;
	}
	
	protected void fillDocumentObject(Document d, Model m, Resource s) {
		
		d.setUri(s.getURI());
		
		d.setAbbreviations(getListPropertyFromEdges(Abbreviation.class, m, s, VitalOntology.Edge_hasAbbreviation));

		d.setAnnotations(getListPropertyFromEdges(Annotation.class, m, s, VitalOntology.Edge_hasAnnotation));
		
		d.setBody(getStringLiteral(s, VitalOntology.hasBody));
		
		d.setCategories(getListPropertyFromEdges(Category.class, m, s, VitalOntology.Edge_hasCategory));
		
		d.setExtractedText(getStringLiteral(s, VitalOntology.hasExtractedText));
		
		d.setTitle(getStringLiteral(s, VitalOntology.hasTitle));
		
		d.setEntities(getListPropertyFromEdges(Entity.class, m, s, VitalOntology.Edge_hasEntity));
		
		d.setTopics(getListPropertyFromEdges(Topic.class, m, s, VitalOntology.Edge_hasTopic));
		
		d.setTextBlocks(getListPropertyFromEdges(TextBlock.class, m, s, VitalOntology.Edge_hasTextBlock));
		
		d.setUrl(getStringLiteral(s, VitalOntology.hasUrl));
		
	}

	@Override
	public void fill(Document d, Model m, Resource r) {

		addListPropertyAsEdges(m, r, VitalOntology.Edge_hasAbbreviation, VitalOntology.Edge_hasAbbreviationURIBase, d.getAbbreviations());
		
		addListPropertyAsEdges(m, r, VitalOntology.Edge_hasAnnotation, VitalOntology.Edge_hasAnnotationURIBase, d.getAnnotations());
		
		addStringLiteral(r, VitalOntology.hasBody, d.getBody());
	
		addListPropertyAsEdges(m, r, VitalOntology.Edge_hasCategory, VitalOntology.Edge_hasCategoryURIBase, d.getCategories());
		
		addStringLiteral(r, VitalOntology.hasExtractedText, d.getExtractedText());
		
		addStringLiteral(r, VitalOntology.hasTitle, d.getTitle());
	
		addListPropertyAsEdges(m, r, VitalOntology.Edge_hasEntity, VitalOntology.Edge_hasEntityURIBase, d.getEntities());
		
		addListPropertyAsEdges(m, r, VitalOntology.Edge_hasTopic, VitalOntology.Edge_hasTopicURIBase, d.getTopics());
		
		addListPropertyAsEdges(m, r, VitalOntology.Edge_hasTextBlock, VitalOntology.Edge_hasTextBlockURIBase, d.getTextBlocks());
		
		addStringLiteral(r, VitalOntology.hasUrl, d.getUrl());
		
	}



}
