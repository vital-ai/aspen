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

import ai.vital.aspen.groovy.nlp.domain.Annotation;
import ai.vital.aspen.groovy.nlp.domain.rdf.AbstractRDFConverter;
import ai.vital.flow.server.ontology.VitalOntology;

public class AnnotationRDFConverter extends AbstractRDFConverter<Annotation> {

	@Override
	public Resource getRDFType() {
		return VitalOntology.Annotation;
	}

	@Override
	public Annotation fromRDF(Model m, Resource s) {

		Annotation a = new Annotation();
		a.setUri(s.getURI());
		a.setAnnotationName(getStringLiteral(s, VitalOntology.hasAnnotationName));
		a.setAnnotationValue(getStringLiteral(s, VitalOntology.hasAnnotationValue));
		return a;
	}

	@Override
	protected void fill(Annotation a, Model m, Resource r) {

		addStringLiteral(r, VitalOntology.hasAnnotationName, a.getAnnotationName());
		addStringLiteral(r, VitalOntology.hasAnnotationValue, a.getAnnotationValue());

	}

}
