/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.domain.rdf;


import ai.vital.aspen.groovy.nlp.domain.URIResource;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

public interface RDFConverter<T extends URIResource> {
	
	Resource getRDFType();
	
	Resource toRDF(T resource, Model model);

	T fromRDF(Model model, Resource subject);
	
	//deletes a single object with given URI
	void delete(String uri, Model model);
	
	boolean hasRDFType(Model model, String resource);
	
}
