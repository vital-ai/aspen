/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.ontology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;

import ai.vital.flow.server.ontology.VitalOntology;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;

public class ModelUtils {

	public static List<Resource> addListPropertyAsEdges(Model m, Resource r, Resource edgeType, String edgeURIBase, List<Resource> value) {
		return addListPropertyAsEdges(m, r, edgeType, edgeURIBase, value.toArray(new Resource[value.size()]));
	}
	
	public static List<Resource> addListPropertyAsEdges(Model m, Resource r, Resource edgeType, String edgeURIBase, Resource... value) {
		
		if(value == null) return Collections.emptyList();
		
		List<Resource> edges = new ArrayList<Resource>();
		
		int index = 0;
		
		for(Resource u : value) {

			String edgeURI = edgeURIBase + RandomStringUtils.randomAlphabetic(8) + '-' + System.currentTimeMillis(); 
			
			Resource edge = m.createResource(edgeURI, edgeType);
			
			edge.addProperty(VitalOntology.hasEdgeSource, r);
			edge.addProperty(VitalOntology.hasEdgeDestination, u);
			edge.addLiteral(VitalOntology.hasListIndex, index++);
			edges.add(edge);
		}
		
		return edges; 
		
	}
	
	public static String getStringLiteral(Resource r, Property p) {
		Statement stmt = r.getProperty(p);
		if(stmt == null) return null;
		return stmt.getString();
	}

	public static void addStringLiteral(Resource docR,
			Property p, String value, boolean overwrite) {

		if(overwrite) {
			docR.removeAll(p);
		}
		
		if(value != null) {
			docR.addLiteral(p, value);
		}
		
	}
}
