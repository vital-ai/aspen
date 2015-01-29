/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.domain.rdf;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import ai.vital.aspen.groovy.nlp.domain.Document;
import ai.vital.aspen.groovy.nlp.domain.rdf.converters.DocumentRDFConverter;
import ai.vital.flow.server.ontology.VitalOntology;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.vocabulary.RDF;

public class DocumentExtractor {

	public static List<Document> readDocuments(Model model) {
		
		DocumentRDFConverter c = new DocumentRDFConverter();
		
		List<Document> res = new ArrayList<Document>();
		
		for( ResIterator iterator = model.listSubjectsWithProperty(RDF.type, VitalOntology.Document); iterator.hasNext(); ) {
		
			Document doc = c.fromRDF(model, iterator.nextResource());
			
			res.add(doc);
			
		}
		
		return res;
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Model m = ModelFactory.createDefaultModel();
		m.read(new FileInputStream(new File("../nlpserver/sample-output.nt")), null, "N-TRIPLE");
	
		List<Document> readDocuments = readDocuments(m);
		
		System.out.println(readDocuments);
		
	}

	public static void updateDoc(Model model, Document doc) {

		//delete the document and reappend it to the model
		DocumentRDFConverter c = new DocumentRDFConverter();
		c.delete(doc.getUri(), model);
		
		c.toRDF(doc, model);
		
		
	}
	
}
