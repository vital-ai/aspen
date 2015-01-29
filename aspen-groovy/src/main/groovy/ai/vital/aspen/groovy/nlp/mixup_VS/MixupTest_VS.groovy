/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.mixup_VS;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

//TODO
//import ai.vital.demoapp.Artist;



import ai.vital.domain.Person;
import ai.vital.domain.Content
import ai.vital.domain.Document;
import ai.vital.domain.Edge_hasContent;
import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.aspen.groovy.nlp.engine.NLPServerEngine;
import ai.vital.flow.server.ontology.VitalOntology;
import ai.vital.flow.server.config.ProcessServerConfig;
import ai.vital.flow.server.config.ProcessServerConfigManager;
import ai.vital.vitalsigns.VitalSigns
import ai.vital.vitalsigns.global.GlobalHashTableEdgesResolver
import ai.vital.vitalsigns.model.container.Payload;
import ai.vital.vitalsigns.model.container.ProcessFlowStep;

import ai.vital.flow.server.utils.EdgeUtils;

public class MixupTest_VS {

	public static void main(String[] args) throws Exception {
		
		Logger.getRootLogger().setLevel(Level.INFO);
		Logger.getRootLogger().addAppender(new ConsoleAppender());
		
		ProcessServerConfig config = ProcessServerConfigManager.loadConfigFromFile("conf/config.xml", NLPServerEngine.class);
		
		VitalSigns signs = VitalSigns.get();
		
		NLPServerEngine engine = new NLPServerEngine();
		engine.initEngine((NLPServerConfig) config.getEngine());
		
		Document doc = new Document();
		
//		String inputHTML = "My name is Mr Dariusz Kobylarz and I am proud of it.";
		String inputHTML = "I met President Barack Obama at the White House. " +
			"Then I visited Prada and Chanel official stores. " +
			"The Beatles is a fine band The Beatmasters Metallica ."
			;
		
//		doc.body = inputHTML;
		doc.setURI("http://example.org/v1");

		
		Content content = new Content();
		content.URI = doc.URI + "#content";
		content.body = inputHTML;
		
		Payload payload = new Payload();
		payload.URI = "http://example.org/payload1";
		payload.putGraphObjects(Arrays.asList(doc, EdgeUtils.createEdge(doc, content, Edge_hasContent, VitalOntology.Edge_hasContentURIBase), content));
		
		long start = System.currentTimeMillis();
				
		ProcessFlowStep step1 = new ProcessFlowStep("", "");
		engine.processPayload("OpenCalais_VS", payload, step1);
		
		long stop = System.currentTimeMillis();
		
		println("TOTAL TIME: "  + ( stop - start) );
		
		
		doc = payload.get(doc.URI);
		
		List annotations = doc.getAnnotations(payload);
		
		payload.getAllObjects()
		println();
		
		for( Person np : payload.iterator(Person.class)) {
			println("NP: " + np.name);
		}
		
		for(Person a : payload.iterator(Person.class)) {
			println("Artist: " + a.name);
		}
		
	}
	
}
