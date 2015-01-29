/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 *
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.workflowstep_VS

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cybozu.labs.langdetect.Detector
import com.cybozu.labs.langdetect.DetectorFactory;

import ai.vital.domain.Annotation
import ai.vital.domain.Document
import ai.vital.domain.Edge_hasAnnotation;
import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.aspen.groovy.nlp.model.DocumentUtils;
import ai.vital.aspen.groovy.nlp.model.EdgeUtils;
import ai.vital.flow.server.ontology.VitalOntology;
import ai.vital.vitalsigns.model.container.Payload;
import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepV2Impl;

class LanguageDetector2WorkflowStep_VS extends WorkflowStepV2Impl<NLPServerConfig> {

	private final static Logger log = LoggerFactory.getLogger(LanguageDetector2WorkflowStep_VS.class);
	
	public final static StepName LANGUAGEDETECTOR2_VS = new StepName("languagedetector2_vs");
	
	public static boolean loaded = false;
	
	@Override
	public void init(NLPServerConfig config) throws StepInitializationException {
		super.init(config);

		if(loaded) return;
				
		log.info("Initializing Language Detector 2 ...");
		
		File profileDir = new File("./langdetect-profiles/");
		
		if(!profileDir.exists()) {
			profileDir = new File("./nlp-flow-server/langdetect-profiles/")
		}
		
		if(!profileDir.exists()) throw new StepInitializationException("Language profiles directory not found: ${new File('./langdetect-profiles/').absolutePath}");
		
		log.info("Loading {} profiles from {}", profileDir.list().length, profileDir.getAbsolutePath());

		DetectorFactory.loadProfile(profileDir);
		
		log.info("Langdetect profiles loaded.");

		loaded = true;
				
	}

	@Override
	public void processPayload(Payload payload)
			throws ai.vital.workflow.IWorkflowStep.WorkflowHaltException,
			ai.vital.workflow.IWorkflowStep.ProcessflowHaltException,
			Exception {

		for( Document doc : payload.iterator(Document.class) ) {
					
			String docUri = doc.getURI();
							
			log.info("Processing document {} ...", docUri);
				
			String text = DocumentUtils.getTextBlocksContent(doc);
			
			Detector detector = DetectorFactory.create();	
			
			detector.append(text);
			
			String langID = "unknown";
			
			try {
				langID = detector.detect();
			} catch(Exception e) {
				log.error(e.getLocalizedMessage(), e);
			}
				
			log.info("Detected language: {}", langID);
				
			Annotation a = new Annotation();
			a.annotationName = 'language-id';
			a.annotationValue = langID;
			a.URI = docUri + "#languageAnnotation";
				
			payload.putGraphObjects(Arrays.asList(a));
			payload.putGraphObjects(EdgeUtils.createEdges(doc, Arrays.asList(a), Edge_hasAnnotation, VitalOntology.Edge_hasAnnotationURIBase));
				
		}
		
	}

	@Override
	public String getName() {
		return LANGUAGEDETECTOR2_VS.getName();
	}

}
