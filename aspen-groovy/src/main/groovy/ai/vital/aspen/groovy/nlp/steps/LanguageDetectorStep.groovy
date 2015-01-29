package ai.vital.aspen.groovy.nlp.steps


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cybozu.labs.langdetect.Detector
import com.cybozu.labs.langdetect.DetectorFactory;

import ai.vital.domain.Annotation
import ai.vital.domain.Document
import ai.vital.domain.Edge_hasAnnotation;
import ai.vital.aspen.groovy.nlp.model.DocumentUtils;
import ai.vital.aspen.groovy.nlp.model.EdgeUtils;
import ai.vital.flow.server.ontology.VitalOntology;
import ai.vital.vitalsigns.model.container.Payload;
import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepV2Impl;


class LanguageDetectorStep {

	private final static Logger log = LoggerFactory.getLogger(LanguageDetectorStep.class);
	
	public final static StepName LANGUAGEDETECTOR2_VS = new StepName("languagedetector2_vs");
	
	public static boolean loaded = false;
	
	
	public void init()  {
		
		if(loaded) return;
				
		log.info("Initializing Language Detector 2 ...");
		
		File profileDir = new File("resources/langdetect-profiles/");
		
		
		if(!profileDir.exists()) throw new StepInitializationException("Language profiles directory not found: ${new File('resources/langdetect-profiles/').absolutePath}");
		
		log.info("Loading {} profiles from {}", profileDir.list().length, profileDir.getAbsolutePath());

		DetectorFactory.loadProfile(profileDir);
		
		log.info("Langdetect profiles loaded.");

		loaded = true;
				
	}

	public void processPayload(Document doc, List list) {
			
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
				
			list.addAll(Arrays.asList(a));
			list.addAll(EdgeUtils.createEdges(doc, Arrays.asList(a), Edge_hasAnnotation, VitalOntology.Edge_hasAnnotationURIBase));
				
		
		
	}

	
	public String getName() {
		return LANGUAGEDETECTOR2_VS.getName();
	}

	
	
	
}
