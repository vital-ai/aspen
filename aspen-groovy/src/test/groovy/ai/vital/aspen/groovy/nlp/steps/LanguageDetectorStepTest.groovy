package ai.vital.aspen.groovy.nlp.steps

import com.cybozu.labs.langdetect.DetectorFactory;

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.nlp.models.LanguageDetector;
import com.vitalai.domain.nlp.Annotation;
import com.vitalai.domain.nlp.Document
import ai.vital.vitalsigns.model.GraphObject
import junit.framework.TestCase;

class LanguageDetectorStepTest extends TestCase {
	
	public void testClasspath() {
		
		AspenGroovyConfig.get().loadResourcesFromClasspath = true
		
		LanguageDetectorStep ldstep = new LanguageDetectorStep()
		
		LanguageDetectorStep.loaded = false
		
		DetectorFactory.clear()
		
		ldstep.init()
		
		
	}
	
	public void testDev() {
		
		AspenGroovyConfig.get().loadResourcesFromClasspath = false
				
		LanguageDetectorStep ldstep = new LanguageDetectorStep()
		
		LanguageDetectorStep.loaded = false
		
		DetectorFactory.clear()
		
		ldstep.init()
		
		
		Document doc = new Document()
		doc.body = "Jestem tak bardzo polski"
		 
		List<GraphObject> objects = ldstep.processDocument(doc)
		assertEquals(2, objects.size())
		for(GraphObject g : objects) {
			if(g instanceof Annotation) {
				Annotation a = g;
				assertEquals('language-id', a.annotationName.toString());
				assertEquals('pl', a.annotationValue.toString());
				
			}
		}
		
		
		
	}

}
