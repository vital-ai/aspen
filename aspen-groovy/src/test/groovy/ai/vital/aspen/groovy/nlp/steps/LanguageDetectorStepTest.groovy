package ai.vital.aspen.groovy.nlp.steps

import com.cybozu.labs.langdetect.DetectorFactory;

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.nlp.models.LanguageDetector;
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
		
	}

}
