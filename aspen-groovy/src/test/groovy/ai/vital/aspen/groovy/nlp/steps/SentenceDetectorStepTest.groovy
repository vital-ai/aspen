package ai.vital.aspen.groovy.nlp.steps

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.nlp.models.POSTaggerModel;
import ai.vital.aspen.groovy.nlp.models.SentenceDetectorModel;
import junit.framework.TestCase

class SentenceDetectorStepTest extends TestCase {

	public void testClasspath() {
		
		AspenGroovyConfig.get().loadResourcesFromClasspath = true
		
		SentenceDetectorModel.purge()

		SentenceDetectorStep sds = new SentenceDetectorStep()
		
		sds.init()
		
		
	}
	
	public void testDev() {
		
		AspenGroovyConfig.get().loadResourcesFromClasspath = false
		
		SentenceDetectorModel.purge()

		SentenceDetectorStep sds = new SentenceDetectorStep()
		
		sds.init()
		
	}
	
}
