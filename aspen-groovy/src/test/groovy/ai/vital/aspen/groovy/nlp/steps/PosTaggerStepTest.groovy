package ai.vital.aspen.groovy.nlp.steps

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.nlp.models.POSTaggerModel;
import junit.framework.TestCase

class PosTaggerStepTest extends TestCase {

	public void testClasspath() {
		
		AspenGroovyConfig.get().loadResourcesFromClasspath = true
		
		POSTaggerModel.purge()

		PosTaggerStep pts = new PosTaggerStep()
		
		pts.init()
		
		
	}
	
	public void testDev() {
		
		AspenGroovyConfig.get().loadResourcesFromClasspath = false
		
		POSTaggerModel.purge()
		
		PosTaggerStep pts = new PosTaggerStep()
		
		pts.init()
		
	}
	
}
