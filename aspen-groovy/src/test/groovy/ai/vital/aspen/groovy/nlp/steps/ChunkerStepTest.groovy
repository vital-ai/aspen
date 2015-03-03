package ai.vital.aspen.groovy.nlp.steps

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.nlp.models.ChunkerModelWrapper;
import junit.framework.TestCase

class ChunkerStepTest extends TestCase {

	public void testClasspath() {
		
		AspenGroovyConfig.get().loadResourcesFromClasspath = true
		
		ChunkerStep chunker = new ChunkerStep()
		
		ChunkerModelWrapper.purge();
		
		chunker.init()
		
	}
	
	public void testDev() {
		
		
		AspenGroovyConfig.get().loadResourcesFromClasspath = false
		
		ChunkerStep chunker = new ChunkerStep()
		
		ChunkerModelWrapper.purge();
		
		chunker.init()
		
	}
	
}
