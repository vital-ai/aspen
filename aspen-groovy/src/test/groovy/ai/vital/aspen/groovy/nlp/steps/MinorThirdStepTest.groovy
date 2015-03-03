package ai.vital.aspen.groovy.nlp.steps

import ai.vital.aspen.groovy.AspenGroovyConfig;
import junit.framework.TestCase

class MinorThirdStepTest extends TestCase {

	public void testClasspath() {
		
		AspenGroovyConfig.get().loadResourcesFromClasspath = true
		
		MinorThirdStep m3rdStep = new MinorThirdStep();
		
		m3rdStep.init()
		
		
	}
	
	public void testDev() {
		
		AspenGroovyConfig.get().loadResourcesFromClasspath = false
		
		MinorThirdStep m3rdStep = new MinorThirdStep();
		
		m3rdStep.init()
		
	}
}
