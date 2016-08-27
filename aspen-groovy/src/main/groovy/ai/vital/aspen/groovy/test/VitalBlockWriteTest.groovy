package ai.vital.aspen.groovy.test


import ai.vital.domain.Person
import ai.vital.vitalsigns.*
import ai.vital.vitalsigns.block.BlockCompactStringSerializer

import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.uri.URIGenerator



class VitalBlockWriteTest {

	
	static VitalApp app
	
	static {
		app = VitalApp.withId('vital-test')
	}
	
	static main(args) {
	
		Person p = new Person()
		
		p.generateURI(app)
		
		p.name = "Marc"
		
		VitalSigns vs = VitalSigns.get()
		
		OutputStream os = new FileOutputStream(new File("testblock.vital"))
		
		
		BlockCompactStringSerializer serializer = new BlockCompactStringSerializer(new BufferedWriter(new OutputStreamWriter(os, "UTF-8")));
		
		serializer.startBlock()
		serializer.writeGraphObject(p)
		serializer.endBlock()
		
		serializer.close()
		
		
		
		
	}

}
