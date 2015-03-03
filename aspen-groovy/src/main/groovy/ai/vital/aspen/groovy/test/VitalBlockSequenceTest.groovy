package ai.vital.aspen.groovy.test

import ai.vital.vitalservice.model.App;
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.vitalsigns.block.BlockCompactStringSerializer

import ai.vital.vitalsigns.VitalSigns


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;

import ai.vital.hadoop.writable.VitalBytesWritable;


class VitalBlockSequenceTest {

	
	static App app
	
	static {
		app = new App(ID: 'vital-test', customerID: 'vital-ai')
	}
	
	
	static main(args) {
	
		
		Configuration conf = new Configuration()
		
		
		FileSystem fs = FileSystem.get(URI.create("testblock.seq"), conf)
		
		Path path = new Path("testblock.seq")
		
		Text key = new Text()
		
		DefaultCodec codec = new DefaultCodec()
		
		Writer writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), VitalBytesWritable.class, CompressionType.RECORD, codec)
		
		
		InputStream inputStream = new FileInputStream("testblock.vital")
		
		
		BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))
		
		Set<String> duplicatedURIs = new HashSet<String>()
		
		int duplicatedURIsCount = 0
		
		
		VitalBytesWritable value = new VitalBytesWritable()
		
		
		for(Iterator<VitalBlock> blocksIterator = BlockCompactStringSerializer.getBlocksIterator(br); blocksIterator.hasNext(); ) {
			
			VitalBlock block = blocksIterator.next()
			
			GraphObject mainObject = block.getMainObject()
			
			String u = mainObject.getURI()
			
			if(!duplicatedURIs.add(u)) {
				duplicatedURIsCount++
				continue
			}
			
			List<GraphObject> objects = block.getDependentObjects()
			
			objects.add(0, mainObject)
			
			byte[] encodedBlock = VitalSigns.get().encodeBlock(objects)
			
			
			key.set(mainObject.getURI())
			
			value.set(encodedBlock)
			
			writer.append(key, value)
			
		}
		
		
		writer.close()
		
		
		
	}

}
