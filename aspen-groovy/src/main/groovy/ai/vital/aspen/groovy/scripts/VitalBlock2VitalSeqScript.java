package ai.vital.aspen.groovy.scripts;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;

import ai.vital.hadoop.writable.VitalBytesWritable;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.BlockIterator;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock;
import ai.vital.vitalsigns.model.GraphObject;

public class VitalBlock2VitalSeqScript {

	static void o(Object o) { System.out.println(o); }
	
	static RuntimeException re(String m) { return new RuntimeException(m); }
	
	public static void main(String[] args) throws Exception {

		if(args.length != 2) {
			System.err.println("Usage: <input_block_file> <output_seq_dir>");
			return;
		}
	
		Configuration conf = new Configuration();
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		o("Input path:  " + inputPath);
		o("Output path: " + outputPath);
		
		if(!outputPath.getName().endsWith(".vital.seq")) {
			throw re("output path must end with .vital.seq");
		}	
		
		FileSystem inputFS = FileSystem.get(inputPath.toUri(), conf);
		FileSystem outputFS = FileSystem.get(outputPath.toUri(), conf);
		
		
		if(!inputFS.exists(inputPath)) throw re("Input path does not exist: " + inputPath);
		if(outputFS.exists(outputPath)) throw re("Output path already exists: " + outputPath);
		
		List<Path> inputPaths = new ArrayList<Path>();
		
		if( inputFS.isDirectory(inputPath) ) {
			
			for( FileStatus fs : inputFS.listStatus(inputPath) ) {
				
				if(fs.isFile()) {
					
					inputPaths.add(fs.getPath());
					
				}
				
			}
			
		} else {
			
			inputPaths.add(inputPath);
			
		}
		
		
		o("Input files list size: " + inputPaths.size());
		
		Text key = new Text();
		
		VitalBytesWritable value = new VitalBytesWritable();
		
		Writer writer = SequenceFile.createWriter(outputFS, conf, outputPath, key.getClass(), value.getClass(), CompressionType.RECORD, new DefaultCodec());
		
		Set<String> duplicatedURIs = new HashSet<String>();
		
		int duplicatedURIsCount = 0;
		
		for(int i = 0; i < inputPaths.size(); i++) {
			
			Path p = inputPaths.get(i);
			
			o("Processing path " + (i+1) + " of " + inputPaths.size() + " - " + p);

			InputStream inputStream = inputFS.open(p);
			
			if(p.getName().endsWith(".gz")) {
				inputStream = new GZIPInputStream(inputStream);
			}
			
			BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8.name()));
			
//		outputPathFile.mkdirs();
//		outputPath = outputPath + "/part-m-00000";
			
			BlockIterator blocksIterator = null;
			
			for(blocksIterator = BlockCompactStringSerializer.getBlocksIterator(br); blocksIterator.hasNext(); ) {
				
				VitalBlock block = blocksIterator.next();
				
				GraphObject mainObject = block.getMainObject();
				
				String u = mainObject.getURI();
				
//				u = u.substring(u.lastIndexOf('/')+1);
				
				if(!duplicatedURIs.add(u)) {
					duplicatedURIsCount++;
					continue;
				}
				
				key.set(mainObject.getURI());
				
				value.set(VitalSigns.get().encodeBlock(block.toList()));
				
				writer.append(key, value);
				
			}
			
			blocksIterator.close();
			
			
			
		}
		
		IOUtils.closeQuietly(writer);

		o("Done, duplicatedURIsCount: " + duplicatedURIsCount);
		
	}

}
