package ai.vital.aspen.groovy.modelmanager.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path

import ai.vital.aspen.groovy.AspenGroovyConfig;

/**
 * Simple utility that packages model directory into zip/jar.
 * The most important part is placing model.builder file as the first entry in order to 
 * speed up model config load time. 
 * @author Derek
 *
 */
class PackageModel {

	static void error(String m) {
		System.err.println(m)
	}
	
	static main(args) {
	
		def cli = new CliBuilder(usage: 'package-model [options]')
		cli.with {
			h longOpt: "help", "Show usage information", args: 0, required: false
			m longOpt: "model-directory", "directory containing model files", args: 1, required: true
			ow longOpt: "overwrite", "overwrite output model file if exists", args: 0, required: false
			o longOpt: "output", "output jar/zip file path", args: 1, required: true
		}
		
		def options = cli.parse(args)
		
		if(!options || options.h) return
		
		Path modelDir = new Path(options.m)
		Path outputPath = new Path(options.o)
		
		println "Model directory: ${modelDir}"
		
		println "Output file path: ${outputPath}"
		
		boolean overwrite = options.ow ? true : false
		
		println "Overwrite output directory ? ${overwrite}"
		
		if( !(outputPath.getName().endsWith(".zip") || outputPath.getName().endsWith(".jar")) ) {
			error("Output file name must end with .jar or .zip")
			return
		}
		
		Configuration conf = AspenGroovyConfig.get().getHadoopConfigration()
		
		FileSystem modelFS = FileSystem.get(modelDir.toUri(), conf);
		
		FileStatus modelStatus = modelFS.getFileStatus(modelDir)
		
		if(!modelStatus.isDirectory()) {
			error("Model directory path is not a directory: ${modelDir.toString()}")
			return
		}
		
		FileSystem zipFS = FileSystem.get(outputPath.toUri(), conf)
		
		if( zipFS.exists( outputPath ) ) {

			if(!overwrite) {
				error("Output jar/zip already exists, use --overwrite option, ${outputPath.toString()}")
				return
			}
			
//			println "Output jar/zip already exists"
						
			FileStatus outputStatus = zipFS.getFileStatus(outputPath)
			
			if(!outputStatus.isFile()) {
				error("Output path already exists but is not a file, cannot proceed: ${outputPath.toString()}");
			}
		
		
			
		}
		
		
		
	}

}
