package ai.vital.aspen.data

import java.util.ArrayList

import org.apache.commons.cli.BasicParser
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.ParseException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path

import ai.vital.aspen.config.AspenConfig

/**
 * A wrapper around DataProfileJob that take cares 
 */
object DataProfileJobWrapper  {
  
  def main(args: Array[String]): Unit = {

      val parser = new BasicParser();
      
      val options = DataProfileJob.getOptions()
      
      if (args.length == 0) {
        val hf = new HelpFormatter()
        hf.printHelp("aspendataprofile", options)
        return
      }


      var cmd: CommandLine = null
  
      try {
        cmd = parser.parse(options, args);
      } catch {
        case ex: ParseException => {
          System.err.println(ex.getLocalizedMessage());
          return
        }
      }
      
      //check the mode now
      var jobServerURL = cmd.getOptionValue(DataProfileJob.jobServerOption.getLongOpt)
      
      if(jobServerURL == null) {
        
    	  println("no jobserver location, checking aspen config in $VITAL_HOME ...")
    	  jobServerURL = AspenConfig.get.getJobServerURL
        
      }
        
      
      if(jobServerURL != null) {
        
        val conf = new Configuration()
        
        //job will be run remotely, check the paths now
        var inputPath = new Path(cmd.getOptionValue(DataProfileJob.inputOption.getLongOpt))
        var outputPath = new Path(cmd.getOptionValue(DataProfileJob.outputOption.getLongOpt))
        
        var overwrite = cmd.hasOption(DataProfileJob.overwriteOption.getLongOpt)
        
        var tempInputPath : Path = null
        
        var tempOutputPath : Path = null
        
        
        val u1 = inputPath.toUri();
        val u2 = outputPath.toUri();
        
        val copyInput = u1.getScheme == null || "file".equals(u1.getScheme)
        
        val copyOutput = u2.getScheme == null || "file".equals(u2.getScheme)
        
        var tempPath : Path = null
        
        if(copyInput || copyOutput) {
          
        	val tempLocation = AspenConfig.get.getTempRemoteLocation;
        	
        	if(tempLocation == null || tempLocation.isEmpty()) {
        		System.err.println("tempRemoteLocation not set in aspen config");
        		return 
        	} 
          
        	val ts = System.currentTimeMillis()
          
          tempPath = new Path(tempLocation, "aspendataprofile_" + ts)
          
        }
        
        if(copyInput) {
          
          tempInputPath = new Path(tempPath, inputPath.getName)
          
        } 
        
        if(copyOutput) {
          
          tempOutputPath = new Path(tempPath, "results")
          
          val fs = FileSystem.get(outputPath.toUri(), conf)
          
          if(!overwrite) {
            
            if(fs.exists(outputPath)) {
              System.err.println("outpath path already exists: " + outputPath + ", use --" + DataProfileJob.overwriteOption.getLongOpt + " option")
              return
            }
            
          } else {
            
            //delete it now?
            fs.delete(outputPath, true)            
            
          }
          
          
        }
        
        
        if(copyInput || copyOutput) {
          
          println("local files paths detected, using temp remote location")
          
        	val updatedArgs = new ArrayList[String]()
        	
          var previous : String = null;
          
        	for(a <- args) {
        		
            var x = a
            
            if(copyInput && previous != null && (previous.equals("--" + DataProfileJob.inputOption.getLongOpt) || previous.equals("-" + DataProfileJob.inputOption.getOpt))) {
              
              x = tempInputPath.toString()
              
            }
            
            if(copyOutput && previous != null && (previous.equals("--" + DataProfileJob.outputOption.getLongOpt) || previous.equals("-" + DataProfileJob.outputOption.getOpt))) {
              
              x = tempOutputPath.toString()
              
            }
            
//            if(!"-js".equals(x) && !("-js".equals(previous))) {
              
              updatedArgs.add(x)
              
//            }
            
            previous = x
            
        	}
          
          if(copyInput) {
            
            var srcFS = FileSystem.get(inputPath.toUri(), conf)
            var destFS = FileSystem.get(tempInputPath.toUri(), conf)
            FileUtil.copy(srcFS, inputPath, destFS, tempInputPath, false, true, conf)
            
          }

          //execute it and wait for results
          val response = DataProfileJob._mainImpl(updatedArgs.toArray(Array[String]()), true)
          
          if(response && copyOutput) {
            
            var srcFS = FileSystem.get(tempOutputPath.toUri(), conf)
            var destFS = FileSystem.get(outputPath.toUri(), conf)
            FileUtil.copy(srcFS, tempOutputPath, destFS, outputPath, true, true, conf)
            
          }
          
          val fs = FileSystem.get(tempPath.toUri(), conf)
          fs.delete(tempPath, true)
          
        	return
          
        }
        
      }
      
      //execute it normally
      println("Normal mode, no moving files around")
      DataProfileJob.main(args)
     
  } 
  
}