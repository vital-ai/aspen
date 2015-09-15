package ai.vital.aspen.data

import ai.vital.aspen.job.AbstractJob
import org.apache.commons.cli.Options
import org.apache.commons.cli.Option
import com.typesafe.config.Config
import ai.vital.aspen.util.SetOnceHashMap
import java.util.HashSet
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.InputStream
import org.apache.commons.io.IOUtils
import scala.collection.JavaConversions._
import ai.vital.aspen.job.TasksHandler

class DataProfileJob {
}

object DataProfileJob extends AbstractJob {
  
  val inputOption  = new Option("i", "input", true, "input RDD[(String, Array[Byte])], either named RDD name (name:<name>) or <path> (no prefix), where path is a .vital.seq or .vital[.gz] file")
  inputOption.setRequired(true)
  
  val outputOption  = new Option("o", "output", true, "output directory containing csv files with properties unique values counts")
  outputOption.setRequired(true)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite output file if exists")
  overwriteOption.setRequired(false)
  
  val propertiesOption = new Option("p", "properties", true, "optional properties filter txt file (one property URI per line)")
  
	def getJobName(): String = {
	  "aspendataprofile"
	}
  
  def getJobClassName(): String = {
    classOf[DataProfileJob].getCanonicalName
  }

  def getOptions(): Options = {
    addJobServerOptions(
        new Options()
        .addOption(inputOption)
        .addOption(outputOption)
        .addOption(masterOption)
        .addOption(overwriteOption)
        .addOption(propertiesOption)
    )
	}

  def runJob(sc: DataProfileJob.C, jobConfig: Config): Any = {
  
    var inputPath = jobConfig.getString(inputOption.getLongOpt)
    var outputPath = jobConfig.getString(outputOption.getLongOpt)
    var propertiesPath = getOptionalString(jobConfig, propertiesOption.getLongOpt)
    val overwrite = getBooleanOption(jobConfig, overwriteOption)
    
    println("Input path: " + inputPath)
    println("Output path: " + outputPath)
    println("Properties path: " + propertiesPath)
    println("Overwrite: " + overwrite)
    
    val propertiesSet = new HashSet[String]()
    
    if(propertiesPath != null) {
      
      val propsPathObj = new Path(propertiesPath)
      
      val propsFS = FileSystem.get(propsPathObj.toUri(), hadoopConfiguration)
      
      if( ! propsFS.exists(propsPathObj) ) {
        throw new Exception("Properties file path does not exist: " + propsPathObj.toString())
      }
      
      if( ! propsFS.isFile(propsPathObj) ) {
        throw new Exception("Properties file path does not denote a file: " + propsPathObj.toString())
      }

      var inputStream : InputStream = null
      
      try {
    	  inputStream = propsFS.open(propsPathObj)
        for(l <- IOUtils.readLines(inputStream, "UTF-8") ) {
          var lt = l.trim()
          if(lt.length() > 0) {
            propertiesSet.add(lt)
          }
        }
      } finally {
        IOUtils.closeQuietly(inputStream)
      }
      
      if(propertiesSet.size() == 0) throw new Exception("No properties found in input file: " + propsPathObj.toString())
      
    }
    
    println("Properties filter: " + propertiesSet.toString())
    
    val globalContext = new SetOnceHashMap
    
    val procedure = new DataProfileProcedureSteps(globalContext, inputPath, outputPath, propertiesSet, overwrite);
   
        
    val handler = new TasksHandler()
    
    val tasks = procedure.generateTasks()
    
    handler.handleTasksList(this, tasks)
    
    
    var stats = globalContext.get(DataProfileTask.DATASET_STATS)
    
    if(stats == null) stats = "(no dataset stats)"
    
    println(stats)
    
    return stats
    
  }
  
   
  def main(args: Array[String]): Unit = {
    
    _mainImpl(args)
     
  } 
  
}