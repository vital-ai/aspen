package ai.vital.aspen.examples

import ai.vital.aspen.job.AbstractJob
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.typesafe.config.Config
import com.typesafe.config.ConfigException
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import ai.vital.hadoop.writable.VitalBytesWritable
import org.apache.hadoop.io.Text
import ai.vital.vitalsigns.VitalSigns


object TwentyNewsAnnotateDocuments extends AbstractJob {

  def getJobClassName(): String = {
     return TwentyNewsAnnotateDocuments.getClass.getCanonicalName
  }

  def getJobName(): String = {
     return "vital naive bayes classification"
  }
  
  val inputOption  = new Option("i", "input", true, "input RDD[(String, Array[Byte])], either RDD name or path:<path>, where path is a .vital.seq file")
  inputOption.setRequired(true)
  
  val outputOption = new Option("o", "output", true, "output RDD[(String,Array[Byte])], either RDD name or path:<path>, where path is a .vital.seq file")
  outputOption.setRequired(true)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite output file if exists")
  overwriteOption.setRequired(false)
 
  val configOption = new Option("c", "config", true, "optional aspen config file path")
  configOption.setRequired(false)
  
  def getOptions(): Options = {
    val options = new Options()
        .addOption(masterOption)
        .addOption(inputOption)
        .addOption(outputOption)
        .addOption(overwriteOption)
        .addOption(configOption)

    return options
  }
  
   def main(args: Array[String]): Unit = {
    
     _mainImpl(args)
     
   }
   
   def runJob(sc: SparkContext, jobConfig: Config): Any = {
   
      val inputPath = jobConfig.getString(inputOption.getLongOpt)
      
      val outputPath = jobConfig.getString(outputOption.getLongOpt)
    
      val overwrite = jobConfig.getBoolean(overwriteOption.getLongOpt)
      
      var configPath : String = null
      
      try {
        configPath = jobConfig.getString(configOption.getLongOpt)
      } catch {
        case ex : ConfigException.Missing => {}
      }
    
      println("input: " + inputPath)
      println("output: " + outputPath)
      println("overwrite: " + overwrite)
      println("aspen config: " + configPath)
      
      var outpuBlockFS : FileSystem = null 
      
      if(!outputPath.startsWith("path:")) {
        throw new RuntimeException("NamedRDD disabled, use path: prefixed output")
      } else {
      
        val outputBlockPath = new Path(outputPath.substring(5))
        
        outpuBlockFS = FileSystem.get(outputBlockPath.toUri(), new Configuration())
        
        //check if output exists
        if(outpuBlockFS.exists(outputBlockPath)) {
          if(!overwrite) {
            throw new RuntimeException("Output file path already exists, use --overwrite option - " + outputBlockPath)
          } else {
            outpuBlockFS.delete(outputBlockPath, true)
          }
        }
      
      }
      
      var inputRDD : RDD[(String, Array[Byte])] = null
      
      if(inputPath.startsWith("path:")) {
      
        val inputPathObj = new Path(inputPath.substring(5))
      
        val inputFS = FileSystem.get(inputPathObj.toUri(), new Configuration())
      
        if (!inputFS.exists(inputPathObj) || !inputFS.isDirectory(inputPathObj)) {
          System.err.println("Input train path does not exist or is not a directory: " + inputPathObj.toString())
          return
        }

        inputRDD = sc.sequenceFile(inputPathObj.toString(), classOf[Text], classOf[VitalBytesWritable]).map{ pair =>

          (pair._1.toString(), pair._2.get)
        
        } 
      } else {
        throw new RuntimeException("NamedRDD disabled, use path: prefixed input")
      }
      
      
      val outputRDD = inputRDD.map { p =>
        
        var objects = VitalSigns.get().decodeBlock(p._2, 0, p._2.length)
        
        var cp : Path = null
        if(configPath != null) cp = new Path(configPath)
        
        //there should be a singleton object in each worker
        val nlp = TwentyNewsNLP.get(cp)
        
        //it will append the results
        objects = nlp.process(objects)
        
        (p._1, VitalSigns.get.encodeBlock(objects))
        
      }
      
      
      if(outputPath.startsWith("path:")) {
      
        val outPath = outputPath.substring(5)
        
        val hadoopOutput = outputRDD.map( pair =>
          (new Text(pair._1), new VitalBytesWritable(pair._2))
        )
      
        hadoopOutput.saveAsSequenceFile(outPath)
        
      } else {
//        this.namedRdds.update(outputRDDName, gidOutQuadsRDD)
        throw new RuntimeException("Named RDD not supported!")
      }
      
      
       
   }
   
}