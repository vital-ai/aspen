package ai.vital.aspen.job


import ai.vital.vitalsigns.VitalSigns

import scala.collection.JavaConversions._

import org.apache.spark.SparkContext
import com.typesafe.config.Config
import scala.util.Try
import org.apache.commons.cli.Options
import org.apache.commons.cli.Option
import org.apache.commons.cli.BasicParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.ParseException
import org.apache.spark.SparkConf
import java.util.HashMap
import com.typesafe.config.ConfigFactory

import org.apache.hadoop.io.Text;


import ai.vital.hadoop.writable.VitalBytesWritable


object TestParseSeqJob {

   val masterOption = new Option("m", "master", true, "optional spark masterURL")
    masterOption.setRequired(false)
  
    def getJobClassName(): String = {
     
    return TestParseSeqJob.getClass.getCanonicalName
  }

  def getJobName(): String = {
    return "testparseseq job"
  }
  
  
    def _mainImpl(args: Array[String]) : Unit = {
      
      val parser = new BasicParser();
      
      val options = getOptions()
      
      //if (args.length == 0) {
      //  val hf = new HelpFormatter()
      //  hf.printHelp(getJobClassName, options)
      //  return
     // }


      var cmd: CommandLine = null
  
      try {
        cmd = parser.parse(options, args);
      } catch {
        case ex: ParseException => {
          System.err.println(ex.getLocalizedMessage());
          return
        }
      }
      
      
      val optionsMap = new HashMap[String, Object]()
      
      for(x <- options.getOptions.toArray()) {
        
        val opt = x.asInstanceOf[Option]
        
        val optName = opt.getLongOpt
        var optValue : Object = null
        if(opt.hasArg()) {
          
          optValue = cmd.getOptionValue(optName)
          
          
        } else {

          if( cmd.hasOption(optName) ) {
            optValue = java.lang.Boolean.TRUE 
          } else {
            optValue = java.lang.Boolean.FALSE 
          }
          
        }
        
        optionsMap.put(optName, optValue)
        
      }
      
      val config = ConfigFactory.parseMap(optionsMap)
      
      val masterURL = cmd.getOptionValue(masterOption.getOpt);
      
      val conf = new SparkConf().setAppName(getJobName())
      
      if (masterURL != null) {
        println("custom masterURL: " + masterURL)
        conf.setMaster(masterURL)
      }
      
      val sc = new SparkContext(conf)
      
       
      runJob(sc, config)
     
      
    }
  
    def getOptions() : Options = {
      val options = new Options()
      options.addOption(masterOption)
      return options
  }
  
  
    
    
    def main(args: Array[String]): Unit = {
    _mainImpl(args)
  }
  
  
    // put job code here
    
    
  def runJob(sc: SparkContext, jobConfig: Config): Any = {
        
    val file = sc.sequenceFile("testblock.seq", classOf[Text], classOf[VitalBytesWritable])
        
    def bytesParse(v : VitalBytesWritable) : String = {
      
      val bs = v.get()
      
      val decodeBlock = VitalSigns.get().decodeBlock(bs, 0, bs.length)
      
      println ("decoding a block")
      
      // This is a Java List of GraphObjects
      decodeBlock.foreach { println }
      
      // this should return the vitalblock
      
      return "hello"
    }
    
    // this should be a map of URI --> VitalBlock
    
    
    val map = file.map{ case (k,v) => 
      
      (k.toString(), bytesParse(v)) }
        
    
    println ("Total Blocks: " + map.count())
    
    
    }
  
  
  
  
  
}