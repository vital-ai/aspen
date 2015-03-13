package ai.vital.aspen.job

import spark.jobserver.SparkJob
import org.apache.spark.SparkContext
import spark.jobserver.SparkJobValidation
import spark.jobserver.SparkJobValid
import com.typesafe.config.Config
import scala.util.Try
import spark.jobserver.SparkJobInvalid
import org.apache.commons.cli.Options
import org.apache.commons.cli.Option
import org.apache.commons.cli.BasicParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.ParseException
import org.apache.spark.SparkConf
import java.util.HashMap
import com.typesafe.config.ConfigFactory


/* this is placeholder code */


trait AbstractJob extends SparkJob {

  val profileOption = new Option("p", "profile", true, "optional vitalservice profile option")
  profileOption.setRequired(false)
  
  val masterOption = new Option("m", "master", true, "optional spark masterURL")
    masterOption.setRequired(false)
  
    def getJobName() : String
    
    def getJobClassName() : String
    
    def _mainImpl(args: Array[String]) : Unit = {
      
      val parser = new BasicParser();
      
      val options = getOptions()
      
      if (args.length == 0) {
        val hf = new HelpFormatter()
        hf.printHelp(getJobClassName, options)
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
  
    def getOptions() : Options
  
    override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
      
      for( o <- getOptions().getOptions.toArray()) {
        
        val optionCasted = o.asInstanceOf[Option]
        
        if(optionCasted.isRequired()) {
          
          if(optionCasted.hasArg()) {
            
            try {
              config.getString(optionCasted.getLongOpt)
            } catch {
              case ex: Exception => {
                return new SparkJobInvalid(ex.getLocalizedMessage)
              }
            }
            
          }
          
        }
        
      }
     
      return SparkJobValid
      
    }

}