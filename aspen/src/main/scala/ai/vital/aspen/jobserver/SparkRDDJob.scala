package ai.vital.aspen.jobserver

import ai.vital.aspen.job.AbstractJob
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import spark.jobserver.SparkJobValidation
import spark.jobserver.SparkJobInvalid
import spark.jobserver.SparkJobValid
import com.typesafe.config.ConfigException

class SparkRDDJob {}

object SparkRDDJob extends AbstractJob {

//  def jobServerOption = new Option("js", "jobserver", true, "optional job-server location (URL), http:// protocol prefix is optional, if not specified the $VITAL_HOME/vital-config/aspen/aspen.config is used.");
//  jobServerOption.setRequired(false)
  
  def actionOption = new Option("ac", "action", true, "rdd action, valid actions: 'list', 'delete' (required)")
  actionOption.setRequired(true)
  
  def nameOption = new Option("n", "rdd-name", true, "rdd name (required)")
  nameOption.setRequired(false)
  
  def getJobClassName(): String = {
    classOf[SparkRDDJob].getCanonicalName
  }

  def getJobName(): String = {
    "Spark RDD Job"
  }
  
  
  def main(args: Array[String]): Unit = {
    
    _mainImpl(args)
     
  }

  def getOptions(): Options = {
    return new Options()
      .addOption(masterOption)
//      .addOption(jobServerOption)
  }
  
  def runJob(sc: SparkContext, jobConfig: Config): Any = {
    
    val action = jobConfig.getString("action")
    println("action: " + action)
    
    if(action.equals("list")) {
      
      return this.namedRdds.getNames().toList;
      
    } else if(action.equals("remove")) {
      
      val rddName = jobConfig.getString(nameOption.getLongOpt)
      
      println("RDD name: " + rddName)
      
      if( this.namedRdds.get(rddName).isDefined ) {
        
    	  this.namedRdds.destroy(rddName)
        return "RDD with name: " + rddName + " removed successfully"
        
      } else {
        
        return "RDD with name: " + rddName + " not found"
        
      }
      
    }
    
  }
 
  override def subvalidate(sc: SparkContext, jobConfig: Config) : SparkJobValidation = {
    
    val action = jobConfig.getString("action")
    
    if(action.equals("list")) {
      
    } else if(action.equals("remove")) {
      
      try {
    	  jobConfig.getString(nameOption.getLongOpt)
      } catch{
        case ex : ConfigException.Missing => {
        	return new SparkJobInvalid("action '" + action + " requires '" + nameOption.getLongOpt + "' param")
        }
      }
      
    } else {
      return new SparkJobInvalid("unknown action '" + action + "', valid: 'list', 'remove'")
    }
    
    try{
        if(this.namedRdds == null) {
        } 
    } catch { case ex: NullPointerException => {
        return new SparkJobInvalid("Cannot use named RDD feature - no spark job context")
    }}
      
    SparkJobValid
    
  }
}