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
import spark.jobserver.NamedRddSupport
import spark.jobserver.SparkJobInvalid
import spark.jobserver.SparkJobInvalid
import com.typesafe.config.ConfigException
import ai.vital.aspen.config.AspenConfig
import ai.vital.aspen.jobserver.client.JobServerClient
import org.codehaus.jackson.map.ObjectMapper
import ai.vital.aspen.groovy.modelmanager.ModelCreator
import ai.vital.aspen.groovy.modelmanager.AspenModel
import ai.vital.aspen.model.CollaborativeFilteringPredictionModel
import ai.vital.aspen.model.DecisionTreePredictionModel
import ai.vital.aspen.model.KMeansPredictionModel
import ai.vital.aspen.model.NaiveBayesPredictionModel
import ai.vital.aspen.model.RandomForestPredictionModel
import ai.vital.aspen.model.SparkLinearRegressionModel
import ai.vital.aspen.model.RandomForestRegressionModel


/* this is placeholder code */


trait AbstractJob extends SparkJob with NamedRddSupport {

  //this flag is set when the job is about to be submitted to the jobserver - it skip the named rdd validation that would prevent posting it
  var skipNamedRDDValidation = false
  
  val profileOption = new Option("prof", "profile", true, "optional vitalservice profile option")
  profileOption.setRequired(false)
  
  val masterOption = new Option("m", "master", true, "optional spark masterURL")
  masterOption.setRequired(false)
    
  //job server options
  val jobServerOption = new Option("js", "jobserver", true, "optional job-server location (URL), http:// protocol prefix is optional, if not specified the $VITAL_HOME/vital-config/aspen/aspen.config is used.")
  jobServerOption.setRequired(false)
  
  val appNameOption = new Option("app", "appName", true, "optional appName, required when jobserver mode is enabled")
  appNameOption.setRequired(false)
  
  val contextOption = new Option("ctx", "context", true, "optional context, only used in jobserver mode")
  contextOption.setRequired(false)
  
  val syncOption = new Option("sync", "sync", false, "optional sync flag, only used in jobserver mode")
  syncOption.setRequired(false)
  
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
      
      var hasJobServerOption = false
      
      var hasAppNameOption = false
      
      var hasContextOption = false
      
      var hasSyncOption = false
      
      for(x <- options.getOptions.toArray()) {
        
        val opt = x.asInstanceOf[Option]
        
        val optName = opt.getLongOpt
        
        if(optName.equals(jobServerOption.getLongOpt)) {
          hasJobServerOption = true
        } else if(optName.equals(appNameOption.getLongOpt)) {
          hasAppNameOption = true
        } else if(optName.equals(contextOption.getLongOpt)) {
          hasContextOption = true
        } else if(optName.equals(syncOption.getLongOpt)) {
          hasSyncOption = true
        }
        
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
      
      if(hasJobServerOption && hasAppNameOption && hasContextOption && hasSyncOption) {
        //ok
      } else if(!hasJobServerOption && !hasAppNameOption && !hasContextOption && !hasSyncOption) {
        //ok
      } else {
        System.err.println("Job class: " + getJobClassName() + " is misconfigured, " + 
            "it has to either support all 4 cli options [" + 
            jobServerOption.getLongOpt + ", " + appNameOption.getLongOpt + ", " + contextOption.getLongOpt + ", " + syncOption.getLongOpt + "] " + 
            "or none of them")
            return
      }
      
      val config = ConfigFactory.parseMap(optionsMap)
      
      var jobServerURL : String = null;
      
      if(hasJobServerOption) {
        
        println("This job supports jobserver execution, checking if job server location is enabled")
        
        jobServerURL = getOptionalString(config, jobServerOption)
        
        if(jobServerURL != null) {
          println("custom jobserver URL: " + jobServerURL)
        } else {
          println("no jobserver location, checking aspen config in $VITAL_HOME ...")
          jobServerURL = AspenConfig.get.getJobServerURL
          
          if(jobServerURL != null) {
            println("default jobserer URL: " + jobServerURL)
          } else {
            println("jobserver URL cannot be determined - the job will be ")
          }
          
        }
        
      }
      
      if(jobServerURL != null) {
        
         println("runnning the job remotely in jobserver ...")
        
         val appName = getOptionalString(config, appNameOption)
         if(appName == null || appName.isEmpty) {
           System.err.println("No " + appNameOption.getLongOpt + " parameter - it is required in jobserver mode.")
           return
         }
         
         
         skipNamedRDDValidation = true
         
         //validate it now
         val status = validate(null, config)
         if(status.isInstanceOf[SparkJobInvalid]) {
            throw new RuntimeException("Local spark job validation failed: " + status.asInstanceOf[SparkJobInvalid].reason)
         }
         
         
         val context = getOptionalString(config, contextOption)
        
         val sync = getBooleanOption(config, syncOption)
         
         println("AppName: " + appName)
         println("Context: " + context)
         println("Sync: " + sync)
         println("Posting the job to the server: " + getJobClassName())

         val client = new JobServerClient(jobServerURL)
          
         val paramsString = config.root().render()
         
         try {
        	 val response = client.jobs_post(appName, getJobClassName(), context, sync, paramsString)
           
           println( new ObjectMapper().defaultPrettyPrintingWriter().writeValueAsString(response) )
           
         } catch {
           case ex : Exception => {
             System.err.println("Job posting error: " + ex.getLocalizedMessage)
           }
         }
          
          
      } else {
        
    	    println("runnning the job locally (non-jobserver) ...")      
          
          val masterURL = cmd.getOptionValue(masterOption.getOpt);
      
          val conf = new SparkConf().setAppName(getJobName())
          if (masterURL != null) {
            println("custom masterURL: " + masterURL)
            conf.setMaster(masterURL)
          }
      
          val sc = new SparkContext(conf)
          
              //validate it now
          val status = validate(sc, config)
          if(status.isInstanceOf[SparkJobInvalid]) {
            throw new RuntimeException("Local spark job validation failed: " + status.asInstanceOf[SparkJobInvalid].reason)
          }
        
      	  runJob(sc, config)
      }
      
      
      
    }
  
    def getOptions() : Options
  
    /**
     * this method is locked as it validates the input config options
     */
    override final def validate(sc: SparkContext, config: Config): SparkJobValidation = {
      
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
     
      return subvalidate(sc, config)
      
    }

    /**
     * override this for custom validation
     */
    def subvalidate(sc: SparkContext, config: Config) : SparkJobValidation = {
      SparkJobValid
    }
    
    def getBooleanOption(jobConfig : Config, option: Option) : Boolean = {
      try {
    	  return jobConfig.getBoolean(option.getLongOpt)
      } catch {
        case ex: ConfigException.Missing => {
          return false
        }
      }
    }
    
    def getOptionalString(config: Config, option: Option) : String = {
      
      try {
    	  val v = config.getString(option.getLongOpt)
        if(v.isEmpty()) return null;
        return v;
      } catch {
        case ex : ConfigException.Missing => {
          return null
        }        
      }
      
    }
    
    def addJobServerOptions(options: Options) : Options = {
      options.addOption(jobServerOption).addOption(appNameOption).addOption(contextOption).addOption(syncOption)
    }

    def isNamedRDDSupported() : Boolean = {
      try{
        if(this.namedRdds != null) {
          return true
        } else {
        	return false;        
        }
      } catch { case ex: NullPointerException => {
        return false;        
      }}
    }
    
    def getModelCreator() : ModelCreator = {
      
      val creatorMap = getCreatorMap()
      val modelCreator = new ModelCreator(creatorMap)
      return modelCreator
      
    }
    
    def getCreatorMap() : HashMap[String, Class[_ <: AspenModel]] = {
      
      val creatorMap = new HashMap[String, Class[_ <: AspenModel]];
      creatorMap.put(CollaborativeFilteringPredictionModel.spark_collaborative_filtering_prediction, classOf[CollaborativeFilteringPredictionModel])
      creatorMap.put(DecisionTreePredictionModel.spark_decision_tree_prediction, classOf[DecisionTreePredictionModel]);
      creatorMap.put(KMeansPredictionModel.spark_kmeans_prediction, classOf[KMeansPredictionModel]);
      creatorMap.put(NaiveBayesPredictionModel.spark_naive_bayes_prediction, classOf[NaiveBayesPredictionModel]);
      creatorMap.put(RandomForestPredictionModel.spark_randomforest_prediction, classOf[RandomForestPredictionModel])
      creatorMap.put(RandomForestRegressionModel.spark_randomforest_regression, classOf[RandomForestRegressionModel])
      creatorMap.put(SparkLinearRegressionModel.spark_linear_regression, classOf[SparkLinearRegressionModel]);
      
      return creatorMap
    }
}