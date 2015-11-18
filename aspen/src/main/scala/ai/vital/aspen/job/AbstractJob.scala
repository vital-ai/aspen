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
import ai.vital.aspen.model.AspenCollaborativeFilteringPredictionModel
import ai.vital.aspen.model.AspenDecisionTreePredictionModel
import ai.vital.aspen.model.AspenKMeansPredictionModel
import ai.vital.aspen.model.AspenNaiveBayesPredictionModel
import ai.vital.aspen.model.AspenRandomForestPredictionModel
import ai.vital.aspen.model.AspenLinearRegressionModel
import ai.vital.aspen.model.AspenRandomForestRegressionModel
import ai.vital.aspen.model.AspenSVMWithSGDPredictionModel
import ai.vital.aspen.model.AspenLogisticRegressionPredictionModel
import ai.vital.aspen.model.AspenDecisionTreeRegressionModel
import ai.vital.aspen.model.AspenGradientBoostedTreesPredictionModel
import ai.vital.aspen.model.AspenGradientBoostedTreesRegressionModel
import scala.collection.JavaConversions._
import ai.vital.aspen.model.AspenIsotonicRegressionModel
import ai.vital.aspen.model.AspenGaussianMixturePredictionModel
import ai.vital.aspen.model.AspenPageRankPredictionModel
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import scala.Array
import ai.vital.aspen.groovy.AspenGroovyConfig
import ai.vital.aspen.analysis.metamind.AspenMetaMindImageCategorizationModel
import ai.vital.aspen.analysis.alchemyapi.AspenAlchemyAPICategorizationModel
import ai.vital.aspen.groovy.modelmanager.AspenModelDomainsLoader
import ai.vital.vitalsigns.VitalSigns
import ai.vital.vitalsigns.conf.VitalSignsConfig.DomainsStrategy
import ai.vital.vitalservice.VitalStatus
import java.util.LinkedHashMap
import ai.vital.vitalsigns.model.VitalServiceKey
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.properties.Property_hasKey
import ai.vital.vitalservice.factory.VitalServiceFactory
import ai.vital.aspen.analysis.alchemyapi.AspenAlchemyAPISentimentModel


/* this is placeholder code */


trait AbstractJob extends SparkJob with NamedRddSupport {

  //this flag is set when the job is about to be submitted to the jobserver - it skip the named rdd validation that would prevent posting it
  var skipNamedRDDValidation = false
  
  val profileOption = new Option("prof", "profile", true, "optional vitalservice profile option")
  profileOption.setRequired(false)
  
  val defaultServiceKey = "aaaa-aaaa-aaaa"
  
  val serviceKeyOption = new Option("sk", "service-key", true, "service key, xxxx-xxxx-xxxx format, '" + defaultServiceKey + "' if not set")
  serviceKeyOption.setRequired(false)
  
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
  
  var sparkContext : SparkContext = null
  
  var hadoopConfiguration : Configuration = null
  
  var datasetsMap : java.util.HashMap[String, RDD[(String, Array[Byte])]] = null;
  
  var serviceProfile : String = null
  
  var serviceKey : VitalServiceKey = null
  
  def _mainImpl(args: Array[String]) : Unit = {
   
    _mainImpl(args, false)
    
  }
  
  def _mainImpl(args: Array[String], waitForJob: Boolean) : Boolean = {
      
      val parser = new BasicParser();
      
      val options = getOptions()
      
      if (args.length == 0) {
        val hf = new HelpFormatter()
        hf.printHelp(getJobClassName, options)
        return false
      }


      var cmd: CommandLine = null
  
      try {
        cmd = parser.parse(options, args);
      } catch {
        case ex: ParseException => {
          System.err.println(ex.getLocalizedMessage());
          return false
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
        
        if(opt.hasArgs()) {
          
          optValue = cmd.getOptionValues(optName).toList
          
        } else if(opt.hasArg()) {
          
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
            return false
      }
      
      //override default aspen-groovy-datasets location
      val location = AspenConfig.get.getDatesetsLocation
      if(location != null) {
        println("Custom datasets location from config: " + location)
        optionsMap.put("datesetsLocation", location)
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
           return false
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
         
         var jobId : String = null;
         
         try {
        	 val response = client.jobs_post(appName, getJobClassName(), context, sync, paramsString)
           
           val result = response.get("result").asInstanceOf[LinkedHashMap[String, Object]];
           
           jobId = result.get("jobId").asInstanceOf[String]
           
           println("jobId: " + jobId)
           
           println( new ObjectMapper().defaultPrettyPrintingWriter().writeValueAsString(response) )
           
         } catch {
           case ex : Exception => {
             System.err.println("Job posting error: " + ex.getLocalizedMessage)
             return false
           }
         }
         
         if(waitForJob) {
           
           print("Waiting for the job to complete ")
           System.out.flush()
           
           while(true) {
             
        	   var jobResponse = client.jobs_get_details(jobId)
             
             var status = jobResponse.get("status").asInstanceOf[String]
           
             if(status.equalsIgnoreCase("error")) {
               print("\n")
               System.out.flush()
               return false
             } else if(status.equalsIgnoreCase("finished") || status.equalsIgnoreCase("ok")) {
            	 print("\n")
            	 System.out.flush()
               return true
             }
             
        	   print(".")
        	   System.out.flush()
             Thread.sleep(3000)
           }
           
           
           print("\n")
           return true;
           
           
         } else {
           return true
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
          
          return true
      }
      
      
      
    }
  
    def getOptions() : Options
  
    /**
     * this method is locked as it validates the input config options
     */
    override final def validate(sc: SparkContext, config: Config): SparkJobValidation = {
      
      sparkContext = sc
      
      
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
     
      hadoopConfiguration = new Configuration()
      
      if(isNamedRDDSupported()) {
        
      } else {
        datasetsMap = new java.util.HashMap[String, RDD[(String, Array[Byte])]]();
      }
      
      
      try {
    	  serviceProfile = getOptionalString(config, profileOption)
      } catch { 
        case ex: Exception => {
        }
      }
      
      if(serviceProfile == null) serviceProfile = "default"
      
      try {
    	  var k = getOptionalString(config, serviceKeyOption)
        if(k == null) {
          k = defaultServiceKey
        }
        serviceKey = new VitalServiceKey()
        serviceKey.generateURI(null.asInstanceOf[VitalApp])
        serviceKey.set(classOf[Property_hasKey], k)
      } catch { 
      case ex: Exception => {}
      }
      
      
      val datesetsLocation = getOptionalString(config, "datesetsLocation")
      if(datesetsLocation != null) {
        AspenGroovyConfig.get.datesetsLocation = datesetsLocation
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
    
    def getOptionalString(config: Config, option: String) : String = {
    		
    		try {
    			val v = config.getString(option)
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
      creatorMap.put(AspenCollaborativeFilteringPredictionModel.spark_collaborative_filtering_prediction, classOf[AspenCollaborativeFilteringPredictionModel])
      creatorMap.put(AspenDecisionTreePredictionModel.spark_decision_tree_prediction, classOf[AspenDecisionTreePredictionModel])
      creatorMap.put(AspenDecisionTreeRegressionModel.spark_decision_tree_regression, classOf[AspenDecisionTreeRegressionModel])
      creatorMap.put(AspenGaussianMixturePredictionModel.spark_gaussian_mixture_prediction, classOf[AspenGaussianMixturePredictionModel])
      creatorMap.put(AspenGradientBoostedTreesPredictionModel.spark_gradient_boosted_trees_prediction, classOf[AspenGradientBoostedTreesPredictionModel])
      creatorMap.put(AspenGradientBoostedTreesRegressionModel.spark_gradient_boosted_trees_regression, classOf[AspenGradientBoostedTreesRegressionModel])
      creatorMap.put(AspenIsotonicRegressionModel.spark_isotonic_regression, classOf[AspenIsotonicRegressionModel])
      creatorMap.put(AspenKMeansPredictionModel.spark_kmeans_prediction, classOf[AspenKMeansPredictionModel])
      creatorMap.put(AspenLinearRegressionModel.spark_linear_regression, classOf[AspenLinearRegressionModel])
      creatorMap.put(AspenLogisticRegressionPredictionModel.spark_logistic_regression_prediction, classOf[AspenLogisticRegressionPredictionModel])
      creatorMap.put(AspenNaiveBayesPredictionModel.spark_naive_bayes_prediction, classOf[AspenNaiveBayesPredictionModel])
      creatorMap.put(AspenPageRankPredictionModel.spark_page_rank_prediction, classOf[AspenPageRankPredictionModel])
      creatorMap.put(AspenRandomForestPredictionModel.spark_randomforest_prediction, classOf[AspenRandomForestPredictionModel])
      creatorMap.put(AspenRandomForestRegressionModel.spark_randomforest_regression, classOf[AspenRandomForestRegressionModel])
      creatorMap.put(AspenSVMWithSGDPredictionModel.spark_svm_w_sgd_prediction, classOf[AspenSVMWithSGDPredictionModel])
      
      creatorMap.put(AspenMetaMindImageCategorizationModel.metamind_image_categorization, classOf[AspenMetaMindImageCategorizationModel])
      creatorMap.put(AspenAlchemyAPICategorizationModel.alchemy_api_categorization, classOf[AspenAlchemyAPICategorizationModel])
      creatorMap.put(AspenAlchemyAPISentimentModel.alchemy_api_sentiment, classOf[AspenAlchemyAPISentimentModel])
      return creatorMap
    }
    
    def getModelManagerMap() : HashMap[String, String] = {
      
      val m = new HashMap[String, String]()
      
      for(x <- getCreatorMap().entrySet()) {
       
        m.put(x.getKey, x.getValue.getCanonicalName)
        
      }
      
      m
      
    }
    
  def getDataset(datasetName :String) : RDD[(String, Array[Byte])] = {
    
    if(isNamedRDDSupported()) {
      
      val ds = this.namedRdds.get[(String, Array[Byte])](datasetName)
          
      if(!ds.isDefined) throw new RuntimeException("Dataset not loaded: " + datasetName)
      
      return ds.get;
      
    } else {
      
      val ds = datasetsMap.get(datasetName)
          
      if(ds == null) throw new RuntimeException("Dataset not loaded: " + datasetName)
      
      return ds;
      
    }
    
  }
  
  def getDatasetOrNull(datasetName :String) : RDD[(String, Array[Byte])] = {
    		
    		if(isNamedRDDSupported()) {
    			
    			val ds = this.namedRdds.get[(String, Array[Byte])](datasetName)
    					
          if(!ds.isDefined) return null
    			
    			return ds.get;
    			
    		} else {
    			
    			val ds = datasetsMap.get(datasetName)
    					
    			return ds;
    			
    		}
    		
  }
  
  def loadDynamicDomainJars(aspenModel: AspenModel) : Unit = {
    
    val domainJars = aspenModel.getModelConfig.getDomainJars
    
    loadDynamicDomainJarsList(domainJars)
     
  }
  
  def unloadDynamicDomains() : Unit = {

    if( VitalSigns.get.getConfig.domainsStrategy == DomainsStrategy.dynamic ) {
      
      for( dm <- VitalSigns.get.getDomainModels ) {
        println("Unloading some old ontology: " + dm.getURI)
        val vs = VitalSigns.get.deregisterOntology(dm.getURI);
        println("Status: " + vs.getStatus +  " - " + vs.getMessage)
      }
      
      
      //let's call it 20
      var i = 0
      val list = new java.util.ArrayList[String]()
      while( i < 20) {
        list.add("" + i)
        i = i + 1
      }

      val parallel = sparkContext.parallelize(list.toSeq, list.size())
      
      parallel.map { x =>
        for( dm <- VitalSigns.get.getDomainModels ) {
          println("Unloading some old ontology: " + dm.getURI)
          val vs = VitalSigns.get.deregisterOntology(dm.getURI);
          println("Status: " + vs.getStatus +  " - " + vs.getMessage)
        }
        x.length() 
      }.collect()
      
    }

    
  }
  
  def loadDynamicDomainJarsList(domainJars : java.util.List[String]) : Unit = {
    
    
    if(domainJars == null) return
    
    //load it locally
    val loaderLocal = new AspenModelDomainsLoader()
    loaderLocal.loadDomainJars(domainJars)
    
    //distribute that model to all workers
    if( domainJars != null && domainJars.size() > 0) {

      //let's call it 20
      var i = 0
      val list = new java.util.ArrayList[String]()
      while( i < 20) {
        list.add("" + i)
        i = i + 1
      }

      val parallel = sparkContext.parallelize(list.toSeq, list.size())
      
      parallel.map { x =>
        println("Loading dynamic domains: " + domainJars)
        val loader = new AspenModelDomainsLoader()
        loader.loadDomainJars(domainJars)
        x.length() 
      }.collect()
      
    }
      
  }
    
}