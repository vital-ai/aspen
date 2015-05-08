package ai.vital.aspen.analysis.testing

import ai.vital.aspen.job.AbstractJob
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.Text
import ai.vital.hadoop.writable.VitalBytesWritable
import spark.jobserver.SparkJobValidation
import spark.jobserver.SparkJobInvalid
import spark.jobserver.SparkJobValid
import ai.vital.aspen.groovy.modelmanager.ModelManager
import ai.vital.aspen.config.AspenConfig
import ai.vital.aspen.groovy.AspenGroovyConfig
import ai.vital.aspen.model.DecisionTreePredictionModel
import ai.vital.aspen.model.NaiveBayesPredictionModel
import ai.vital.aspen.model.RandomForestPredictionModel
import ai.vital.aspen.groovy.modelmanager.AspenModel
import ai.vital.vitalsigns.VitalSigns
import scala.collection.JavaConversions._
import ai.vital.domain.TargetNode
import ai.vital.property.IProperty
import java.util.ArrayList
import ai.vital.vitalservice.segment.VitalSegment
import ai.vital.vitalservice.factory.VitalServiceFactory
import ai.vital.vitalservice.factory.VitalServiceFactory
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.query.querybuilder.VitalBuilder
import ai.vital.vitalservice.query.VitalGraphQuery
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalsigns.model.GraphMatch
import ai.vital.property.URIProperty
import ai.vital.property.URIProperty
import ai.vital.vitalsigns.block.CompactStringSerializer

class ModelTestingJob {}

object ModelTestingJob extends AbstractJob {
  
   def main(args: Array[String]): Unit = {
    
    _mainImpl(args)
     
  }
  
  def getJobClassName(): String = {
    classOf[ModelTestingJob].getCanonicalName
  }
  
  def getJobName(): String = {
     "Model Testing Job"
  }

  
  //expects 20news messages
  val inputOption  = new Option("i", "input", true, "input RDD[(String, Array[Byte])], either named RDD name (name:<name>) or <path> (no prefix), where path is a .vital.seq")
  inputOption.setRequired(true)
  
  val modelOption = new Option("mod", "model", true, "model path (directory or a zip/jar file)")
  modelOption.setRequired(true)
  
  val featureQueryOption = new Option("fq", "feature-query", false, "use model builder query, a model must provide it")
  featureQueryOption.setRequired(false)
  
  val segmentsOption = new Option("segs", "segments", true, "optional segments list (csv), required when feature-query is enabled")
  segmentsOption.setRequired(false)
  
//  val outputOption = new Option("o", "output", true, "optional output RDD[(String, Array[Byte])], either named RDD name (name:<name> or <path> (no prefix), where path is a .vital.seq")
//  outputOption.setRequired(true)
  
  def getOptions(): Options = {
    addJobServerOptions(
    new Options().addOption(masterOption)
      .addOption(inputOption)
      .addOption(modelOption)
      .addOption(featureQueryOption)
      .addOption(segmentsOption)
      .addOption(profileOption)
    )
  }
  
  def runJob(sc: SparkContext, jobConfig: Config): Any = {

    val modelPath = new Path(jobConfig.getString(modelOption.getLongOpt))

    val inputName = jobConfig.getString(inputOption.getLongOpt)
    
    val featureQuery = getBooleanOption(jobConfig, featureQueryOption)
    val segmentsString = getOptionalString(jobConfig, segmentsOption);
    val serviceProfile = getOptionalString(jobConfig, profileOption)
        
    var inputRDDName : String = null
    if(inputName.startsWith("name:")) {
      inputRDDName = inputName.substring("name:".length())
    }
    
    val hadoopConfig = new Configuration()
    
    println("Input name/path: " + inputName)
    println("Model path: " + modelPath)
    
    println("feature query ? " + featureQuery)
    println("segments: " + segmentsString)
    println("service profile: " + serviceProfile)
    
//    val modelManager = new ModelManager()
    val mt2c = AspenGroovyConfig.get.modelType2Class

    mt2c.put(DecisionTreePredictionModel.spark_decision_tree_prediction, classOf[DecisionTreePredictionModel].getCanonicalName)
    mt2c.put(NaiveBayesPredictionModel.spark_naive_bayes_prediction, classOf[NaiveBayesPredictionModel].getCanonicalName)
    mt2c.put(RandomForestPredictionModel.spark_randomforest_prediction, classOf[RandomForestPredictionModel].getCanonicalName)

    
    val modelManager = new ModelManager()
    println("Loading model ...")
    val aspenModel = modelManager.loadModel(modelPath.toUri().toString())
    
    println("Model loaded successfully")
    
    val queryString = aspenModel.getModelConfig.getQuery
    
    val segmentsList = new ArrayList[VitalSegment]()
    
    if(featureQuery) {
      if(queryString == null || queryString.isEmpty() ) {
        throw new RuntimeException("Cannot use feture-query - the model builder does not provide query string")
      }
      
      if(segmentsString == null || segmentsString.isEmpty()) {
        throw new RuntimeException("segments param is required with feature-query")
      }
      
      for( s <- segmentsString.split(",") ) {
        val t = s.trim()
        if(t.length() > 0) {
          segmentsList.add(VitalSegment.withId(t))
        }
      }
      
      if(segmentsList.size() < 1) throw new RuntimeException("No segments decoded from string: " + segmentsString)
      
      if(serviceProfile != null) {
        VitalServiceFactory.setServiceProfile(serviceProfile)
      }
      
    }
    
      //URI, category, text
    var inputBlockRDD : RDD[(String, Array[Byte])] = null
    
    
    if(inputRDDName == null) {
      
      println("loading data from path...")
      
        val inputPath = new Path(inputName)
        
        val inputFS = FileSystem.get(inputPath.toUri(), hadoopConfig)
        
        if (!inputFS.exists(inputPath) /*|| !inputFS.isDirectory(inputPath)*/) {
          throw new RuntimeException("Input test path does not exist " + /*or is not a directory*/ ": " + inputPath.toString())
        }
        
        val inputFileStatus = inputFS.getFileStatus(inputPath)
        
        if(inputName.endsWith(".vital") || inputName.endsWith(".vital.gz")) {
            
            if(!inputFileStatus.isFile()) {
              throw new RuntimeException("input path indicates a block file but does not denote a file: " + inputName)
            }
            throw new RuntimeException("Vital block files not supported yet")
            
        } else {
          
          inputBlockRDD = sc.sequenceFile(inputPath.toString(), classOf[Text], classOf[VitalBytesWritable]).map { pair =>
            (pair._1.toString(), pair._2.get)
          }
          
        }
        
    } else {
      
      println("loading data from named rdd...")
      
      inputBlockRDD = this.namedRdds.get[(String, Array[Byte])](inputRDDName).get
      
    }
    
    //matched, morethan1 input target, no targets
    val results : RDD[(Int, Int, Int, Int)] = inputBlockRDD.map { pair =>
      
        var block : java.util.List[GraphObject] = null
      
        if(featureQuery) {
        
        val _uri = pair._1
        val qString = queryString.replace("$URI", _uri)
        val vitalQuery = new VitalBuilder().queryString(qString).toQuery()
        
        if(serviceProfile != null && !VitalServiceFactory.getServiceProfile.equals(serviceProfile)) {
          VitalServiceFactory.setServiceProfile(serviceProfile)
        }
        
        vitalQuery.setSegments(segmentsList)
        
        if(vitalQuery.isInstanceOf[VitalGraphQuery]) {
          vitalQuery.asInstanceOf[VitalGraphQuery].setPayloads(true)
        }
        
        val rs = VitalServiceFactory.getVitalService.query(vitalQuery)
        
        if(rs.getStatus().getStatus != VitalStatus.Status.ok) {
          throw new RuntimeException("Query exception: " + rs.getStatus.getMessage)
        }
        
        block = new ArrayList[GraphObject]()
        
        for(g <- rs) {
          if(g.isInstanceOf[GraphMatch]) {
            //make sure payloads is e
            for( e <- g.getPropertiesMap.entrySet() ) {
              
              val un = e.getValue.unwrapped();
              
              if(un.isInstanceOf[URIProperty]) {
                
                try {
                  val uri = un.asInstanceOf[URIProperty].get()
                  val x = CompactStringSerializer.fromString(g.getProperty(uri).toString())
                  if(x != null) {
                    block.add(x)
                  }
                } catch {
                  case ex : Exception => {}
                }
                
              }
              
            }
          } else {
            block.add(g)
          }
        }
        
      } else {
        
    	  block = VitalSigns.get.decodeBlock(pair._2, 0, pair._2.length)
        
      }
      
      var targetValue : String = null 
      
      var matched = 0
      
      var moreThanOne = 0
      
      var noTargets = 0
      
      for( b <- block ) {
        if(b.isInstanceOf[TargetNode]) {
          val pv = b.getProperty("targetStringValue");
          if(pv != null) {
            if(targetValue != null) {
              moreThanOne = 1           
            }
            targetValue = pv.asInstanceOf[IProperty].toString()
          }
        }
      }
      
      if(targetValue != null) {
        
    	    val predictions = aspenModel.predict(block)
          
          for(p <- predictions) {
            
            if(p.isInstanceOf[TargetNode]) {
              
              val pv = p.getProperty("targetStringValue");
              if(pv != null) {              
            	  val prediction = pv.asInstanceOf[IProperty].toString()
                
                if(targetValue.equals(prediction)) {
                  matched = 1
                }
                
              }
              
            }
            
          }
          
      } else {
        noTargets = 1
      }
      

      (1, matched, moreThanOne, noTargets)
      
    }
    
    val reduced = results.reduce { ( a, b ) =>
      (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4)
    }
    
    val samples = reduced._1 - reduced._4
    
    val output = new StringBuilder("Total test samples: " + reduced._1);
    output.append("\nSamples with targets: " + ( samples) )
    output.append("\nSamples with more than 1 input target: " + reduced._3)
    output.append("\nCorrect predictions: " + reduced._2)
    
    if(samples > 0) {
      
      output.append("\nAccuracy: " + (reduced._2.doubleValue() / samples.doubleValue()))
      
    } else {
      
      output.append("\nno accuracy stats - no samples with targets")
      
    }
    
    println(output.toString())
    
    return output.toString()
    
  }
  
  override def subvalidate(sc: SparkContext, config: Config) : SparkJobValidation = {
    
    val inputValue = config.getString(inputOption.getLongOpt)
    
    if(!skipNamedRDDValidation && inputValue.startsWith("name:")) {
      
      val inputRDDName = inputValue.substring("name:".length)
      
      try{
        if(this.namedRdds == null) {
        } 
      } catch { case ex: NullPointerException => {
        return new SparkJobInvalid("Cannot use named RDD output - no spark job context")
        
      }}
      
      val inputRDD = this.namedRdds.get[(String, Array[Byte])](inputRDDName)
//      val rdd = this.namedRdds.get[(Long, scala.Seq[String])]("dictionary")
      
      if( !inputRDD.isDefined ) SparkJobInvalid("Missing named RDD [" + inputRDDName + "]")
        
      
    }
    
    SparkJobValid
    
  }
}