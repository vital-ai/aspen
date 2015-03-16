package ai.vital.aspen.examples

import java.util.LinkedHashMap
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.mapAsScalaMap
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.io.IOUtils
import org.apache.commons.lang.SerializationUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.linalg.Vectors
import com.typesafe.config.Config
import spark.jobserver.NamedRddSupport
import org.apache.spark.rdd.RDD
import au.com.bytecode.opencsv.CSVReader
import au.com.bytecode.opencsv.CSVWriter
import java.io.StringReader
import java.io.StringWriter
import ai.vital.aspen.job.AbstractJob
import org.apache.hadoop.io.Text
import ai.vital.hadoop.writable.VitalBytesWritable
import ai.vital.vitalsigns.VitalSigns
import org.example.twentynews.domain.Message
import ai.vital.domain.Edge_hasTargetNode
import ai.vital.vitalservice.model.App
import ai.vital.domain.TargetNode
import org.apache.spark.mllib.tree.model.RandomForestModel
import ai.vital.domain.EntityInstance
import java.util.Set
import java.util.HashSet
import ai.vital.property.IProperty
import java.util.Collection
import scala.collection.JavaConversions._

object TwentyNewsRandomForestWithEntitiesClassification extends AbstractJob {

  def getJobClassName(): String = {
     return TwentyNewsRandomForestWithEntitiesClassification.getClass.getCanonicalName
  }

  def getJobName(): String = {
     return "twentynews random forest with entities classification"
  }
  
  def ENTITY = "ENTITY__"
  
  def SPANTYPE = "SPANTYPE__"

  val modelOption = new Option("mod", "model", true, "input model path (directory)")
  modelOption.setRequired(true)
  
  val inputNameOption = new Option("in", "input-name", true, "input RDD[(String, String, String)] (gid,newsgroup,text) name")
  inputNameOption.setRequired(true)
  
  val outputNameOption = new Option("out", "output-name", true, "output RDD[(String, String)] (gid, categoryquads) name")
  outputNameOption.setRequired(true)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite model if exists")
  overwriteOption.setRequired(false)
  

  
  def getOptions(): Options = {
    return new Options()
      .addOption(masterOption)
      .addOption(modelOption)
      .addOption(inputNameOption)
      .addOption(outputNameOption)
      .addOption(overwriteOption)
  }
  
  def main(args: Array[String]): Unit = {
    _mainImpl(args)
  }
  
  def runJob(sc: SparkContext, jobConfig: Config): Any = {
    
    val modelPath = new Path(jobConfig.getString(modelOption.getLongOpt))
    val inputRDDName = jobConfig.getString(inputNameOption.getLongOpt)
    val outputRDDName = jobConfig.getString(outputNameOption.getLongOpt)
    
  val overwrite = jobConfig.getBoolean(overwriteOption.getLongOpt)

    
    println("Model path     : " + modelPath)
    println("Input RDD name : " + inputRDDName)
    println("Output RDD name: " + outputRDDName)
    println("Overwrite: " + overwrite)
    
    if(outputRDDName.startsWith("path:")) {
      
      val outPath = new Path(outputRDDName.substring(5))
      val outFS = FileSystem.get(outPath.toUri(), new Configuration())
      
      if(outFS.exists(outPath)) {
        
        if(!overwrite) {
        	System.err.println("Output path already exists: " + outPath.toString() + " - user --overwrite param")
        	return
        }
        
        outFS.delete(outPath, true)
        
      }
      
    }
    
    
    
    var inputRDD : RDD[(String, Array[Byte])] = null
    
    if(inputRDDName.startsWith("path:")) {
      
      println("test mode - input path: " + inputRDDName)
      
      val inputPath = new Path(inputRDDName.substring(5))
      
      val inputFS = FileSystem.get(inputPath.toUri(), new Configuration())
      
      if (!inputFS.exists(inputPath) /*|| !inputFS.isDirectory(inputPath)*/) {
        System.err.println("Input train path does not exist " + /*or is not a directory*/ ": " + inputRDDName)
        return
      }

      inputRDD = sc.sequenceFile(inputPath.toString(), classOf[Text], classOf[VitalBytesWritable]).map{ pair =>
      
//        var newsgroup = "X"
//        var text = ""
//        
//        val inputObjects = VitalSigns.get().decodeBlock(pair._2.get, 0, pair._2.get.length)
//        
//        for( g <- inputObjects ) {
//          if(g.isInstanceOf[Message]){
//            newsgroup = g.getProperty("newsgroup").toString()
//            val title = g.getProperty("title")
//            val body = g.getProperty("body")
//            if(title != null) text = text + title + "\n"
//            text += body
//          }
//        } 
//      (pair._1.toString(), newsgroup, text)
          (pair._1.toString(), pair._2.get)
        
      }
      
      //cache it only if it's not a named RDD? 
      inputRDD.cache()
      
    } else {
      
      throw new RuntimeException("Input named RDD not supported!")
//      inputRDD = this.namedRdds.get[(String, String, String)](inputRDDName).get
      
      
    }
    
    
    val modelFS = FileSystem.get(modelPath.toUri(), new Configuration())
    
    println("Loading model files from path: " + modelPath.toString())
    
    val categoriesPath = new Path(modelPath, "categories.tsv");
    val dictionaryPath = new Path(modelPath, "dictionary.tsv");
    val modelBinPath = new Path(modelPath, "model.bin");
    
    if(!modelFS.exists(modelPath) || !modelFS.isDirectory(modelPath)) {
      throw new RuntimeException("Model path does not exist or is not a directory: " + modelPath)
    }
    
    if(!modelFS.exists(categoriesPath) || !modelFS.isFile(categoriesPath)) {
      throw new RuntimeException("Categories file does not exist or is not a file: " + categoriesPath);
    }
    
    if(!modelFS.exists(dictionaryPath) || !modelFS.isFile(dictionaryPath)) {
    	throw new RuntimeException("Dictionary file does not exist or is not a file: " + dictionaryPath);
    }
    
    if(!modelFS.exists(modelBinPath) || !modelFS.isFile(modelBinPath)) {
    	throw new RuntimeException("Model binary file does not exist or is not a file: " + modelBinPath);
    }
    
    val categoriesMap = new LinkedHashMap[Int, String]()
    val dictionaryMap = new LinkedHashMap[String, Int]()
    
    val categoriesIS = modelFS.open(categoriesPath)
    for(catLine <- IOUtils.readLines(categoriesIS, "UTF-8")) {
      if(!catLine.isEmpty()) {
    	  val s = catLine.split("\t")
        categoriesMap.put(Integer.parseInt(s(0)), s(1))
      }
    }
    categoriesIS.close()
    
    println("Categories loaded: " + categoriesMap.size() + " : " + categoriesMap)
    
    val dictionaryIS = modelFS.open(dictionaryPath)
    for(dictLine <- IOUtils.readLines(dictionaryIS, "UTF-8")) {
      if(!dictLine.isEmpty()) {
        val s = dictLine.split("\t")
        dictionaryMap.put(s(1), Integer.parseInt(s(0)))
      }
    }
    dictionaryIS.close()
    
    println("Dictionary size: " + dictionaryMap.size())
    
    var modelIS = modelFS.open(modelBinPath)
    val deserializedModel = SerializationUtils.deserialize(modelIS)
    modelIS.close()
    
    val model : RandomForestModel = deserializedModel match {
      case x: RandomForestModel => x
      case _ => throw new ClassCastException
    }

     
    val vectorized = inputRDD.map { gidNewsgroupText =>

      var index2Value: Map[Int, Double] = Map[Int, Double]()

      var newsgroup = ""
      var text = ""
      
      val entities : Set[String] = new HashSet[String]()
      val spanTypes : Set[String] = new HashSet[String]()
      
      val inputObjects = VitalSigns.get().decodeBlock(gidNewsgroupText._2, 0, gidNewsgroupText._2.length)
        
      for( g <- inputObjects ) {
        if(g.isInstanceOf[Message]){
          newsgroup = g.getProperty("newsgroup").toString()
          val title = g.getProperty("title")
          val body = g.getProperty("body").toString()
          if(title != null) text = text + title.toString() + "\n"
          text += body
        }
        if(g.isInstanceOf[EntityInstance]) {
          if( g.getProperty("exactString")  != null ) {
            val entity = g.getProperty("exactString").toString()
            entities.add(entity.toLowerCase().replaceAll("\\s+", " ").trim())
          }
          if( g.getProperty("spanType") != null) {
            val st = g.getProperty("spanType").asInstanceOf[IProperty].rawValue().asInstanceOf[Collection[Any]]
            for( t <- st ) {
              spanTypes.add(t.asInstanceOf[String])
            }
          }
          
          }
      } 
      
      for( entity <- entities ) {
        val index = dictionaryMap.getOrElse(ENTITY + entity, -1)
        if(index >= 0) {
          index2Value += (index -> 1d)
        }
      }
      
      for( spantype <- spanTypes ) {
        val index = dictionaryMap.getOrElse(SPANTYPE + spantype, -1)
        if(index >= 0) {
          index2Value += (index -> 1d)
        }
      }
      
      
      val words = text.toLowerCase().split("\\s+")

      for (x <- words) {

        val index = dictionaryMap.getOrElse(x, -1)

        if (index >= 0) {

          var v = index2Value.getOrElse(index, 0D);
          v = v + 1
          index2Value += (index -> v)

        }

      }

      val s = index2Value.toSeq.sortWith({ (p1, p2) =>
        p1._1 < p2._1
      })

      (gidNewsgroupText._1, gidNewsgroupText._2, Vectors.sparse(dictionaryMap.size, s))

    }
    
    
    val gidOutQuadsRDD = vectorized.map { p =>
      
      val app = new App()
      app.setCustomerID("customer")
      app.setID("app")
      
      val category = model.predict(p._3)
      
      val label = categoriesMap.get(category.intValue())
      
      //n-triple in the short term
      val inputObjects = VitalSigns.get().decodeBlock(p._2, 0, p._2.length)
      
      val target = new TargetNode().generateURI(app).asInstanceOf[TargetNode]
      target.setProperty("name", label)
      target.setProperty("targetScore", 1D)
      
      val targetEdge = new Edge_hasTargetNode().addDestination(target).generateURI(app).asInstanceOf[Edge_hasTargetNode]
      targetEdge.setSourceURI(p._1)
      
      
      inputObjects.add(target)
      inputObjects.add(targetEdge)
      
      (p._1, VitalSigns.get().encodeBlock(inputObjects))
      
    }
    
    
    if(outputRDDName.startsWith("path:")) {
      
       val hadoopOutput = gidOutQuadsRDD.map( pair =>
        (new Text(pair._1), new VitalBytesWritable(pair._2))
       )
      
       hadoopOutput.saveAsSequenceFile(outputRDDName.substring(5))
      
    } else {
//        this.namedRdds.update(outputRDDName, gidOutQuadsRDD)
      throw new RuntimeException("Named RDD not supported!")
    }
    
  }

}