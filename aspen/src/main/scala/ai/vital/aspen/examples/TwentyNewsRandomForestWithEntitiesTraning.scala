package ai.vital.aspen.examples

import ai.vital.aspen.job.AbstractJob
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import com.typesafe.config.Config
import com.typesafe.config.ConfigException
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import ai.vital.hadoop.writable.VitalBytesWritable
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.Text
import ai.vital.vitalsigns.VitalSigns
import scala.collection.JavaConversions._
import org.example.twentynews.domain.Message
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.mllib.tree.RandomForest
import java.util.Date
import java.util.Set
import java.util.HashSet
import ai.vital.domain.EntityInstance
import ai.vital.property.MultiValueProperty
import ai.vital.property.IProperty
import java.util.Collection
import java.util.HashMap

object TwentyNewsRandomForestWithEntitiesTraning extends AbstractJob {

  def MIN_DF = 1
  
  def MAX_DF_PERCENT = 100
  
  def getJobName(): String = {
    return "twentynews random forest with features training job"
  }
  
  def ENTITY = "ENTITY__"
  
  def SPANTYPE = "SPANTYPE__"
  
  //expects 20news messages
  val inputOption  = new Option("i", "input", true, "input RDD[(String, Array[Byte])], either RDD name or path:<path>, where path is a .vital.seq file")
  inputOption.setRequired(true)
  
  val outputOption = new Option("mod", "model", true, "output model path (directory)")
  outputOption.setRequired(true)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite model if exists")
  overwriteOption.setRequired(false)
  
  val minDFOption = new Option("minDF", "minimumDocFreq", true, "minimum term document frequency, default: " + MIN_DF)
  minDFOption.setRequired(false)
  
  val maxDFPercentOption = new Option("maxDFP", "maxDocFreqPercent", true, "maximum term document frequency (percent), default: " + MAX_DF_PERCENT)
  maxDFPercentOption.setRequired(false)
  
  def getJobClassName(): String = {
    TwentyNewsRandomForestWithEntitiesTraning.getClass.getCanonicalName
  }
  
  def getOptions(): Options = {
    return new Options()
      .addOption(masterOption)
      .addOption(inputOption)
      .addOption(outputOption)
      .addOption(overwriteOption)
      .addOption(minDFOption)
      .addOption(maxDFPercentOption)
  }
  
   def main(args: Array[String]): Unit = {
    
     _mainImpl(args)
     
   }
   
   def runJob(sc: SparkContext, jobConfig: Config): Any = {
     
    val inputName = jobConfig.getString(inputOption.getLongOpt)
    val outputModelPath = new Path(jobConfig.getString(outputOption.getLongOpt))
    val overwrite = jobConfig.getBoolean(overwriteOption.getLongOpt)

    var minDF = MIN_DF
    var maxDFPercent = MAX_DF_PERCENT
    
    try {
      	minDF = Integer.parseInt(jobConfig.getString(minDFOption.getLongOpt))
    } catch {
      case ex: ConfigException.Missing => {}
    }
    
    try {
    	maxDFPercent = Integer.parseInt(jobConfig.getString(maxDFPercentOption.getLongOpt))
    } catch {
      case ex: ConfigException.Missing => {}
    }
    
    if(minDF < 1) {
      System.err.println("minDF must be > 0")
      return
    }
    
    if(maxDFPercent > 100 || maxDFPercent < 1) {
      System.err.println("maxDFPercent must be within range [1, 100]")
      return
    }
    
    
    
    println("input train name: " + inputName)
    println("output model path: " + outputModelPath)
    println("overwrite if exists: " + overwrite)
    println("minDF: " + minDF)
    println("maxDFPercent: " + maxDFPercent)

    val modelFS = FileSystem.get(outputModelPath.toUri(), new Configuration())

    
    if (modelFS.exists(outputModelPath) && !overwrite) {
      System.err.println("Output model path already exists, use -ow option")
      return
    }
    
    //gid, newsgroup, text
    var inputRDD : RDD[(String, String, String, Set[String], Set[String])] = null

    if(inputName.startsWith("path:")) {
      
      println("test mode - input path: " + inputName)
      
      val inputPath = new Path(inputName.substring(5))
      
      val inputFS = FileSystem.get(inputPath.toUri(), new Configuration())
      
      if (!inputFS.exists(inputPath) || !inputFS.isDirectory(inputPath)) {
        System.err.println("Input train path does not exist or is not a directory: " + inputName)
        return
      }

      inputRDD = sc.sequenceFile(inputPath.toString(), classOf[Text], classOf[VitalBytesWritable]).map{ pair =>
        
        var newsgroup = "X"
        var text = ""
        
        val inputObjects = VitalSigns.get().decodeBlock(pair._2.get, 0, pair._2.get.length)
        
        val entities : Set[String] = new HashSet[String]()
        val spanTypes : Set[String] = new HashSet[String]()
        
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
          
        (pair._1.toString(), newsgroup, text, entities, spanTypes)
        
      }

      //cache it only if it's not a named RDD? 
      inputRDD.cache()
      
    } else {
      
      throw new RuntimeException("namedRDD not supported yet")
//      inputRDD = this.namedRdds.get[(String, String, String)](inputName).get
      
      
    }

    
    val splits = inputRDD.randomSplit(Array(0.6, 0.4), seed = 11L)
    
    val trainRDD = splits(0)
    
    val testRDD = splits(1)
    
    
    //filename->category | message -> extracted text content

    val docsCount = trainRDD.count();
    
    println("Documents count: " + docsCount)
    
    val maxDF = docsCount * maxDFPercent / 100
    
    println("MaxDF: " + maxDF)
    
    //collect distinct categories
    val categoriesRDD: RDD[String] = trainRDD.map { gidNewsgroupText =>

      gidNewsgroupText._2

    }.distinct()
    
    
    val categories: Array[String] = categoriesRDD.toArray().sortWith((s1, s2) => s1.compareTo(s2) < 0)

    println("categories count: " + categories.size)
    
    
    
  	//find all unique entities
  	val entitiesRDD: RDD[String] = trainRDD.flatMap { gidNewsgroupText =>
    	gidNewsgroupText._4.toSeq 
    }.distinct()
    
    val allEntities : Array[String] = entitiesRDD.toArray().sortWith((s1, s2) => s1.compareTo(s2) < 0)
    println("all entities types count: " + allEntities.size)
    
    val spanTypesRDD : RDD[String] = trainRDD.flatMap { gidNewsgroupText =>
      gidNewsgroupText._5.toSeq
    }.distinct()
    
    val allSpanTypes : Array[String] = spanTypesRDD.toArray().sortWith((s1, s2) => s1.compareTo(s2) < 0)
    println("all span types count: " + allSpanTypes.size)
      



    val wordsRDD: RDD[String] = trainRDD.flatMap { gidNewsgroupText =>

      
      var l = new HashSet[String]()

      /*
      val inputObjects = VitalSigns.get().decodeBlock(gidNewsgroupText._2, 0, gidNewsgroupText._2.length)
      
      var msg : Message = null
      
      for(g <- inputObjects) {
        if(g.isInstanceOf[Message]) {
          msg = g.asInstanceOf[Message]
        }
      }
      
      if(msg == null) throw new RuntimeException("No 20 news message found in block")
      
      val text = msg.getProperty("title").toString() + " " + msg.getProperty("body").toString()
      */
      val text = gidNewsgroupText._3
      
      for (x <- text.toLowerCase().split("\\s+")) {
        if (x.isEmpty()) {

        } else {
          l.add(x)
        }
      }

      l.toSeq

    }
    
    val categoriesOS = modelFS.create(new Path(outputModelPath, "categories.tsv"), true)
    
    var i = 0
    for(c <- categories) {
      categoriesOS.write( (i + "\t" + c + "\n").getBytes("UTF-8") )
      println("category: " + i + "\t" + c);
      i += 1
    }
    categoriesOS.close()

    //wordFrequencies
    val wordsOccurences = wordsRDD.map(x => (x, 1)).reduceByKey((i1, i2) => i1 + i2).filter(wordc => wordc._2 >= minDF && wordc._2 <= maxDF)

    var dictionary = new java.util.HashMap[String, Int]()

    var dicList = new java.util.ArrayList[String]()

    var l = wordsOccurences.toLocalIterator.toList

    l.sortBy(e => e._2)

    var counter = 0

    
    val dictionaryFilePath = new Path(outputModelPath, "dictionary.tsv")
    
    var dictionaryOS = modelFS.create(dictionaryFilePath, true)
    
    
    //prepend entities and span types
    for( e <- allEntities ) {
      
      //prefix must be very unlikely to appear in normal text
      val encoded = ENTITY + e
      
      dictionary.put(encoded, counter)
      
      dictionaryOS.write( (counter + "\t" + encoded + "\n").getBytes("UTF-8") )
      
      counter += 1
    }
    
    for( t <- allSpanTypes ) {
      
      val encoded = SPANTYPE + t
      
      dictionary.put(encoded, counter)
      
      dictionaryOS.write( (counter + "\t" + encoded + "\n").getBytes("UTF-8") )
      
      counter += 1
      
    } 
    

    
    for (e <- l) {
      
      if(e._2 >= minDF && e._2 <= maxDF) {
        
        dictionary.put(e._1, counter)
        
        dictionaryOS.write( (counter + "\t" + e._1 + "\n").getBytes("UTF-8") )
        
        counter += 1
        
      }
      
      

    }
    
    dictionaryOS.close()

    println("dictionary size: " + dictionary.size())
    
    
    val vectorized = trainRDD.map { gidNewsgroupText =>

      val catgoryID: Double = categories.indexOf(gidNewsgroupText._2);

      var index2Value: Map[Int, Double] = Map[Int, Double]()

      
      for( entity <- gidNewsgroupText._4 ) {
        val index = dictionary.getOrElse(ENTITY + entity, -1)
        if(index >= 0) {
          index2Value += (index -> 1d)
        }
      }
      
      for( spantype <- gidNewsgroupText._5 ) {
        val index = dictionary.getOrElse(SPANTYPE + spantype, -1)
        if(index >= 0) {
          index2Value += (index -> 1d)
        }
      }
      
      /*
      var msg : Message = null
      
      val inputObjects = VitalSigns.get().decodeBlock(gidNewsgroupText._2, 0, gidNewsgroupText._2.length)
      
      for(g <- inputObjects) {
        if(g.isInstanceOf[Message]) {
          msg = g.asInstanceOf[Message]
        }
      }
      
      if(msg == null) throw new RuntimeException("No 20 news message found in block")
      
      val text = msg.getProperty("title").toString() + " " + msg.getProperty("body").toString()
      */
      
      val text = gidNewsgroupText._3
      
      val words = text.toLowerCase().split("\\s+")

      for (x <- words) {

        val index = dictionary.getOrElse(x, -1)

        if (index >= 0) {

          var v = index2Value.getOrElse(index, 0D);
          v = v + 1
          index2Value += (index -> v)

        }

      }

      val s = index2Value.toSeq.sortWith({ (p1, p2) =>
        p1._1 < p2._1
      })

      LabeledPoint(catgoryID, Vectors.sparse(dictionary.size, s))

    }

//    println("Training dataset size: " + vectorized.count());

    println("Training model...")
    /*
    val numClasses = categories.length
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 100
      
    val model = DecisionTree.trainClassifier(vectorized, numClasses, categoricalFeaturesInfo, impurity,
      maxDepth, maxBins)
    
    println("Model trained, persisting...")

    val modelBinPath = new Path(outputModelPath, "model.bin")
    val modelOS = modelFS.create(modelBinPath, true)
    SerializationUtils.serialize(model, modelOS)
    modelOS.close()
      
    println("Model persisted.")
    */
    
    val catMap = new HashMap[Int, Int]()
    val categoricalFeaturesTotal = allEntities.size + allSpanTypes.size
    
    var x = 0
    while ( x < categoricalFeaturesTotal) {
      catMap.put(x, 2)
      x += 1
    }

    val numClasses = categories.length
    val categoricalFeaturesInfo = catMap.toMap
    val numTrees = 20 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 20
    val maxBins = 32
    
    
    


    val model = RandomForest.trainClassifier(vectorized, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    
    //not until spark 1.3.0
    //model.save(sc, "myModelPath")
      
    val modelBinPath = new Path(outputModelPath, "model.bin")
    val modelOS = modelFS.create(modelBinPath, true)
    SerializationUtils.serialize(model, modelOS)
    modelOS.close()

    
    // Evaluate model on training instances and compute training error
    /*
    val labelAndPreds = vectorized.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / vectorized.count
    println("Training Error = " + trainErr)
    println("Learned classification tree model:\n" + model)
    */
    
    println("Testing ...")
    
    println("Test documents count: " + testRDD.count())
    
    val vectorizedTest = testRDD.map { gidNewsgroupText =>

      val catgoryID: Double = categories.indexOf(gidNewsgroupText._2);

      var index2Value: Map[Int, Double] = Map[Int, Double]()

      
      for( entity <- gidNewsgroupText._4 ) {
        val index = dictionary.getOrElse(ENTITY + entity, -1)
        if(index >= 0) {
          index2Value += (index -> 1d)
        }
      }
      
      for( spantype <- gidNewsgroupText._5 ) {
        val index = dictionary.getOrElse(SPANTYPE + spantype, -1)
        if(index >= 0) {
          index2Value += (index -> 1d)
        }
      }
      
      /*
      var msg : Message = null
      
      val inputObjects = VitalSigns.get().decodeBlock(gidNewsgroupText._2, 0, gidNewsgroupText._2.length)
      
      for(g <- inputObjects) {
        if(g.isInstanceOf[Message]) {
          msg = g.asInstanceOf[Message]
        }
      }
      
      if(msg == null) throw new RuntimeException("No 20 news message found in block")
      
      val text = msg.getProperty("title").toString() + " " + msg.getProperty("body").toString()
      */
      val text = gidNewsgroupText._3
      for (x <- text.toLowerCase().split("\\s+")) {

        val index = dictionary.getOrElse(x, -1)

        if (index >= 0) {

          var v = index2Value.getOrElse(index, 0D);
          v = v + 1
          index2Value += (index -> v)

        }

      }

      val s = index2Value.toSeq.sortWith({ (p1, p2) =>
        p1._1 < p2._1
      })

      LabeledPoint(catgoryID, Vectors.sparse(dictionary.size, s))

    }
    
    
//    val predictionAndLabel = vectorizedTest.map(p => (model.predict(p.features), p.label))
//    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / vectorizedTest.count()
//
//    println("Accuracy: " + accuracy)
    
    // Evaluate model on test instances and compute test error
    val labelAndPreds = vectorizedTest.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / vectorizedTest.count()
//    println("Learned classification forest model:\n" + model.toDebugString)
    
    println("Test Error = " + testErr)
    
    
    println("DONE " + new Date().toString())
   }
  
}