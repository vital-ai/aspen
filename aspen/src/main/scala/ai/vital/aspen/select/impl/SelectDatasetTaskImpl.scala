package ai.vital.aspen.select.impl

import ai.vital.aspen.groovy.select.tasks.SelectDataSetTask
import ai.vital.aspen.task.TaskImpl
import ai.vital.aspen.job.AbstractJob
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.commons.io.IOUtils
import ai.vital.vitalservice.factory.VitalServiceFactory
import ai.vital.vitalsigns.VitalSigns
import ai.vital.query.querybuilder.VitalBuilder
import org.apache.spark.rdd.RDD
import java.nio.charset.StandardCharsets
import java.util.Random
import ai.vital.vitalservice.query.VitalSelectQuery
import scala.collection.JavaConversions._
import ai.vital.vitalsigns.model.URIReference
import ai.vital.vitalsigns.uri.URIGenerator
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.hadoop.writable.VitalBytesWritable
import org.apache.hadoop.io.Text
import java.util.Arrays
import ai.vital.vitalsigns.model.property.URIProperty

class SelectDatasetTaskImpl(job: AbstractJob, task: SelectDataSetTask) extends TaskImpl[SelectDataSetTask](job.sparkContext, task) {
  def checkDependencies(): Unit = {
  }

  def execute(): Unit = {
    
    val queryPathParam = task.queryPath
    
    val urisListPath = task.urisListPath
    
    val outputPath = task.outputPath
    
    val overwrite = task.overwrite
    
    val builderPath = new Path(task.modelBuilderPath)
    
    var percent = task.percent
    
    val uriOnly = task.uriOnlyOutput
    
    if(queryPathParam != null) {
      println("query file path: " + queryPathParam)
    }
    
    if(urisListPath != null) {
      println("uris list path: " + urisListPath);
    }
    println("output: " + outputPath)
    println("Overwrite ? " + overwrite)
    println("URI only ? " + uriOnly)
    println("builder path: " + builderPath)

    var outputRDDName : String = null
    
    if(outputPath.startsWith("name:")) {
      outputRDDName = outputPath.substring("name:".length())
      println("output is a namedRDD: " + outputRDDName)
    }
    
    if(outputRDDName != null) {
      
    } else {
      
      val outputBlockPath = new Path(outputPath)
      
      val outpuBlockFS = FileSystem.get(outputBlockPath.toUri(), job.hadoopConfiguration)
      
      //check if output exists
      if(outpuBlockFS.exists(outputBlockPath)) {
        if(!overwrite) {
          throw new RuntimeException("Output file path already exists, use --overwrite option - " + outputBlockPath)
        } else {
//          if(!outpuBlockFS.isFile(outputBlockPath)) {
//            throw new RuntimeException("Output block file path exists but is not a file: " + outputBlockPath)
//          }
          outpuBlockFS.delete(outputBlockPath, true)
        }
      }
      
    }
    
    val builderFS = FileSystem.get(builderPath.toUri(), job.hadoopConfiguration)
    if(!builderFS.exists(builderPath)) {
      throw new RuntimeException("Builder file not found: " + builderPath.toString())
    }
    
    val builderStatus = builderFS.getFileStatus(builderPath)
    if(!builderStatus.isFile()) {
      throw new RuntimeException("Builder path does not denote a file: " + builderPath.toString())
    }
    
    
    val buildInputStream = builderFS.open(builderPath)
    val builderBytes = IOUtils.toByteArray(buildInputStream)
    buildInputStream.close()
    
    
    val modelCreator = job.getModelCreator()
        //not loaded!
    val aspenModel = modelCreator.createModel(builderBytes)
    
    
    
    val profile = job.serviceProfile 
    
    val serviceKey = job.serviceKey
    
    val limit = task.limit
    
    if(limit <= 0) throw new RuntimeException("Limit must be greater than 0: " + limit)
    
    var maxDocs = task.maxDocs
    
    println("limit: " + limit)
    println("maxDocs: " + maxDocs)
    
//    if(profile != null) {
//      println("Setting custom vitalservice profile: " + profile)
//      VitalServiceFactory.setServiceProfile(profile)
//    } else {
//      println("Using default service profile...")
//    }
//    VitalSigns.get.setVitalService( VitalServiceFactory.getVitalService )
    val vitalService = VitalSigns.get.getVitalService
    
    val builder = new VitalBuilder()
    
    var offset = 0
    
    var total = 0
    
    
    var inputURIsList : RDD[(String, Array[Byte])] = null;
    
    var output : RDD[(String, Array[Byte])] = null
    
    
    val expandObjects = urisListPath != null || ( !uriOnly && ( aspenModel.getModelConfig.getFeatureQueries.size() > 0 || aspenModel.getModelConfig.getTrainQueries.size() > 0 ) )
    
  
    if(queryPathParam != null) {

      if(expandObjects) {
        
        inputURIsList = job.sparkContext.parallelize(Array[(String, Array[Byte])]())
        
      } else {
        
        output = job.sparkContext.parallelize(Array[(String, Array[Byte])]())
        
      }
      
      val queryPath = new Path(queryPathParam)
      println("query builder path: " + queryPath)
      val queryFS = FileSystem.get(queryPath.toUri(), job.hadoopConfiguration)
      
      if(!queryFS.exists(queryPath)) throw new RuntimeException("Query builder file not found: " + queryPath.toString())
      
      val queryStream = queryFS.open(queryPath)
      val queryString = IOUtils.toString(queryStream, StandardCharsets.UTF_8.name())
      queryStream.close()
      
      
      val queryObject = builder.queryString(queryString).toQuery()
      
      if(!queryObject.getClass().equals(classOf[VitalSelectQuery])) {
        throw new RuntimeException("Query string must evaluate to VitalSelectQuery (exact)")
      }
      
      val selectQuery = queryObject.asInstanceOf[VitalSelectQuery]
  
      selectQuery.setLimit(limit)
      
      val random = new Random(1000L)
      
      var skipped = 0
      
      
      
      
      while(offset >= 0) {
        
  
        selectQuery.setOffset(offset)
        
        val rl = vitalService.query(selectQuery)
        
        var c = 0
        
        var l = new java.util.ArrayList[(String, Array[Byte])]()
        
        var limitReached = false
        
        for( g <- rl) {
          
          if(maxDocs > 0 && total >= maxDocs) {
            
            limitReached = true
            
          } else {
          
            total = total + 1
            
            var accept = true
            
            if(percent < 100D) {
            
              if(random.nextDouble() * 100D > percent) {
                accept = false
                skipped = skipped + 1
              }
            }
              
            if(accept) {
              
              var bytes : Array[Byte] = null;
              
              //dump to output
              if(uriOnly || expandObjects) {
                
                val uriRef = new URIReference()
                uriRef.setURI(URIGenerator.generateURI(null, classOf[URIReference], true))
                uriRef.setProperty("uRIRef", URIProperty.withString(g.getURI()));
                
                l.add((g.getURI, VitalSigns.get.encodeBlock(Arrays.asList(uriRef))))
                
                //just dump to output
              } else {
                
                l.add((g.getURI, VitalSigns.get.encodeBlock(Arrays.asList(g))))
                
              }
              
            }
              
          }
          
          
        }
        
        if(l.size() > 0) {
          
          if(uriOnly || !expandObjects) {
            
            output = output.union(job.sparkContext.parallelize(l))
            
          } else {
            
            inputURIsList = inputURIsList.union(job.sparkContext.parallelize(l))
            
          }
          
          
        }
        
        if( !limitReached && c == limit ) {
          offset += limit
        } else {
          offset = -1
        }
        
  
      }
      
      println("Total graph objects: " + total + ", skipped: " + skipped)
      
    } else {
      
      inputURIsList = job.sparkContext.sequenceFile(urisListPath.toString(), classOf[Text], classOf[VitalBytesWritable]).map { pair =>
        
        //verify input list is a list of URI elements ?
        (pair._1.toString(), pair._2.get)
      }
      
//      inputBlockRDD = sc.sequenceFile(inputPath.toString(), classOf[Text], classOf[VitalBytesWritable]).map { pair =>
//            (pair._1.toString(), pair._2.get)
//          }
      //just load uris list from collection
      
    }
    
    
    //expand objects now
    if(inputURIsList != null) {
      
      output = inputURIsList.map { pair =>
        
        if(VitalSigns.get.getVitalService() == null) {
          
          val vitalService = VitalServiceFactory.openService(serviceKey, profile)
    
          VitalSigns.get.setVitalService( vitalService )
          
        }
        
        val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
            
        val vitalBlock = new VitalBlock(inputObjects)
        
        //model is not intialized but has feature exctractor that will fetch all the data
        aspenModel.getFeatureExtraction.composeBlock(vitalBlock)
        
        (vitalBlock.getMainObject.getURI, VitalSigns.get.encodeBlock(vitalBlock.toList()))
        
      }
      
    }
    
    if(outputPath.startsWith("name:")) {
      
      println("persisting as named RDD: " + outputRDDName)
      
      if(job.isNamedRDDSupported()) {
    	  job.namedRdds.update(outputRDDName, output)
      } else {
        job.datasetsMap.put(outputRDDName, output)
      }
      
      
    } else {
      
      println("writing results to sequence file: " + outputPath)
      
      val hadoopOutput = output.map( pair =>
        (new Text(pair._1), new VitalBytesWritable(pair._2))
      )
          
      hadoopOutput.saveAsSequenceFile(outputPath)
      
    }
    
    
    task.getParamsMap.put(task.outputPath, java.lang.Boolean.TRUE)
    
  }
}