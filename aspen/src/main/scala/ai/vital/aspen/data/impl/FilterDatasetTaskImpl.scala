package ai.vital.aspen.data.impl

import ai.vital.aspen.groovy.data.tasks.FilterDatasetTask
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.task.TaskImpl
import ai.vital.query.querybuilder.VitalBuilder
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import ai.vital.vitalservice.query.VitalQuery
import ai.vital.vitalsigns.VitalSigns
import ai.vital.vitalsigns.model.VITAL_Container
import java.util.ArrayList
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalservice.factory.VitalServiceFactory
import java.util.Arrays
import scala.collection.JavaConversions._
import ai.vital.vitalsigns.model.GraphMatch
import ai.vital.vitalsigns.model.property.URIProperty

class FilterDatasetTaskImpl(job: AbstractJob, task: FilterDatasetTask) extends TaskImpl[FilterDatasetTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {
  }

  def execute(): Unit = {
  
    val builder = new VitalBuilder()
    
    val queryPath = new Path(task.queryBuilderPath)
    val queryFS = FileSystem.get(queryPath.toUri(), job.hadoopConfiguration)
      
    if(!queryFS.exists(queryPath)) throw new RuntimeException("Query builder file not found: " + queryPath.toString())
      
    val queryStream = queryFS.open(queryPath)
    val queryString = IOUtils.toString(queryStream, StandardCharsets.UTF_8.name())
    queryStream.close()
      
      
    val queryObject = builder.queryString(queryString).toQuery()
      
//    if(!queryObject.getClass().equals(classOf[VitalQuery])) {
//      throw new RuntimeException("Query string must evaluate to VitalSelectQuery (exact)")
//    }
    
    val inputRDD = job.getDataset(task.inputDatasetName);
    
    val blocksIterated = job.sparkContext.accumulator(0)
    
    val blocksFilteredIn = job.sparkContext.accumulator(0)
    
    val outputRDD = inputRDD.flatMap { pair =>
    
      val graphObjects = VitalSigns.get.decodeBlock(pair._2, 0, pair._2.length)
      
      val container = new VITAL_Container(true)
      container.putGraphObjects(graphObjects)
      
      val rs = VitalSigns.get.getVitalService.queryContainers(queryObject, Arrays.asList(container))
      
      var outputURIs : java.util.HashSet[String] = null
      
      val outputList = new ArrayList[GraphObject]()
      
      for( g <- rs ) {

        if(g.isInstanceOf[GraphMatch]) {
         
          if(outputURIs == null) outputURIs = new java.util.HashSet[String]()
          
          for( x <- g.getPropertiesMap) {
            val unwrapped = x._2.unwrapped();
            if(unwrapped.isInstanceOf[URIProperty]) {
              outputURIs.add(unwrapped.asInstanceOf[URIProperty].get)
            }
          }
          
        } else {
          
          outputList.add(g)    
          
        }
        
      }
      
      if(outputURIs != null) {
        
        for(x <- graphObjects) {
          
          if(outputURIs.contains(x.getURI)) {
            outputList.add(x)
          }
          
        }
        
      }
      
      blocksIterated.add(1)
      
      if(outputList.size() > 0) {

        blocksFilteredIn.add(1)
        
        Seq( (pair._1, VitalSigns.get.encodeBlock(outputList)) )
        
      } else {

        Seq()
        
      }
      
      
    }
    
    task.getParamsMap.put(task.outputDatasetName, outputRDD)
    
    if(job.isNamedRDDSupported()) {
      job.namedRdds.update(task.outputDatasetName, outputRDD)
    } else {
      job.datasetsMap.put(task.outputDatasetName, outputRDD)
    }
    
    var stats = "Blocks iterated: " + blocksIterated.value
    stats += "\nBlocks filtered in: " + blocksFilteredIn.value
    
    task.getParamsMap.put(FilterDatasetTask.FILTER_DATASET_STATS, stats)
    
    
  }
  
}