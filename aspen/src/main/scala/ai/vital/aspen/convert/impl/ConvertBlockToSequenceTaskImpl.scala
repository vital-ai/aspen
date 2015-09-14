package ai.vital.aspen.convert.impl

import ai.vital.aspen.task.TaskImpl
import ai.vital.aspen.groovy.convert.tasks.ConvertBlockToSequenceTask
import org.apache.spark.SparkContext
import scala.collection.JavaConversions._
import org.apache.hadoop.fs.Path
import ai.vital.vitalsigns.block.BlockCompactStringSerializer
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.hadoop.fs.FileSystem
import ai.vital.aspen.job.AbstractJob
import java.util.ArrayList
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import org.apache.spark.rdd.RDD
import ai.vital.vitalsigns.VitalSigns
import ai.vital.hadoop.writable.VitalBytesWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.hadoop.io.SequenceFile.CompressionType
import java.util.HashSet
import java.util.zip.GZIPInputStream
import java.nio.charset.StandardCharsets
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.BlockIterator
import org.apache.hadoop.io.compress.CompressionCodec
import ai.vital.aspen.data.LoaderSingleton
import ai.vital.vitalservice.impl.UpgradeDowngradeProcedure
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalsigns.binary.VitalSignsBinaryFormat
import java.util.Map

class ConvertBlockToSequenceTaskImpl(job: AbstractJob, task: ConvertBlockToSequenceTask) extends TaskImpl[ConvertBlockToSequenceTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {
  }

  def execute(): Unit = {
    
//    task.inputPaths
    if( task.outputPath .startsWith("name:") ) {
      
      handleRDD(task.outputPath.substring(5))
      
    } else {
      
      handleFS()
      
    }
    
    task.getParamsMap.put(ConvertBlockToSequenceTask.VITAL_BLOCK_TO_SEQUENCE_PREFIX + task.outputPath, new java.lang.Boolean(true));
    
  }
  
  def handleRDD(name : String) : Unit = {
    
    if( ! job.isNamedRDDSupported() ) {
      throw new RuntimeException("NamedRDDs not supported")
    }
    
    var outputRDD : RDD[(String, Array[Byte])] = null;
    
    var c = 0
    
    val loader = LoaderSingleton.getActiveInputLoader()
    
		for(x <- task.inputPaths ) {

			  val inputPath = new Path(x)
        
        val fs = FileSystem.get(inputPath.toUri(), job.hadoopConfiguration)
			  
        val inputStream = fs.open(inputPath)
        
        val reader = new BufferedReader(new InputStreamReader(inputStream))
        
			  val buffer = new ArrayList[VitalBlock]()
        
        /* loader no longer needed, objects versions are persisted in seq files
        if(loader != null) {
          
          
          var line = reader.readLine()
          
          var lastBlock = new ArrayList[GraphObject]()

          var inblock = false
          
          while(line != null) {
            
            if(line.isEmpty() || line.startsWith(BlockCompactStringSerializer.DOMAIN_HEADER_PREFIX)) {
            } else if(line.startsWith(BlockCompactStringSerializer.BLOCK_SEPARATOR)) {
              
              inblock = true;
              
              if(lastBlock.size() > 0) {
                
                c = c+1
                
                buffer.add(new VitalBlock(lastBlock));
                
                outputRDD = flushBufferToRDD(outputRDD, buffer, false)
                
                lastBlock = new ArrayList[GraphObject]();
               
              }
              
            } else {
              
              lastBlock.add(loader.readConverted(line))
              
            }
            
        	  line = reader.readLine()
          }

          if (lastBlock.size() > 0) {
  
            c = c + 1
  
            buffer.add(new VitalBlock(lastBlock));
  
            outputRDD = flushBufferToRDD(outputRDD, buffer, false)
  
            lastBlock = new ArrayList[GraphObject]();
  
          }
          
          outputRDD = flushBufferToRDD(outputRDD, buffer, true)
          
          reader.close()
          
          
        } else {
         */
        	val iter = BlockCompactStringSerializer.getBlocksIterator(reader)
        			
          //buffer 1000 blocks
        			
        			
        	while(iter.hasNext()) {
        				
        	  val block = iter.next()
        						
            c = c+1
        						
        		buffer.add(block)
        						
        		outputRDD = flushBufferToRDD(outputRDD, buffer, false)
        						
          }
          
        	outputRDD = flushBufferToRDD(outputRDD, buffer, true)
          
          iter.close()
          
//        }
        
        
        
        
		}
    
    job.namedRdds.update(name, outputRDD)
    
    println("Blocks count: " + c)
    
  }
  
  private def flushBufferToRDD(rdd : RDD[(String, Array[Byte])], buffer : ArrayList[VitalBlock], forced: java.lang.Boolean ) : RDD[(String, Array[Byte])] = {

    var outputRDD : RDD[(String, Array[Byte])] = null
    
    if(forced || buffer.size() >= 1000) {
      
      val converted = new ArrayList[(String, Array[Byte])](buffer.size())

      for(x <- buffer) {
        
        converted.add((x.getMainObject.getURI, VitalSigns.get.encodeBlock(x.toList())))
        
      }
      
      buffer.clear()
      
      if(rdd == null) {
        
        outputRDD = job.sparkContext.parallelize(converted)
        
      } else {
        
        outputRDD = rdd.union(job.sparkContext.parallelize(converted))
        
      }
      
    }

    if(outputRDD == null) {
      outputRDD = job.sparkContext.parallelize(Seq[(String, Array[Byte])]())
    }
    
    outputRDD
    
  }
  
  def handleFS() : Unit = {
    
//    val outputStream
    
    val outputPath = new Path(task.outputPath)
    
    val outputFS = FileSystem.get(outputPath.toUri(), job.hadoopConfiguration);
    
    val key = new Text();
    
    val value = new VitalBytesWritable();
    
    var compression = CompressionType.NONE
    
    var codec : CompressionCodec = null;
    
//    if(task.outputPath.endsWith(".seq.gz")) {
      
    	compression = CompressionType.RECORD
      
      codec = new DefaultCodec()
      
//    }
    
    val writer = SequenceFile.createWriter(outputFS, job.hadoopConfiguration, outputPath, key.getClass(), value.getClass(), compression, codec);
    
//    val duplicatedURIs = new HashSet[String]();
    
    var c = 0
    
		for(x <- task.inputPaths ) {
			  
		  val inputPath = new Path(x)
      
      val inputFS = FileSystem.get(inputPath.toUri(), job.hadoopConfiguration);

      var inputStream : java.io.InputStream = inputFS.open(inputPath);
      
      if(inputPath.getName().endsWith(".gz")) {
        inputStream = new GZIPInputStream(inputStream);
      }
      
      
      val br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8.name()));
      /*
      val loader = LoaderSingleton.getActiveInputLoader()
      
      if(loader != null) {

        var line = br.readLine()
        
        var lastBlock = new ArrayList[GraphObject]()

        var inblock = false

        while (line != null) {

          if (line.isEmpty() || line.startsWith(BlockCompactStringSerializer.DOMAIN_HEADER_PREFIX)) {
          } else if (line.startsWith(BlockCompactStringSerializer.BLOCK_SEPARATOR)) {

            inblock = true;

            if (lastBlock.size() > 0) {


              key.set(lastBlock.get(0).getURI());
              
              value.set(VitalSignsBinaryFormat.encodeBlock(lastBlock))
              
              writer.append(key, value);
              
              c = c + 1

              lastBlock = new ArrayList[GraphObject]();

            }

          } else {

            lastBlock.add(loader.readConverted(line))

          }

          line = br.readLine()
        }

        if (lastBlock.size() > 0) {

          key.set(lastBlock.get(0).getURI());
              
          value.set(VitalSignsBinaryFormat.encodeBlock(lastBlock))
              
          writer.append(key, value);
              
          c = c + 1

          lastBlock = new ArrayList[GraphObject]();

        }
        
      } else {
        */
        
    	  var blocksIterator : BlockIterator = BlockCompactStringSerializer.getBlocksIterator(br)
    			  
//      var duplicatedURIsCount = null
    			  
    			  while( blocksIterator.hasNext() ) {
    				  
    				  val block = blocksIterator.next();
    				  
    				  val mainObject = block.getMainObject();
    				  
    				  val u = mainObject.getURI();
    				  
//        u = u.substring(u.lastIndexOf('/')+1);
    				  
//        if(!duplicatedURIs.add(u)) {
//          duplicatedURIsCount++;
//          continue;
//        }
    				  
    				  key.set(mainObject.getURI());
    				  
              var overriddenDomainVersions : Map[String, String] = null;
              
              
              val loader = LoaderSingleton.getActiveInputLoader()
              
              if(loader != null) {
                overriddenDomainVersions = loader.getDomainURI2VersionMap
              }
              
              val encoded = VitalSignsBinaryFormat.encodeBlock(block.toList(), overriddenDomainVersions);
              
              val s = new String(encoded, "UTF-8")
              
    				  value.set(encoded);
    				  
    				  writer.append(key, value);
    				  
    				  c = c+1
    						  
    			  }
      
        blocksIterator.close();
        
//      }
      

		}

    writer.close();
    
    println("Blocks count: " + c)
    
  }
}