package ai.vital.aspen.examples


import org.apache.commons.cli.BasicParser
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.ParseException
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.MutableList
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import java.util.regex.Pattern
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import java.io.InputStream
import java.io.BufferedOutputStream
import java.io.FileOutputStream
import org.apache.hadoop.io.compress.GzipCodec.GzipOutputStream
import java.io.OutputStream
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import ai.vital.aspen.utils.XMLChar
import ai.vital.vitalsigns.block.BlockCompactStringSerializer
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.compress.GzipCodec
import org.example.twentynews.domain.Message
import ai.vital.domain.Document
import ai.vital.vitalsigns.VitalSigns
import java.util.Arrays
import ai.vital.hadoop.writable.VitalBytesWritable
import ai.vital.domain.TargetNode
import ai.vital.vitalsigns.uri.URIGenerator
import ai.vital.domain.Edge_hasTargetNode
import ai.vital.vitalsigns.model.GraphObject
import java.util.ArrayList
import scala.collection.JavaConversions._
import java.util.Random
import ai.vital.vitalsigns.model.VITAL_Category
import ai.vital.aspen.groovy.modelmanager.ModelTaxonomyQueries
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalservice.VitalStatus
import ai.vital.domain.Edge_hasCategory

object TwentyNewsToVitalBlock {

    def MESSAGES_NS = "http://vital.ai/ontology/twentynews-data/"
  
    def NS = "http://vital.ai/ontology/twentynews#"
    
    def main(args: Array[String]): Unit = {
      
      val parser = new BasicParser();

      val inputOption = new Option("i", "input-dir", true, "input twenty news root dir parent of both 20news-bydate-test and 20news-bydate-train")
      inputOption.setRequired(true)

      val outputOption = new Option("o", "output-block", true, "output vital block file .vital[.gz] or .vital.seq")
      outputOption.setRequired(true)
      
      val overwriteOption = new Option("ow", "overwrite", false, "overwrite output file if exists")
      overwriteOption.setRequired(false)
      
      val targetNodesOption = new Option("tn", "target-nodes", false, "add target nodes")
//      targetNodesOption.setRequired(true)
      
      val categoryEdgesOption = new Option("ce", "category-edges", false, "add category edge")
      
      
      val percentOption = new Option("p", "percent", true, "optional output objects percent limit")
      percentOption.setRequired(false)
      
      val options = new Options()
        .addOption(inputOption)
        .addOption(outputOption)
        .addOption(overwriteOption)
        .addOption(targetNodesOption)
        .addOption(categoryEdgesOption)
        .addOption(percentOption)
        
      if (args.length == 0) {
        val hf = new HelpFormatter()
        hf.printHelp(TwentyNewsToVitalBlock.getClass.getCanonicalName, options)
        return
      }


      var cmd: CommandLine = null
  
      try {
        cmd = parser.parse(options, args);
      } catch {
        case ex: ParseException => {
          error(ex.getLocalizedMessage());
          return
        }
      }
      
      val inputPath = new Path(cmd.getOptionValue(inputOption.getOpt))
      val outputBlockPath = new Path(cmd.getOptionValue(outputOption.getOpt))
      val overwrite = cmd.hasOption(overwriteOption.getOpt)
      val targetNodes = cmd.hasOption(targetNodesOption.getOpt)
      val categoryEdges = cmd.hasOption(categoryEdgesOption.getOpt)
      
      val percentValue = cmd.getOptionValue(percentOption.getOpt)
      var percent = 100D
      if(percentValue != null && !percentValue.isEmpty()) {
         percent = java.lang.Double.parseDouble(percentValue)
      }
      
      
      println("Input path:  " + inputPath.toString())
      println("Output block path:  " + outputBlockPath.toString())
      println("Overwrite ? " + overwrite)
      println("Add target nodes ? " + targetNodes)
      println("Add category edges ? " + categoryEdges)
      println("Output docs percent: " + percent)
      
      if(categoryEdges && targetNodes) {
        error(categoryEdgesOption.getLongOpt + " and " + targetNodesOption.getLongOpt + " options are mutually exclusive")
        return
      }
      
      if(percent <= 0D || percent > 100D) {
        error("percent value must be in (0; 100] range: " + percent)
        return
      }
      
      val outPath = outputBlockPath.toString()
      
      var seqOut = false;
      if(outPath.endsWith(".vital.seq")) {
        seqOut = true
        println("Output is a <Text, VitalBytesWritable> sequence file...")
      } else if(outPath.endsWith(".vital") || outPath.endsWith(".vital.gz")) {
    	  println("Output is a vital block file...")
      } else {
        error("Output block file name must end with .vital[.gz] or .vital.seq")
      }
      
      val blockGzip = outputBlockPath.toString().endsWith(".gz")
      
      val inputFS = FileSystem.get(inputPath.toUri(), new Configuration())
      val outpuBlockFS = FileSystem.get(outputBlockPath.toUri(), new Configuration())
      
      if( ! inputFS.isDirectory(inputPath) ) {
        error("Input path is not a directory: " + inputPath)
      }
      
      if(outpuBlockFS.exists(outputBlockPath)) {
        if(!overwrite) {
          error("Output block file path already exists, use --overwrite option - " + outputBlockPath)
        } else {
          if(!outpuBlockFS.isFile(outputBlockPath)) {
            error("Output block file path exists but is not a file: " + outputBlockPath)
          } 
        }
      }
      
      var categoriesResults : ResultList = null
      
      if(categoryEdges) {
        
        val taxonomyRoot = NS + "Taxonomy"
        println("Checking 20news taxonomy, URI: " + taxonomyRoot)
        
        val root = VitalSigns.get.getIndividual(taxonomyRoot)
        if(root == null) {
          error("Taxonomy root not found: " + taxonomyRoot)
          return
        }
        
        if(!root.isInstanceOf[VITAL_Category]) {
          error("Taxonomy root is not an instance of " + classOf[VITAL_Category].getCanonicalName)
          return
        }
        
        val query = ModelTaxonomyQueries.getTaxonomyPathQuery(root.asInstanceOf[VITAL_Category])
        
        categoriesResults = VitalSigns.get.query(query)
        
        if(categoriesResults.getStatus.getStatus != VitalStatus.Status.ok) {
          error("taxonomy query error: " + categoriesResults.getStatus.getMessage )
          return
        }
        
        
      }
      
      
      var files = MutableList[Path]()
      
      println("Listing files...")
      val inputPaths = collectFiles(inputFS, inputPath, files)
      
      println("Input files count: " + files.size)
      
      

      var blockWriter : BlockCompactStringSerializer = null
      
      var seqWriter : SequenceFile.Writer = null;
      
      var processed = 0;
      var skipped = 0;
      
      if( seqOut ) {
        
//        val codec = new GzipCodec()
        
    	  seqWriter = SequenceFile.createWriter(new Configuration(), outpuBlockFS.create(outputBlockPath, overwrite), classOf[Text], classOf[VitalBytesWritable], SequenceFile.CompressionType.NONE, null);
         
      } else {
    	  var fos : OutputStream = outpuBlockFS.create(outputBlockPath, overwrite)
 			  if(blockGzip) {
   				  fos = new GzipOutputStream(fos)
 			  }
        blockWriter = new BlockCompactStringSerializer(new OutputStreamWriter(fos, "UTF-8"))
      }
      
      val seqKey = new Text()
      val seqValue = new VitalBytesWritable() 
      
      val random = new Random(1000L)
      
      for(file <- files) {
        
        var accept = true
        
        if(percent < 100D) {
          
          if(random.nextDouble() * 100D > percent) {
            accept = false
            skipped = skipped + 1
          }
          
        }
        
        if(accept) {
          
          processed = processed + 1
          
        	var inBody = false;
        	
        	var inSubject = false;
        	
        	var subject = ""
        			
      		var body = ""
        			
      		var message : String = null
        			
      		var newsgroup = file.getParent.getName
        			
      		var is : InputStream = null
          
       		try {
        				
       				is = inputFS.open(file)
        						
      				message = IOUtils.toString(is, "UTF-8")
        						
       		} catch {
        		case ex: Exception => {
        			error(ex.getLocalizedMessage())
         		}
       		} finally {
        			IOUtils.closeQuietly(is)
        	}
        	
          for (line <- scala.io.Source.fromString(message).getLines()) {
        		
        		breakable {
        			//extract title ( subject ) and body
        			if (line.isEmpty()) {
        				
        				if (!inBody) {
        					
        					inSubject = false
        							
        							inBody = true
        							
        							//continue
        							break
        							
        				}
        			}
        			
        			if (inBody) {
        				
        				if (body.length() > 0) body += "\n"
        						
        						body += line
        						
        						//continue
        						
        						break
        						
        			}
        			
        			if (line.startsWith("Subject:")) {
        				inSubject = true
        						subject += line.substring("Subject:".length()).trim();
        			} else if (inSubject && line.startsWith(" ")) {
        				subject += "\n"
        						subject += line
        			} else {
        				inSubject = false;
        			}
        		}
        		
        	}
        	
        	subject = XMLChar.filterXML(subject)
        			body = XMLChar.filterXML(body)
        			
        			
        			val s = MESSAGES_NS + newsgroup + "/" + file.getName
        			
        			val msg = new Message()
        	msg.setURI(s) 
        	msg.setProperty("subject", subject)
        	msg.setProperty("body", body)
        	msg.setProperty("newsgroup", newsgroup)
        	
//        nquadsWriter.handleStatement(vf.createStatement(subjectURI, RDF.TYPE, MESSAGE, context))
//        nquadsWriter.handleStatement(vf.createStatement(subjectURI, hasNewsgroup, vf.createLiteral(newsgroup), context))
//        nquadsWriter.handleStatement(vf.createStatement(subjectURI, hasSubject, vf.createLiteral(subject), context))
//        nquadsWriter.handleStatement(vf.createStatement(subjectURI, hasBody, vf.createLiteral(body), context))
        	
        	val outputObjs = new ArrayList[GraphObject]();
        	outputObjs.add(msg)
        	
        	if(targetNodes) {
        		val targetNode = new TargetNode()
        		targetNode.setURI(URIGenerator.generateURI(null, classOf[TargetNode], false))
        		targetNode.setProperty("targetStringValue", newsgroup)
        		targetNode.setProperty("targetScore", 1D)
        		
        		val targetEdge = new Edge_hasTargetNode()
        		targetEdge.setURI(URIGenerator.generateURI(null, classOf[Edge_hasTargetNode], false))
        		targetEdge.addSource(msg).addDestination(targetNode)
        		
        		outputObjs.add(targetNode)
        		outputObjs.add(targetEdge)
        		
        	}
          
          if(categoryEdges) {
            
        	  val categoryURI = NS + newsgroup
            
            val category = categoriesResults.get(categoryURI)
            
            if(category == null) {
              error("Newsgroup category not found: " + categoryURI)
              return;
            }
            
            
            val categoryEdge = new Edge_hasCategory()
            categoryEdge.setURI(URIGenerator.generateURI(null, classOf[Edge_hasCategory], false))
            categoryEdge.addSource(msg)
            categoryEdge.setDestinationURI(categoryURI)
            
            outputObjs.add(categoryEdge)
            
          }
        	
        	if(blockWriter != null) {
        		
        		blockWriter.startBlock()
        		for(g <- outputObjs) {
        			blockWriter.writeGraphObject(g)
        		}
        		blockWriter.endBlock()
        		
        	} 
        	
        	if(seqWriter != null) {
        		
        		val block = VitalSigns.get().encodeBlock(outputObjs)
        				
        				seqKey.set(s)
        				seqValue.set(block)
        				
        				seqWriter.append(seqKey, seqValue)
        				
        	}
          
        }
        
      }

      
      if(blockWriter != null) {
        blockWriter.close()
      }
      
      if(seqWriter != null) {
        seqWriter.close()
      }
      
      
      println("DONE")
      println("processed: " + processed)
      println("skipped: " + skipped)
      
      
    }
    
    def error(msg: String) : Unit = {
      System.err.println(msg)
      System.exit(1)
    }
    
    
    def pattern = Pattern.compile("\\d+")
    
    def collectFiles(inputFS: FileSystem, inputDir: Path, res: MutableList[Path]) : Unit = {
      
      for( p <- inputFS.listStatus(inputDir) ) {
       
        if( p.isDirectory()) {
          
          
          collectFiles(inputFS, p.getPath, res)
          
          
        } else {
          
          if( pattern.matcher(p.getPath.getName).matches() ) {
            
            res += p.getPath
            
          }
          
        }
        
      }
      
    }
    
}