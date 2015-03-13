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
      
      val options = new Options()
        .addOption(inputOption)
        .addOption(outputOption)
        .addOption(overwriteOption)
      
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
          System.err.println(ex.getLocalizedMessage());
          return
        }
      }
      
      val inputPath = new Path(cmd.getOptionValue(inputOption.getOpt))
      val outputBlockPath = new Path(cmd.getOptionValue(outputOption.getOpt))
      val overwrite = cmd.hasOption(overwriteOption.getOpt)
      
      println("Input path:  " + inputPath.toString())
      println("Output block path:  " + outputBlockPath.toString())
      println("Overwrite ? " + overwrite)
      
      val outPath = outputBlockPath.toString()
      
      var seqOut = false;
      if(outPath.endsWith(".vital.seq")) {
        seqOut = true
        println("Output is a<Text, VitalBytesWritable> sequence file...")
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
      
      var files = MutableList[Path]()
      
      println("Listing files...")
      val inputPaths = collectFiles(inputFS, inputPath, files)
      
      println("Input files count: " + files.size)
      
      

      var blockWriter : BlockCompactStringSerializer = null
      
      var seqWriter : SequenceFile.Writer = null;
      
      if( seqOut ) {
        
        val codec = new GzipCodec()
        
    	  seqWriter = SequenceFile.createWriter(new Configuration(), outpuBlockFS.create(outputBlockPath, overwrite), classOf[Text], classOf[VitalBytesWritable], SequenceFile.CompressionType.RECORD, codec);
         
      } else {
    	  var fos : OutputStream = outpuBlockFS.create(outputBlockPath, overwrite)
 			  if(blockGzip) {
   				  fos = new GzipOutputStream(fos)
 			  }
        blockWriter = new BlockCompactStringSerializer(new OutputStreamWriter(fos, "UTF-8"))
      }
      
      val seqKey = new Text()
      val seqValue = new VitalBytesWritable() 
      
      for(file <- files) {
        
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

        
        if(blockWriter != null) {
          
          blockWriter.startBlock()
          blockWriter.writeGraphObject(msg)
          blockWriter.endBlock()
          
        } 
        
        if(seqWriter != null) {
          
          val block = VitalSigns.get().encodeBlock(Arrays.asList(msg))
          
          seqKey.set(s)
          seqValue.set(block)
          
          seqWriter.append(seqKey, seqValue)
          
        }
          
        
      }

      
      if(blockWriter != null) {
        blockWriter.close()
      }
      
      if(seqWriter != null) {
        seqWriter.close()
      }
      
      println("DONE")
      
      
    }
    
    def error(msg: String) : Unit = {
      System.err.println(msg)
      System.exit(1)
    }
    
    
    def pattern = Pattern.compile("\\d+")
    
    def collectFiles(inputFS: FileSystem, inputDir: Path, res: MutableList[Path]) : Unit = {
      
      for( p <- inputFS.listStatus(inputDir) ) {
       
        //TODO, isDirectory changed to isDir() ?        
        if( p.isDir()) {
          
          
          collectFiles(inputFS, p.getPath, res)
          
          
        } else {
          
          if( pattern.matcher(p.getPath.getName).matches() ) {
            
            res += p.getPath
            
          }
          
        }
        
      }
      
    }
    
}