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
import com.franz.openrdf.rio.nquads.NQuadsWriter
import java.io.BufferedOutputStream
import java.io.FileOutputStream
import org.openrdf.model.impl.StatementImpl
import org.openrdf.model.ValueFactory
import org.openrdf.model.impl.ValueFactoryImpl
import org.openrdf.model.vocabulary.RDF
import org.apache.hadoop.io.compress.GzipCodec.GzipOutputStream
import java.io.OutputStream
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import ai.vital.aspen.utils.XMLChar
import com.hp.hpl.jena.rdf.model.ResourceFactory

object TwentyNewsToVitalBlock {

    val vf : ValueFactory = new ValueFactoryImpl()
  
    def MESSAGES_NS = "http://vital.ai/ontology/twentynews-data/"
  
    def NS = "http://vital.ai/ontology/twentynews#"
    
    def MESSAGE = vf.createURI(NS + "Message")
    
    def hasSubject = vf.createURI(NS + "hasSubject")
    
    def hasBody = vf.createURI(NS + "hasBody")
    
    def hasNewsgroup = vf.createURI(NS + "hasNewsgroup")
    
    def hasCategory = vf.createURI(NS + "hasCategory")
  
    
    def hasSubjectJena = ResourceFactory.createProperty(NS + "hasSubject")
    
    def hasBodyJena = ResourceFactory.createProperty(NS + "hasBody")
    
    def hasNewsgroupJena = ResourceFactory.createProperty(NS + "hasNewsgroup")
    
    def hasCategoryJena = ResourceFactory.createProperty(NS + "hasCategory")
    
    
    def main(args: Array[String]): Unit = {
      
      val parser = new BasicParser();

      val inputOption = new Option("i", "input-dir", true, "input twenty news root dir parent of both 20news-bydate-test and 20news-bydate-train")
      inputOption.setRequired(true)

      val outputQuadsOption = new Option("oq", "output-quads", true, "output nquads file .nq[.gz]")
      outputQuadsOption.setRequired(true)
      
      val outputGraphsOption = new Option("og", "output-graph-ids", true, "output graph ids text file")
      outputGraphsOption.setRequired(true)
      
      val overwriteOption = new Option("ow", "overwrite", false, "overwrite output file if exists")
      overwriteOption.setRequired(false)
      
      val options = new Options()
        .addOption(inputOption)
        .addOption(outputQuadsOption)
        .addOption(outputGraphsOption)
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
      val outputQuadsPath = new Path(cmd.getOptionValue(outputQuadsOption.getOpt))
      val outputGraphsPath = new Path(cmd.getOptionValue(outputGraphsOption.getOpt))
      val overwrite = cmd.hasOption(overwriteOption.getOpt)
      
      if(!( outputQuadsPath.getName.endsWith(".nq") || outputQuadsPath.getName.endsWith(".nq.gz"))) {
        error("Output quads path maust end with .nq[.gz]")
      }
      
      if(!( outputGraphsPath.getName.endsWith(".txt") || outputGraphsPath.getName.endsWith(".txt.gz"))) {
        error("Output graphids path maust end with .txt[.gz]")
      }
      
      println("Input path:  " + inputPath.toString())
      println("Output quads path:  " + outputQuadsPath.toString())
      println("Output graphs path: " + outputGraphsPath.toString())
      println("Overwrite ? " + overwrite)
      
      val quadsGzip = outputQuadsPath.toString().endsWith(".gz")
      val graphsGzip = outputGraphsPath.toString().endsWith(".gz")
      
      val inputFS = FileSystem.get(inputPath.toUri(), new Configuration())
      val outpuQuadstFS = FileSystem.get(outputQuadsPath.toUri(), new Configuration())
      val outpuGraphsFS = FileSystem.get(outputGraphsPath.toUri(), new Configuration())
      
      if( ! inputFS.isDirectory(inputPath) ) {
        error("Input path is not a directory: " + inputPath)
      }
      
      if(outpuQuadstFS.exists(outputQuadsPath)) {
        if(!overwrite) {
          error("Output quads path already exists, use --overwrite option - " + outputQuadsPath)
        } else {
          if(!outpuQuadstFS.isFile(outputQuadsPath)) {
            error("Output quads path exists but is not a file: " + outputQuadsPath)
          } 
        }
      }
      
      if(outpuGraphsFS.exists(outputGraphsPath)) {
        if(!overwrite) {
          error("Output graphs ids path already exists, use --overwrite option - " + outputGraphsPath)
        } else {
          if(!outpuGraphsFS.isFile(outputGraphsPath)) {
            error("Output graph ids path exists but is not a file: " + outputGraphsPath)
          }
        }
      }
      
      var files = MutableList[Path]()
      
      println("Listing files...")
      val inputPaths = collectFiles(inputFS, inputPath, files)
      
      println("Input files count: " + files.size)
      
      
      var fos : OutputStream = outpuQuadstFS.create(outputQuadsPath, overwrite)
      if(quadsGzip) {
        fos = new GzipOutputStream(fos)
      }
      
      var gfos : OutputStream = outpuGraphsFS.create(outputGraphsPath, overwrite)
      if(graphsGzip) {
        gfos = new GzipOutputStream(gfos)
      }
      
      val gfosWriter = new BufferedWriter(new OutputStreamWriter(gfos, "UTF-8"))
      
      val nquadsWriter = new NQuadsWriter(fos)
      nquadsWriter.startRDF()
      
      var first = true
      
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
        val subjectURI = vf.createURI(s)
        val context = subjectURI
        
        nquadsWriter.handleStatement(vf.createStatement(subjectURI, RDF.TYPE, MESSAGE, context))
        nquadsWriter.handleStatement(vf.createStatement(subjectURI, hasNewsgroup, vf.createLiteral(newsgroup), context))
        nquadsWriter.handleStatement(vf.createStatement(subjectURI, hasSubject, vf.createLiteral(subject), context))
        nquadsWriter.handleStatement(vf.createStatement(subjectURI, hasBody, vf.createLiteral(body), context))

        if(first) {
          first = false
        } else {
          gfosWriter.write("\n")
        }
        
        gfosWriter.write(s)
        
      }
      
      nquadsWriter.endRDF()
      fos.close()
      gfosWriter.close()
      
      println("DONE")
      
      
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