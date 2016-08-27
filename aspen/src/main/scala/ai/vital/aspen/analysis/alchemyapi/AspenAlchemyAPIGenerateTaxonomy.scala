package ai.vital.aspen.analysis.alchemyapi

import java.io.File
import groovy.json.JsonSlurper
import scala.collection.JavaConversions._
import java.util.Map
import java.text.Normalizer
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringEscapeUtils
import ai.vital.vitalsigns.model.VITAL_Category
import java.io.InputStreamReader
import java.io.FileInputStream
import java.util.ArrayList
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.io.FileOutputStream
import java.util.Collections
import ai.vital.aspen.groovy.taxonomy.HierarchicalCategories
import au.com.bytecode.opencsv.CSVReader

object AspenAlchemyAPIGenerateTaxonomy {

  def main(args: Array[String]) : Unit = {
    
    if(args.length != 2) {
      System.err.println("usage:  <input_taxonomy_csv> <output_vital_taxonomy_file>");
      return
    }
    
    //<slugified_label_without_slashes_and_spaces>
    val URI_BASE = "http://vital.ai/vital.ai/app/VITAL_Category/alchemyapi_"
    
    val inputFile = new File(args(0))
    
    val outputFile = new File(args(1))
    
    println("Input classes csv: " + inputFile.getAbsolutePath)
    println("Output taxonomy file: " + outputFile.getAbsolutePath)
    
    println("Generating taxonomy");

    val lines = new ArrayList[String]()
    
    val reader2 = new CSVReader(new InputStreamReader(new FileInputStream(inputFile), "UTF-8"))
    
//    val reader = new CSVReader(new InputStreamReader(new FileInputStream(inputFile)))
    
    var  line : Array[String] = null
    
    var break = false
    while( !break ) {
      
      line = reader2.readNext()
      
      if(line != null) {
        
        if( line.length != 5 ) throw new Exception("Expected 5 columns")
        
        //concatenate label
        
        var label = ""
        
        var i = 0
        
        while( i < line.length) {
          val v = line(i).trim()
          i = i + 1
          
          if(!v.isEmpty()) label = label + "/" + v
          
          
        }
        
        lines.add(label)
      
      } else {
        break = true
      }
      
    }
    
    reader2.close()
    
    println("Labels count: " + lines.size());
    
    //sort ?
    Collections.sort(lines);
    
    //fix missing categories
    var previousDepth : Int = -1
    
    var artificialCategories = 0
    
    val fixedLines = new ArrayList[String]()
    
    for(label <- lines) {
      
      val s = label.split("/")
      
      val depth = s.length
      
      if(previousDepth > 0 && previousDepth < depth) {
        
        //insert artificial categories to keep depth in order
        
        var diff = depth - previousDepth
        
        while(diff > 1) {
          
          var currentDepth = depth - diff +1
          
          var i = 0 
          
          var x = ""
          
          while( i < currentDepth ) {
            if(i > 0) x = x + "/"
            x = x + s(i) 
            i = i + 1 
          }
          
          fixedLines.add(x)
          
          diff = diff - 1
          
          artificialCategories = artificialCategories + 1
         
          
        }
        
      }
      
      fixedLines.add(label)
      
      previousDepth = depth
      
    }
    
    println("Fixed Labels count: " + fixedLines.size());
    
    var writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8"))

    writer.write("#alchemy lab taxonomy, generated from http://www.alchemyapi.com/sites/default/files/taxonomyCategories.zip\n\n")

    writer.write(s"""+ ${URI_BASE}ROOT "AlchemyAPI Taxonomy Root"\n\n""")
    
    for(label <- fixedLines) {
      
      val depth = label.split("/").length
      
      var p = ""
      
      while(p.length < depth) {
        p = p + "+"
      }
      
      val URI = URI_BASE + label.replace(" ", "_").replace("/", "___");

      if(depth == 2) {
        writer.write("\n")
      }
      
      writer.write(s"""$p $URI "$label"\n""")
      
    }
    
    writer.close()
    
    
    val hc = new HierarchicalCategories(outputFile);
    
    
    
  }
  
  def slugify(inputX : String) : String = {
    
    if (inputX == null) {
      return "";
    }

    var input = inputX.trim();

//    for (Entry<Object, Object> e : replacements.entrySet()) {
//      input = input.replace((String) e.getKey(), (String) e.getValue());
//    }

    input = Normalizer.normalize(input, Normalizer.Form.NFD)
        .replaceAll("[^\\p{ASCII}]", "")
        .replaceAll("[^\\w+]", "-")
        .replaceAll("\\s+", "-")
        .replaceAll("[-]+", "-")
        .replaceAll("^-", "")
        .replaceAll("-$", "");

//    if (getLowerCase()) {
      input = input.toLowerCase();
//    }

    return input;
    
  }
  
}