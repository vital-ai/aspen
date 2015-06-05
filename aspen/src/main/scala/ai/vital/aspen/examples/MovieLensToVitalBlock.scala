package ai.vital.aspen.examples

import org.apache.commons.cli.BasicParser
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.ParseException
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import ai.vital.vitalsigns.block.BlockCompactStringSerializer
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.Text
import ai.vital.hadoop.writable.VitalBytesWritable
import java.io.OutputStream
import org.apache.hadoop.io.compress.GzipCodec.GzipOutputStream
import java.io.OutputStreamWriter
import java.util.Random
import java.io.InputStreamReader
import java.io.BufferedReader
import java.nio.charset.StandardCharsets
import java.util.HashMap
import org.movielens.domain.Edge_hasMovieRating
import org.movielens.domain.Movie
import org.movielens.domain.User
import java.util.ArrayList
import ai.vital.vitalsigns.VitalSigns
import ai.vital.vitalsigns.model.GraphObject
import scala.collection.JavaConversions._
import java.util.Arrays
import java.util.LinkedHashMap
import ai.vital.vitalsigns.model.property.IProperty
import java.util.Collection

class MovieLensToVitalBlock {
  
}
object MovieLensToVitalBlock {
  
  val NS = "http://vital.ai/movielens/"
  val MOVIE_NS = NS + "movie/"
  val USER_NS = NS + "user/"
  val RATING_NS = NS + "rating/"
  val EDGE_HAS_RATING_NS = NS + "edge_has_rating/"
  val EDGE_HAS_DIRECT_RATING_NS = NS + "edge_has_direct_rating/"
  val EDGE_HAS_RATED_MOVIE_NS = NS + "edge_has_rated_movie/"
  
  val mode_movies = "movies"
  
  val mode_users = "users"
  
  val mode_predict_all = "predict-all"
  
  val mode_predict_direct_edges = "predict-direct-edges"
  
  val modesList = Arrays.asList(mode_movies, mode_users,/* mode_predict_all,*/ mode_predict_direct_edges)
  
  def main(args: Array[String]) : Unit = {
    
    val parser = new BasicParser();

    val modeOption = new Option("m", "mode", true, "mode, one of " + modesList)
    modeOption.setRequired(true)
    
    val inputOption = new Option("i", "input-dir", true, "input movielens directory, must contain movies.dat and ratings.dat")
    inputOption.setRequired(true)
    
    val outputOption = new Option("o", "output-block", true, "output vital block file .vital[.gz] or .vital.seq")
    outputOption.setRequired(true)
      
    val overwriteOption = new Option("ow", "overwrite", false, "overwrite output file if exists")
    overwriteOption.setRequired(false)
      
    val percentOption = new Option("p", "percent", true, "optional output objects percent limit")
    percentOption.setRequired(false)
      
    val options = new Options()
      .addOption(modeOption)
      .addOption(inputOption)
      .addOption(outputOption)
      .addOption(overwriteOption)
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
    
    val mode = cmd.getOptionValue(modeOption.getOpt)
    
    val inputPath = new Path(cmd.getOptionValue(inputOption.getOpt))
    val outputBlockPath = new Path(cmd.getOptionValue(outputOption.getOpt))
    val overwrite = cmd.hasOption(overwriteOption.getOpt)
      
    val percentValue = cmd.getOptionValue(percentOption.getOpt)
    var percent = 100D
    if(percentValue != null && !percentValue.isEmpty()) {
       percent = java.lang.Double.parseDouble(percentValue)
    }
      
    println("Mode: " + mode)
    
    if(!modesList.contains(mode)) error("Unknown mode: " + mode)
    
    println("Input path:  " + inputPath.toString())
    println("Output block path:  " + outputBlockPath.toString())
    println("Overwrite ? " + overwrite)
    println("Output docs percent: " + percent)
     
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
      
    val hConf = new Configuration()
      
    val inputFS = FileSystem.get(inputPath.toUri(), hConf)
    val outpuBlockFS = FileSystem.get(outputBlockPath.toUri(), hConf)
      
    if( ! inputFS.isDirectory(inputPath) ) {
      error("Input path is not a directory: " + inputPath)
    }
     
    val moviesPath = new Path(inputPath, "movies.dat");
    if( ! inputFS.isFile( moviesPath) ) {
      error("Input movies path is not a file: " + moviesPath)
    }
      
    val ratingsPath = new Path(inputPath, "ratings.dat")
    if( ! inputFS.isFile( ratingsPath )) {
      error("Input ratings path is not a file: " + ratingsPath);
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
      
      
      
    
    val movieID2Movie = new LinkedHashMap[Int, Movie]()
    val userID2User = new LinkedHashMap[Int, User]()
    
    var l : String = null;
    
    if( ! mode.equals(mode_users)) {
      
    	println("Collecting movies list ...")
      
    	val moviesReader = new BufferedReader(new InputStreamReader(inputFS.open(moviesPath), StandardCharsets.UTF_8))
    	
    	l = moviesReader.readLine()
    	while ( l != null ) {
    		l = l.trim
    				if(!l.isEmpty) {
    					//MovieID::Title::Genres
    					val record = l.split("::");
    					val movieID = Integer.parseInt(record(0))
    							val title = record(1)
//        val genres = record(2)
    							val movie = new Movie()
    					movie.setURI(MOVIE_NS + movieID)
    					movie.setProperty("name", title)
    					if(movieID2Movie.containsKey(movieID)) error("Duplicate movie: " + movieID)
    					movieID2Movie.put(movieID, movie)
    				}
    		
    		l = moviesReader.readLine()
    				
    	}
    	
    	moviesReader.close()
    	
    	println("Movies count: " + movieID2Movie.size())
      
    } else {
      println("skipping movies collection")
    }
    
      

    var blockWriter : BlockCompactStringSerializer = null
      
    var seqWriter : SequenceFile.Writer = null;
      
    var processed = 0;
    var skipped = 0;
      
    if( seqOut ) {
        
//      val codec = new GzipCodec()
        
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
    
    if(mode.equals(mode_movies)) {
      
      for(e <- movieID2Movie.entrySet()) {
        
        val movie = e.getValue
        
        val outputObjs = new ArrayList[GraphObject]()
        outputObjs.add(movie)

        if(blockWriter != null) {
                  
          blockWriter.startBlock()
          for(g <- outputObjs) {
            blockWriter.writeGraphObject(g)
          }
          blockWriter.endBlock()
                  
        }
        
        if(seqWriter != null) {
                  
          val block = VitalSigns.get().encodeBlock(outputObjs)
                      
          seqKey.set(movie.getURI)
          seqValue.set(block)
                      
          seqWriter.append(seqKey, seqValue)
                      
        }
                
        processed = processed + 1
      }
      
    } else {
      
    	val ratingsReader = new BufferedReader(new InputStreamReader(inputFS.open(ratingsPath), StandardCharsets.UTF_8.name()))
    	
    	l = ratingsReader.readLine()
    	
      val lastUserID : Integer = null
      
    	while( l != null) {
    		
    		l = l.trim
    				
    				if(!l.isEmpty()) {
    					
    					var accept = true
    							
    							if(percent < 100D) {
    								
    								if(random.nextDouble() * 100D > percent) {
    									accept = false
    											skipped = skipped + 1
    								}
    								
    							}
    					
    					if(accept) {
    						
    						//UserID::MovieID::Rating::Timestamp
    						val record = l.split("::");
    						
    						val userID = Integer.parseInt(record(0))
                val movieID = Integer.parseInt(record(1))
    						val rating = java.lang.Double.parseDouble(record(2))
    						val timestampSeconds = java.lang.Long.parseLong(record(3))
                
                var user = userID2User.get(userID)
                if(user == null) {
                  user = new User()
                  user.setURI(USER_NS + userID)
                  user.setProperty("name", "User " + userID)
                  userID2User.put(userID, user)
                }
                
                
                if(mode.equals(mode_users)) {
                  
                	var ratedMovieURIs = user.getProperty("ratedMovieURIs")
                			
                  var newRatedMovieURIs = new ArrayList[String]();
                	
                	if(ratedMovieURIs != null) {
                		val current = ratedMovieURIs.asInstanceOf[IProperty].rawValue().asInstanceOf[Collection[Any]]
                				for(x<-current) {
                					newRatedMovieURIs.add(x.asInstanceOf[String]) 
                				}
                	}
                  
                  newRatedMovieURIs.add(MOVIE_NS + movieID)
                	
                	user.setProperty("ratedMovieURIs", newRatedMovieURIs);

                  //continue
                  
                } else {
                  
                    var keyURI : String = null
                                  
                    val movie = movieID2Movie.get(movieID)
                  
                    if(movie == null) error("Movie not found: " + movieID)
                    
                    
                    val outputObjs = new ArrayList[GraphObject]()
                    
                    if(mode.equals(mode_predict_all)) {
                      
//                      
//                    	val ratingNode = new Rating()
//                    	ratingNode.setURI(RATING_NS + userID + "_" + movieID)
//                    	ratingNode.setProperty("rating", rating)
//                    	
//                    	val edgeHasRating = new Edge_hasRating()
//                    	edgeHasRating.setURI(EDGE_HAS_RATING_NS + userID + "_" + movieID)
//                    	edgeHasRating.addSource(user).addDestination(ratingNode)
//                    	
//                    	val edgeHasRatedMovie = new Edge_hasRatedMovie()
//                    	edgeHasRatedMovie.setURI(EDGE_HAS_RATED_MOVIE_NS + userID + "_" + movieID)
//                    	edgeHasRatedMovie.addSource(ratingNode).addDestination(movie)
//                    	
//                    	
//                    	
//                    	outputObjs.add(ratingNode)
//                    	outputObjs.add(user)
//                    	outputObjs.add(movie)
//                    	outputObjs.add(edgeHasRating)
//                    	outputObjs.add(edgeHasRatedMovie)
//                      
//                      keyURI = ratingNode.getURI()
                      
                    } else {
                      
                      val directEdge = new Edge_hasMovieRating()
                      directEdge.setURI(EDGE_HAS_DIRECT_RATING_NS + userID + "_" + movieID)
                      directEdge.addSource(user).addDestination(movie)
                      directEdge.setProperty("rating", rating);
                      directEdge.setProperty("timestamp", new java.lang.Long(timestampSeconds * 1000L).longValue());
                      
                      outputObjs.add(directEdge)
                      
                      keyURI = directEdge.getURI()
                      
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
                          
                          seqKey.set(keyURI)
                          seqValue.set(block)
                          
                          seqWriter.append(seqKey, seqValue)
                          
                    }
                  
                }
    								
    								


    						
    						processed = processed + 1
    								
    					}
    					
    				}
    		
    		l = ratingsReader.readLine()
    				
    	}
      
      
      if(mode.equals(mode_users)) {
        
        
        for( e <- userID2User.entrySet() ) {
          
          if(blockWriter != null) {
            blockWriter.startBlock()
            blockWriter.writeGraphObject(e.getValue)
            blockWriter.endBlock()
                      
          } 
                    
          if(seqWriter != null) {
                      
            val block = VitalSigns.get().encodeBlock(Arrays.asList(e.getValue))
            seqKey.set(e.getValue.getURI)
            seqValue.set(block)
                          
            seqWriter.append(seqKey, seqValue)
                          
          }
        
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

}