package org.apache.spark.mllib

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.hadoop.fs.Path
import org.apache.commons.lang.SerializationUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * As MatrixFactorizationModel is not java-serializable we need a wrapper that persists both RDDs and rank
 */
object MatrixFactorizationModelWrapper {

  def deserializeModel(sc: SparkContext, outputDir: Path) : MatrixFactorizationModel = {
    
		val r = new Path(outputDir, "rank.java.object")
      
    val uf = new Path(outputDir, "userFeatures.rdd.object")
    
    val pf = new Path(outputDir, "productFeatures.rdd.object")
    
    
		val ris = FileSystem.get(outputDir.toUri(), new Configuration()).open(r)
		val rank = ris.readInt()
    
    val ufRDD = sc.objectFile(uf.toString()).asInstanceOf[RDD[(Int, Array[Double])]]
    val pfRDD = sc.objectFile(pf.toString()).asInstanceOf[RDD[(Int, Array[Double])]]
    
    val mfm = new MatrixFactorizationModel(rank, ufRDD, pfRDD)
    
    return mfm
  }
  
  def serializeModel(model: MatrixFactorizationModel, outputDir: Path) : Unit = {
    
		val r = new Path(outputDir, "rank.java.object")
      
		val uf = new Path(outputDir, "userFeatures.rdd.object")
    
		val pf = new Path(outputDir, "productFeatures.rdd.object")
    
		val ros = FileSystem.get(outputDir.toUri(), new Configuration()).create(r, true)
		ros.writeInt(model.rank)
		model.userFeatures.saveAsObjectFile(uf.toString())
    model.productFeatures.saveAsObjectFile(pf.toString())
    
    
    
  }
}