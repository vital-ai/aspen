package ai.vital.aspen.analysis.regression

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayOps._
import scala.Predef
import java.util.Arrays
import org.apache.spark.mllib.feature.StandardScaler
import scala.collection.mutable.MutableList

object LinearRegressionTest {

  def main( args: Array[String] ) {
    
    val sc = new SparkContext("local", "app")

    // Load and parse the data
    val data = sc.textFile("./regression/auto-mpg.data-clean")
//    val data = sc.textFile("file:/D:/lib/spark/spark-1.3.1-bin-hadoop2.6/data/mllib/ridge-data/lpsa.data")
    var parsedData = data.map { line =>
//        val parts = line.split(',')
//        LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
        
      val parts = line.split('\t')
      val cols = parts(0).split("\\s+").map(_.toDouble)
      LabeledPoint(cols(0), Vectors.dense( Arrays.copyOfRange(cols, 1, cols.length) ) )
      
    }.cache()
    
    
    val vectors = parsedData.map { lp =>
      var vals = MutableList[Double]()
      vals += lp.label
      for( v <- lp.features.toArray ) {
        vals += v
      }
      Vectors.dense(vals.toArray)
    }
    
    
    
    val scaler2 = new StandardScaler(true, true).fit(vectors);
    
    val labelsVals = vectors.map { x => x(0) }
    
    val minV = labelsVals.min()  //x1
    
    val maxV = labelsVals.max() //x2
    
    val minVVectorA = MutableList[Double]()
    minVVectorA+=(minV)
    var c = 1
    while(c < scaler2.mean.size) {
      minVVectorA+=0d
      c = c + 1
    }

    val maxVVectorA = MutableList[Double]()
    maxVVectorA+=(maxV)
    c = 1
    while(c < scaler2.mean.size) {
      maxVVectorA+=0d
      c = c+1
    }
    
    val minVS = scaler2.transform(Vectors.dense(minVVectorA.toArray))(0) //y1
    val maxVS = scaler2.transform(Vectors.dense(maxVVectorA.toArray))(0) //y2
    
    //solve linear equation 
    val a = (maxVS - minVS) / (maxV-minV)
    val b1 = minVS - a * minV
    val b2 = maxVS - a * maxV
    
    println ("MIN: " + minV + " -> " + minVS + "    MAX: " + maxV + " -> " + maxVS)
    
    println ("solved, a: "+ a + " b1: " + b1 + " b2: " + b2)
    
    
    vectors.map { x =>
        println("" + x(0) + ", " + scaler2.transform(x)(0))
    }.count()
    
    println ( 6 + ", " + scaler2.transform(Vectors.dense(6)))
    
    //reverse enginering to get the input
    
    
    if(true) return;
    
    //scale
    val scaler = new StandardScaler(true, true).fit(vectors)

    parsedData = vectors.map { x =>
      val y = scaler.transform(x)
      LabeledPoint(y(0), Vectors.dense( Arrays.copyOfRange(y.toArray, 1, y.size) ) )
    }
    
    for( x <- parsedData.collect() ) {
      println (x)
    }
    
    // Building the model
    val numIterations = 100
    val model = LinearRegressionWithSGD.train(parsedData, numIterations)
    
    // Evaluate model on training examples and compute training error
    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      println("point: " + point.label + " prediction:" + prediction)
      (point.label, prediction)
    }
    val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
    println("training Mean Squared Error = " + MSE)
    
    // Save and load model
//    model.save(sc, "myModelPath")
//    val sameModel = LinearRegressionModel.load(sc, "myModelPath")
        
  }
  
}