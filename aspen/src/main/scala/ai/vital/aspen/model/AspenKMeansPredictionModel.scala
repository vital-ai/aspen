package ai.vital.aspen.model

import java.io.InputStream
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.commons.io.IOUtils
import java.io.File
import java.io.FileOutputStream
import java.nio.charset.StandardCharsets
import org.apache.commons.io.FileUtils
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.predictmodel.Prediction
import java.io.Serializable
import ai.vital.predictmodel.NumericalFeature
import org.apache.spark.mllib.clustering.VectorWithNorm
import org.apache.spark.mllib.clustering.KMeans

import com.github.fommil.netlib.{BLAS => NetlibBLAS, F2jBLAS}


object AspenKMeansPredictionModel {
  
	val spark_kmeans_prediction = "spark-kmeans-prediction";

  //from MLUtils
  lazy val EPSILON = {
    var eps = 1.0
    while ((1.0 + (eps / 2.0)) != 1.0) {
      eps /= 2.0
    }
    eps
  }
  
}

@SerialVersionUID(1L)
class AspenKMeansPredictionModel extends PredictionModel {

  var clustersCount = 10
  
  var numIterations = 20

  var model : KMeansModel = null;

  def setModel(_model: KMeansModel) : Unit = {
    model = _model
  }
  
  def supportedType(): String = {
    return AspenKMeansPredictionModel.spark_kmeans_prediction
  }

  def deserializeModel(stream: InputStream): Unit = {
    
      val deserializedModel : KMeansModel = SerializationUtils.deserialize(IOUtils.toByteArray(stream))
    
      model = deserializedModel match {
        case x: KMeansModel => x
        case _ => throw new ClassCastException
      }
      
  }

  def doPredict(v: Vector): Double = {
    throw new RuntimeException("shouldn't be called!")
    //return model.predict(v).intValue()
  }
  
  @Override
  override def _predict(vitalBlock : VitalBlock, featuresMap : java.util.Map[String, Object]) : Prediction = {
    
    val point = vectorizeNoLabels(vitalBlock, featuresMap)
    
    val clusterID = model.predict(point).intValue()
    
    val clusterCentersWithNorm = model.clusterCenters.map(new VectorWithNorm(_))
    
    val squaredDistance = pointCost(clusterCentersWithNorm, new VectorWithNorm(point))
    
    var cp = new ClusterPrediction()
    cp.clusterID = clusterID
    cp.squaredDistance = squaredDistance 
    return cp
    
  }  
  
  
  
  override def isClustering() : Boolean = {
    true
  }
 
  def getClustersCount() : Int = {
    model.k
  }
  
  @Override
  def persistFiles(tempDir : File) : Unit = {

    val os = new FileOutputStream(new File(tempDir, model_bin))
    SerializationUtils.serialize(model, os)
    os.close()
    
    if(error != null) {
      FileUtils.writeStringToFile(new File(tempDir, error_txt), error, StandardCharsets.UTF_8.name())
    }
  }
  
  @Override
  def isTestedWithTrainData() : Boolean = {
    return true;
  }
  
  @Override
  def onAlgorithmConfigParam(param: String, value: Serializable): Boolean = {
    
     if("clustersCount".equals(param)) {
      
      if(!value.isInstanceOf[Number]) ex(param + " must be an int/long number")
      
      clustersCount = value.asInstanceOf[Number].intValue()
      
      if(clustersCount < 2) ex(param + " must be >= 2")
      
    } else if("numIterations".equals(param)){
      
      if(!value.isInstanceOf[Number]) ex(param + " must be an int/long number")
      
      numIterations = value.asInstanceOf[Number].intValue()
      
      if(numIterations < 1) ex(param + " must be >= 1") 
      
    } else {
      
      return false
      
    }
    
    return true
  }

  def getTrainFeatureType(): Class[_ <: ai.vital.predictmodel.Feature] = {
    classOf[NumericalFeature]
  }
 
  class VectorWithNorm(val vector: Vector, val norm: Double) extends Serializable {

    def this(vector: Vector) = this(vector, Vectors.norm(vector, 2.0))

    def this(array: Array[Double]) = this(Vectors.dense(array))

    /** Converts the vector to a dense vector. */
    def toDense: VectorWithNorm = new VectorWithNorm(Vectors.dense(vector.toArray), norm)
  }
 
  //from KMeans class
  def pointCost(
      centers: TraversableOnce[VectorWithNorm],
      point: VectorWithNorm): Double =
    findClosest(centers, point)._2
  
  //from KMeans class
  def findClosest(
      centers: TraversableOnce[VectorWithNorm],
      point: VectorWithNorm): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    centers.foreach { center =>
      // Since `\|a - b\| \geq |\|a\| - \|b\||`, we can use this lower bound to avoid unnecessary
      // distance computation.
      var lowerBoundOfSqDist = center.norm - point.norm
      lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
      if (lowerBoundOfSqDist < bestDistance) {
        val distance: Double = fastSquaredDistance(center, point)
        if (distance < bestDistance) {
          bestDistance = distance
          bestIndex = i
        }
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }
  
  //from KMeans
  def fastSquaredDistance(
      v1: VectorWithNorm,
      v2: VectorWithNorm): Double = {
      fastSquaredDistance2(v1.vector, v1.norm, v2.vector, v2.norm)
  }
  
  //from MLUtils
  val EPSILON = AspenKMeansPredictionModel.EPSILON
  
  //from MLUtils
  /**
   * Returns the squared Euclidean distance between two vectors. The following formula will be used
   * if it does not introduce too much numerical error:
   * <pre>
   *   \|a - b\|_2^2 = \|a\|_2^2 + \|b\|_2^2 - 2 a^T b.
   * </pre>
   * When both vector norms are given, this is faster than computing the squared distance directly,
   * especially when one of the vectors is a sparse vector.
   *
   * @param v1 the first vector
   * @param norm1 the norm of the first vector, non-negative
   * @param v2 the second vector
   * @param norm2 the norm of the second vector, non-negative
   * @param precision desired relative precision for the squared distance
   * @return squared distance between v1 and v2 within the specified precision
   */
  def fastSquaredDistance2(
      v1: Vector,
      norm1: Double,
      v2: Vector,
      norm2: Double,
      precision: Double = 1e-6): Double = {
    val n = v1.size
    require(v2.size == n)
    require(norm1 >= 0.0 && norm2 >= 0.0)
    val sumSquaredNorm = norm1 * norm1 + norm2 * norm2
    val normDiff = norm1 - norm2
    var sqDist = 0.0
    /*
     * The relative error is
     * <pre>
     * EPSILON * ( \|a\|_2^2 + \|b\\_2^2 + 2 |a^T b|) / ( \|a - b\|_2^2 ),
     * </pre>
     * which is bounded by
     * <pre>
     * 2.0 * EPSILON * ( \|a\|_2^2 + \|b\|_2^2 ) / ( (\|a\|_2 - \|b\|_2)^2 ).
     * </pre>
     * The bound doesn't need the inner product, so we can use it as a sufficient condition to
     * check quickly whether the inner product approach is accurate.
     */
    val precisionBound1 = 2.0 * EPSILON * sumSquaredNorm / (normDiff * normDiff + EPSILON)
    if (precisionBound1 < precision) {
      sqDist = sumSquaredNorm - 2.0 * dot(v1, v2)
    } else if (v1.isInstanceOf[SparseVector] || v2.isInstanceOf[SparseVector]) {
      val dotValue = dot(v1, v2)
      sqDist = math.max(sumSquaredNorm - 2.0 * dotValue, 0.0)
      val precisionBound2 = EPSILON * (sumSquaredNorm + 2.0 * math.abs(dotValue)) /
        (sqDist + EPSILON)
      if (precisionBound2 > precision) {
        sqDist = Vectors.sqdist(v1, v2)
      }
    } else {
      sqDist = Vectors.sqdist(v1, v2)
    }
    sqDist
  }
  
  
  //from org/apache/spark/mllib/linalg/BLAS.scala
    /**
   * dot(x, y)
   */
  def dot(x: Vector, y: Vector): Double = {
    require(x.size == y.size,
      "BLAS.dot(x: Vector, y:Vector) was given Vectors with non-matching sizes:" +
      " x.size = " + x.size + ", y.size = " + y.size)
    (x, y) match {
      case (dx: DenseVector, dy: DenseVector) =>
        dot(dx, dy)
      case (sx: SparseVector, dy: DenseVector) =>
        dot(sx, dy)
      case (dx: DenseVector, sy: SparseVector) =>
        dot(sy, dx)
      case (sx: SparseVector, sy: SparseVector) =>
        dot(sx, sy)
      case _ =>
        throw new IllegalArgumentException(s"dot doesn't support (${x.getClass}, ${y.getClass}).")
    }
  }
  
   /**
   * dot(x, y)
   */
  private def dot(x: DenseVector, y: DenseVector): Double = {
    val n = x.size
    f2jBLAS.ddot(n, x.values, 1, y.values, 1)
  }

  /**
   * dot(x, y)
   */
  private def dot(x: SparseVector, y: DenseVector): Double = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val nnz = xIndices.size

    var sum = 0.0
    var k = 0
    while (k < nnz) {
      sum += xValues(k) * yValues(xIndices(k))
      k += 1
    }
    sum
  }

  /**
   * dot(x, y)
   */
  private def dot(x: SparseVector, y: SparseVector): Double = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val yIndices = y.indices
    val nnzx = xIndices.size
    val nnzy = yIndices.size

    var kx = 0
    var ky = 0
    var sum = 0.0
    // y catching x
    while (kx < nnzx && ky < nnzy) {
      val ix = xIndices(kx)
      while (ky < nnzy && yIndices(ky) < ix) {
        ky += 1
      }
      if (ky < nnzy && yIndices(ky) == ix) {
        sum += xValues(kx) * yValues(ky)
        ky += 1
      }
      kx += 1
    }
    sum
  }
  
  @transient private var _f2jBLAS: NetlibBLAS = _
  @transient private var _nativeBLAS: NetlibBLAS = _
  
    // For level-1 routines, we use Java implementation.
  private def f2jBLAS: NetlibBLAS = {
    if (_f2jBLAS == null) {
      _f2jBLAS = new F2jBLAS
    }
    _f2jBLAS
  }
}