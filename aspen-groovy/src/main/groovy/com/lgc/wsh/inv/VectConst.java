package com.lgc.wsh.inv;

/** Vector operations that do not change the state of the vector */
public interface VectConst extends Cloneable, java.io.Serializable {
  /** Return the Cartesian dot product of this vector with another
      vector (not including any inverse covariance).
      [Feel free to normalize by the number of elements in the array,
      if the inverse convariance is defined consistently.]
      @param other The vector to be dotted.
      @return The dot product.
  */
  public double dot(VectConst other);

  /** This is the dot product of the vector with
      itself premultiplied by the inverse covariance.
      If the inverse covariance is an identity, then
      the result is just the dot product with itself.
      Equivalently,
      <pre>
      Vect vect = (Vect) this.clone();
      vect.multiplyInverseCovariance();
      return this.dot(vect);
      </pre>
      But you can usually avoid the clone.
 * @return magnitude of vector.
  */
  public double magnitude() ;

  // You can clone a mutable version of a VectConst
  public Vect clone();
}

