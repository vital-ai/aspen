package com.lgc.wsh.inv;

import com.lgc.wsh.util.Almost;
import java.util.logging.*;
import java.io.*;

/** Implements convenience methods for Vect. */
public class VectUtil {
  private static final Logger LOG
    = Logger.getLogger(VectUtil.class.getName());

  /** Scale a vector by a scalar constant.
      @param v Vector to scale.
      @param scalar Factor to scale the vector.
  */
  public static void scale(Vect v, double scalar) {
    v.add(scalar, 0., v);
  }

  /** Set the magnitude of this vector to zero, so that this.dot(this) == 0.
      @param v Vector to zero
   */
  public static void zero(Vect v) {
    scale(v, 0.);
  }

  /** Copy the state of one vector onto another.
      @param to Vector whose state should be initialized
      with the state of from.
      @param from Vector whose state should be copied.
   */
  public static void copy(Vect to, VectConst from) {
    to.add(0., 1., from);
  }

  /** Clone a vector and initialized to zero, so that
      out.dot(out) == 0.
      @param v Vect to clone
      @return A cloned copy of the vector set to zero magnitude.
  */
  public static Vect cloneZero(VectConst v) {
    Vect result = v.clone();
    zero(result);
    return result;
  }

  static final Almost ALMOST_DOT = new Almost(0.000015);

  /** See if two vectors are the same.  Useful for test code.
      @param v1 First vector
      @param v2 Second vector
      @return true if vectors appear to be the same, within
      floating precision.
   */
  public static boolean areSame(VectConst v1, VectConst v2) {
    double aa = v1.dot(v1);
    double ab = v1.dot(v2);
    double bb = v2.dot(v2);
    // LOG.info("aa="+aa+" ab="+ab+" bb="+bb);
    return
      ALMOST_DOT.equal(aa,bb) &&
      ALMOST_DOT.equal(aa,ab) &&
      ALMOST_DOT.equal(ab,bb) ;
  }

  /** Exercise all methods of Vect.
      @param vect An instance of a Vect to test.
      Should be initialized to random non-zero values.
      A vector of zero magnitude will fail.
  */
  public static void test(VectConst vect) {
    double originalDot = vect.dot(vect);
    ass (!Almost.FLOAT.zero(originalDot), "cannot test a zero vector");

    Vect t = VectUtil.cloneZero(vect);
    ass (Almost.FLOAT.zero(t.dot(t)), "cloneZero() did not work");

    VectUtil.copy(t, vect);
    double check = t.dot(vect)/vect.dot(vect);
    ass(Almost.FLOAT.equal(check, 1.), "not 1. check="+check);

    VectUtil.scale(t, 0.5);
    check = t.dot(vect)/vect.dot(vect);
    ass(Almost.FLOAT.equal(check, 0.5), "not 0.5 check="+check);

    t.add(1., 1.,vect);
    check = t.dot(vect)/vect.dot(vect);
    ass(Almost.FLOAT.equal(check, 1.5), "not 1.5 check="+check);

    // t.add(1., -3.5, vect);
    t.add(2., -5., vect);
    check = t.dot(vect)/vect.dot(vect);
    ass(Almost.FLOAT.equal(check, -2.), "not -2, check="+check);

    t.project(0., 1., vect);
    t.project(1.75, -0.75, vect);
    ass (VectUtil.areSame(t, vect), "project failed");

    t.dispose();
    ass (Almost.FLOAT.equal(originalDot, vect.dot(vect)),
         "exercise of clone damaged original");

    t = vect.clone();
    t.multiplyInverseCovariance();
    double mag1 = vect.dot(t);
    t.dispose();
    double mag2 = vect.magnitude();
    ass (Almost.FLOAT.equal(mag1, mag2),
         "magnitude() inconsistent with "
         +"multiplyInverseCovariance() and dot(): "+
         mag1+"!="+mag2);
    ass(mag1 > 0, "inverse covariance gave zero magnitude");
    ass(mag2 > 0, "magnitude was zero when dot product was not zero");

    // simple test of constrain
    t = vect.clone();
    t.constrain();
    double mag3 = t.magnitude();
    ass(mag3 > 0, "constrain() gave zero magnitude");
    t.dispose();

    // make sure postCondition can be called
    t = vect.clone();
    t.postCondition();
    t.dispose();

    // some will override toString method
    String vs = vect.toString();
    assert vs != null && vs.length() > 0;

    // test serialization
    byte[] data = null;
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      t = vect.clone();
      oos.writeObject(t);
      oos.flush();
      oos.close();
      t.dispose();
      t = null;
      data = baos.toByteArray();
    } catch (IOException e) {
      e.printStackTrace(new com.lgc.wsh.util.LoggerStream(LOG, Level.SEVERE));
      ass(false, "writing serialization failed "+e.getMessage());
    }
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(data);
      ObjectInputStream ois = new ObjectInputStream(bais);
      t = (Vect) (ois.readObject());
      ass (VectUtil.areSame(t, vect),
           "Serialization did not preserve Vect "
           +t.dot(t)+"=="+t.dot(vect)+"=="+vect.dot(vect));
      // check these are not sharing anything
      VectUtil.scale(t,0.5);
      double tt = t.dot(t);
      double tv = t.dot(vect);
      double vv = vect.dot(vect);
      ass (tt>0, "Scaling set serialized vect to zero magnitude");
      ass (Almost.FLOAT.equal(tt*2, tv),
           "Serialized vector does not have independent magnitude tt="+tt+
           " tv="+tv);
      ass (Almost.FLOAT.equal(tv*2, vv),
           "serialized vector does not have independent magnitude tv="+tv+
           " vv="+vv);
      t.dispose();
    } catch (IOException e) {
      e.printStackTrace(new com.lgc.wsh.util.LoggerStream(LOG, Level.SEVERE));
      ass(false, "reading serialization failed "+e.getMessage());
    } catch (ClassNotFoundException e) {
      e.printStackTrace(new com.lgc.wsh.util.LoggerStream(LOG, Level.SEVERE));
      ass (false, "Can't find class just written "+e.getMessage());
    }

  }

  /** Return the number of significant digits in the dot product
      when calculated with and without the transpose.
      @param data Nonzero sample data
      @param model A nonzero sample model.
      @param transform The transform to test.
 * @return number of digits in precision.
   */
  public static int getTransposePrecision(VectConst data, VectConst model,
                                          Transform transform) {
    int precision = 200;
    boolean dampOnlyPerturbation = true; // results in a bigger b
    VectUtil.test(data);
    VectUtil.test(model);
    TransformQuadratic tq = new TransformQuadratic
      (data, model, null, transform, dampOnlyPerturbation);
    precision = Math.min(precision,tq.getTransposePrecision());
    return precision;
  }

  private static void ass(boolean condition, String requirement) {
    if (!condition) throw new IllegalStateException(requirement);
  }

}

