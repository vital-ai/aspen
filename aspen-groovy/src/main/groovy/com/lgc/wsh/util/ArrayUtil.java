package com.lgc.wsh.util;

import static java.lang.Math.abs;
import static java.lang.Math.sqrt;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;
import java.util.logging.Logger;

/** Static utilities for this package. */
public class ArrayUtil {
  private static final Logger LOG
    = Logger.getLogger(ArrayUtil.class.getName());

  // SecureRandom avoids identical sequences when started at the same time
  // Random performs faster.
  private static Random s_random = new Random(new SecureRandom().nextLong());

  //////////////   Simple array methods ///////////////

  /**
   * Create an array of floats (Useful in jython)
   *
   * @param n0 number of elements
   * @return an array of floats
   */
  public static float[] allocFloat(int n0) {
    return new float[n0];
  }

  /**
   * Create a 2D array of floats (Useful in jython)
   *
   * @param n0 number of elements in fast dimension
   * @param n1 number of elements in slow dimension
   * @return a 2 dimensional array of floats
   */
  public static float[][] allocFloat(int n0, int n1) {
    return new float[n1][n0];
  }

  /**
   * Create a 3D array of floats (Useful in jython)
   *
   * @param n0 number of elements in fast dimension
   * @param n1 number of elements in second fastest dimension
   * @param n2 number of elements in slow dimension
   * @return a 3 dimensional array of floats
   */
  public static float[][][] allocFloat(int n0, int n1, int n2) {
    return new float[n2][n1][n0];
  }

  /**
   * Create an array of ints (Useful in jython)
   *
   * @param n0 number of elements
   * @return an array of ints
   */
  public static int[] allocInt(int n0) {
    return new int[n0];
  }

  /**
   * Create a 2D array of ints (Useful in jython)
   *
   * @param n0 number of elements in fast dimension
   * @param n1 number of elements in slow dimension
   * @return a 2 dimensional array of ints
   */
  public static int[][] allocInt(int n0, int n1) {
    return new int[n1][n0];
  }

  /**
   * Create a 3D array of ints (Useful in jython)
   *
   * @param n0 number of elements in fast dimension
   * @param n1 number of elements in second fastest dimension
   * @param n2 number of elements in slow dimension
   * @return a 3 dimensional array of ints
   */
  public static int[][][] allocInt(int n0, int n1, int n2) {
    return new int[n2][n1][n0];
  }

  /** Make an efficient copy of a float array.
      @param in Input array
      @param out Output array of same size (or larger) than input
   */
  public static void copy(float[] out, float[] in) {
    System.arraycopy(in, 0, out, 0,
                     in.length < out.length ? in.length : out.length);
  }

  /** Make an efficient copy of a float array.
      @param in Input array
      @param out Output array of same size (or larger) than input
   */
  public static void copy(float[][] out, float[][] in) {
    for (int i=0; i<in.length; ++i) {
      copy(out[i], in[i]);
    }
  }

  /** Make an efficient copy of a float array.
      @param in Input array
      @param out Output array of same size (or larger) than input
   */
  public static void copy(float[][][] out, float[][][] in) {
    for (int i=0; i<in.length; ++i) {
      copy(out[i], in[i]);
    }
  }

  /** Make an efficient copy of a float array. (Faster than a clone())
      @param arr Copy this array.
      @return new instance of array with same contents as arr
   */
  public static float[] copy(float[] arr) {
    float[] result = new float[arr.length];
    System.arraycopy(arr, 0, result, 0, arr.length);
    return result;
  }

  /** Make an efficient deep copy of a float array
      @param arr Copy this array.
      @return new instance of array with same contents as arr
   */
  public static float[][] copy(float[][] arr) {
    float[][] result = new float[arr.length][];
    for (int i=0; i<result.length; ++i) {
      result[i] = copy(arr[i]);
    }
    return result;
  }

  /** Make an efficient deep copy of a float array
      @param arr Copy this array.
      @return new instance of array with same contents as arr
   */
  public static float[][][] copy(float[][][] arr) {
    float[][][] result = new float[arr.length][][];
    for (int i=0; i<result.length; ++i) {
      result[i] = copy(arr[i]);
    }
    return result;
  }

  /** Make an efficient copy of a double array. (Faster than a clone())
      @param arr Copy this array.
      @return new instance of array with same contents as arr
   */
  public static double[] copy(double[] arr) {
    double[] result = new double[arr.length];
    System.arraycopy(arr, 0, result, 0, arr.length);
    return result;
  }

  /** Make an efficient deep copy of a double array
      @param arr Copy this array.
      @return new instance of array with same contents as arr
   */
  public static double[][] copy(double[][] arr) {
    double[][] result = new double[arr.length][];
    for (int i=0; i<result.length; ++i) {
      result[i] = copy(arr[i]);
    }
    return result;
  }

  /** Make an efficient deep copy of a double array
      @param arr Copy this array.
      @return new instance of array with same contents as arr
   */
  public static double[][][] copy(double[][][] arr) {
    double[][][] result = new double[arr.length][][];
    for (int i=0; i<result.length; ++i) {
      result[i] = copy(arr[i]);
    }
    return result;
  }

  /** Fill all values of an array with a constant.
      @param arr Array to fill
      @param value value to assign to all elements
   */
  public static void fill(float[][][] arr, float value) {
    for (int i=0; i<arr.length; ++i) {ArrayUtil.fill(arr[i], value);}
  }

  /** Fill all values of an array with a constant.
      @param arr Array to fill
      @param value value to assign to all elements
   */
  public static void fill(float[][] arr, float value) {
    for (int i=0; i<arr.length; ++i) {Arrays.fill(arr[i], value);}
  }

  /** Fill all values of an array with a constant.
      @param arr Array to fill
      @param value value to assign to all elements
   */
  public static void fill(double[][][] arr, double value) {
    for (int i=0; i<arr.length; ++i) {ArrayUtil.fill(arr[i], value);}
  }

  /** Fill all values of an array with a constant.
      @param arr Array to fill
      @param value value to assign to all elements
   */
  public static void fill(double[][] arr, double value) {
    for (int i=0; i<arr.length; ++i) {Arrays.fill(arr[i], value);}
  }

  /** Add a constant to all values of an array
      @param arr Array to fill
      @param value value to add to all elements
   */
  public static void add(float[][][] arr, float value) {
    for (int i=0; i<arr.length; ++i) {ArrayUtil.add(arr[i], value);}
  }

  /** Add a constant to all values of an array
      @param arr Array to fill
      @param value value to add to all elements
   */
  public static void add(float[][] arr, float value) {
    for (int i=0; i<arr.length; ++i) {ArrayUtil.add(arr[i], value);}
  }

  /** Add a constant to all values of an array
      @param arr Array to fill
      @param value value to add to all elements
   */
  public static void add(float[] arr, float value) {
    for (int i=0; i<arr.length; ++i) {arr[i] += value;}
  }

  /** Return the dot product of two equal length arrays
      @param arr1 Array to dot with arr2.  Same length as arr2.
      @param arr2 Array to dot with arr1.  Same length as arr1.
      @return Some of product of each corresponding element.
   */
  public static double dot(float[] arr1, float[] arr2) {
    assert arr1.length == arr2.length;
    double result = 0;
    for (int i=0; i<arr1.length; ++i) {
      result += arr1[i]*arr2[i];
    }
    return result;
  }

  /** Return the dot product of two equal length arrays
      @param arr1 Array to dot with arr2.  Same length as arr2.
      @param arr2 Array to dot with arr1.  Same length as arr1.
      @return Some of product of each corresponding element.
   */
  public static double dot(double[] arr1, double[] arr2) {
    assert arr1.length == arr2.length;
    double result = 0;
    for (int i=0; i<arr1.length; ++i) {
      result += arr1[i]*arr2[i];
    }
    return result;
  }

  /** Return the dot product of two equal length arrays
      @param arr1 Array to dot with arr2.  Same length as arr2.
      @param arr2 Array to dot with arr1.  Same length as arr1.
      @return Some of product of each corresponding element.
   */
  public static double dot(float[][] arr1, float[][] arr2) {
    assert arr1.length == arr2.length;
    double result = 0;
    for (int col=0; col<arr1.length; ++col) {
      assert arr1[col].length == arr2[col].length;
      for (int row=0; row<arr1[col].length; ++row) {
        result += arr1[col][row]*arr2[col][row];
      }
    }
    return result;
  }

  /** Return the dot product of two equal length arrays
      @param arr1 Array to dot with arr2.  Same length as arr2.
      @param arr2 Array to dot with arr1.  Same length as arr1.
      @return Some of product of each corresponding element.
   */
  public static double dot(float[][][] arr1, float[][][] arr2) {
    assert arr1.length == arr2.length;
    double result = 0;
    for (int col=0; col<arr1.length; ++col) {
      assert arr1[col].length == arr2[col].length;
      for (int row=0; row<arr1[col].length; ++row) {
        assert arr1[col][row].length == arr2[col][row].length;
        for (int dim=0; dim<arr1[col][row].length; ++dim) {
          result += arr1[col][row][dim]*arr2[col][row][dim];
        }
      }
    }
    return result;
  }

  /** Find the correlation of data fit with modeled data.
      @param modeled Compare this with the data
      @param data Data to be fit
      @return coefficient calculated as dot(model,data)/dot(data,data)
  */
  public static double corrFit(float[] modeled, float[] data) {
    return dot(modeled,data)/dot(data,data);
  }

  /** Find the correlation of data fit with modeled data.
      @param modeled Compare this with the data
      @param data Data to be fit
      @return coefficient calculated as dot(model,data)/dot(data,data)
  */
  public static double corrFit(float[][] modeled, float[][] data) {
    return dot(modeled,data)/dot(data,data);
  }

  /** Get the root-mean-square value of the array
      @param arr Use all values of this array.
      @return Root mean square value of the array.
   */
  public static double rms(float[] arr) {
    int n=arr.length;
    if (n==0) return 0;
    return sqrt(dot(arr,arr)/n);
  }

  /** Get the root-mean-square value of the array
      @param arr Use all values of this array.
      @return Root mean square value of the array.
   */
  public static double rms(double[] arr) {
    int n=arr.length;
    if (n==0) return 0;
    return sqrt(dot(arr,arr)/n);
  }

  /** Get the root-mean-square value of the array
      @param arr Use all values of this array.
      @return Root mean square value of the array.
   */
  public static double rms(float[][] arr) {
    int n=arr.length;
    if (n==0) return 0;
    n *= arr[0].length;
    if (n==0) return 0;
    return sqrt(dot(arr,arr)/n);
  }

  /** Get the root-mean-square value of the array
      @param arr Use all values of this array.
      @return Root mean square value of the array.
   */
  public static double rms(float[][][] arr) {
    int n=arr.length;
    if (n==0) return 0;
    n *= arr[0].length;
    if (n==0) return 0;
    n *= arr[0][0].length;
    if (n==0) return 0;
    return sqrt(dot(arr,arr)/n);
  }

  /** Multiply all values of one array with another.
      @param out Modify this array by multiplying each element
      by corresponding elements of the scale array;
      @param scale Multiply this array by the other array,
      without modification.
   */
  public static void multiply(float[][][] out, float[][][] scale) {
    for (int i=0; i<out.length; ++i) {
      ArrayUtil.multiply(out[i], scale[i]);
    }
  }

  /** Multiply all values of one array with another.
      @param out Modify this array by multiplying each element
      by corresponding elements of the scale array;
      @param scale Multiply this array by the other array,
      without modification.
   */
  public static void multiply(float[][] out, float[][] scale) {
    for (int i=0; i<out.length; ++i) {
      ArrayUtil.multiply(out[i], scale[i]);
    }
  }

  /** Multiply all values of one array with another.
      @param out Modify this array by multiplying each element
      by corresponding elements of the scale array;
      @param scale Multiply this array by the other array,
      without modification.
   */
  public static void multiply(float[] out, float[] scale) {
    for (int i=0; i<out.length; ++i) {
      out[i] *= scale[i];
    }
  }

  /** Multiply all values of one array with a scale factor
      @param out Modify this array by multiplying each element
      by the scale factor
      @param scale Multiply the array by this scale factor.
   */
  public static void multiply(float[][][] out, float scale) {
    for (int i=0; i<out.length; ++i) {
      ArrayUtil.multiply(out[i], scale);
    }
  }

  /** Multiply all values of one array with a scale factor
      @param out Modify this array by multiplying each element
      by the scale factor
      @param scale Multiply the array by this scale factor.
   */
  public static void multiply(float[][] out, float scale) {
    for (int i=0; i<out.length; ++i) {
      ArrayUtil.multiply(out[i], scale);
    }
  }

  /** Multiply all values of one array with a scale factor
      @param out Modify this array by multiplying each element
      by the scale factor
      @param scale Multiply the array by this scale factor.
   */
  public static void multiply(float[] out, float scale) {
    for (int i=0; i<out.length; ++i) {
      out[i] *= scale;
    }
  }

  /** Multiply all values of one array with a scale factor
      @param out Modify this array by multiplying each element
      by the scale factor
      @param scale Multiply the array by this scale factor.
   */
  public static void multiply(double[] out, double scale) {
    for (int i=0; i<out.length; ++i) {
      out[i] *= scale;
    }
  }

  /** Scale and add one array to another.
      @param out Modify this array by adding scaled version of other.
      @param second Multiply this array by the scale factor
      before adding to the out array.
      @param scale Multiply the second array by this scale factor.
   */
  public static void scaleAdd(float[][] out, float[][] second, double scale) {
    for (int i=0; i<out.length; ++i) {
      scaleAdd(out[i], second[i], scale);
    }
  }

  /** Scale and add one array to another.
      @param out Modify this array by adding scaled version of other.
      @param second Multiply this array by the scale factor
      before adding to the out array.
      @param scale Multiply the second array by this scale factor.
   */
  public static void scaleAdd(float[] out, float[] second, double scale) {
    for (int i=0; i<out.length; ++i) {
      out[i] += scale*second[i];
    }
  }

  /** Take reciprocal of all elements of an array.
      @param out Modify this array by taking the reciprocal
      (safely) of all elements.
   */
  public static void reciprocal(float[][][] out) {
    for (int i=0; i<out.length; ++i) {
      ArrayUtil.reciprocal(out[i]);
    }
  }

  /** Take reciprocal of all elements of an array.
      @param out Modify this array by taking the reciprocal
      (safely) of all elements.
   */
  public static void reciprocal(float[][] out) {
    for (int i=0; i<out.length; ++i) {
      ArrayUtil.reciprocal(out[i]);
    }
  }

  /** Take reciprocal of all elements of an array.
      @param out Modify this array by taking the reciprocal
      (safely) of all elements.
   */
  public static void reciprocal(float[] out) {
    for (int i=0; i<out.length; ++i) {
      out[i] = (float) Almost.FLOAT.reciprocal(out[i]);
    }
  }

  /** Return if two vectors are equal within floating precision
      @param v First vector
      @param w Second vector
      @return true if equal
   */
  public static boolean equal(float[] v, float[] w) {
    float max = getMax(v); // to avoid inequality for near-zero values
    float min = getMin(v); // to avoid inequality for near-zero values
    for (int i=0; i<v.length; ++i) {
      if (!s_mostly.equal(v[i], w[i]) &&
          !s_mostly.equal(v[i]+max, w[i]+max) &&
          !s_mostly.equal(v[i]+min, w[i]+min)) {return false;}
    }
    return true;
  }

  /** Return if two vectors are equal within floating precision
      @param v First vector
      @param w Second vector
      @return true if equal
   */
  public static boolean equal(float[][] v, float[][] w) {
    for (int i=0; i<v.length; ++i) {
      if (!equal(v[i], w[i])) {return false;}
    }
    return true;
  }

  /** Return if two vectors are equal within floating precision
      @param v First vector
      @param w Second vector
      @return true if equal
   */
  public static boolean equal(float[][][] v, float[][][] w) {
    for (int i=0; i<v.length; ++i) {
      if (!equal(v[i], w[i])) {return false;}
    }
    return true;
  }

  /** Return the minimum value of an array
      @param v array to examine
      @return the minimum
  */
  public static float getMin(float[] v) {
    float result = Float.MAX_VALUE;
    for (int i=0; i<v.length; ++i) {
      if (result > v[i]) result = v[i];
    }
    return result;
  }

  /** Return the minimum value of an array
      @param v array to examine
      @return the minimum
  */
  public static float getMin(float[][] v) {
    float result = Float.MAX_VALUE;
    for (int i=0; i<v.length; ++i) {
      float min = getMin(v[i]);
      if (result > min) result = min;
    }
    return result;
  }

  /** Return the minimum value of an array
      @param v array to examine
      @return the minimum
  */
  public static float getMin(float[][][] v) {
    float result = Float.MAX_VALUE;
    for (int i=0; i<v.length; ++i) {
      float min = getMin(v[i]);
      if (result > min) result = min;
    }
    return result;
  }

  /** Return the maximum value of an array
      @param v array to examine
      @return the maximum values.
  */
  public static float getMax(float[] v) {
    float result =  -Float.MAX_VALUE;
    for (int i=0; i<v.length; ++i) {
      if (result < v[i]) result = v[i];
    }
    return result;
  }

  /** Return the maximum value of an array
      @param v array to examine
      @return the maximum values.
  */
  public static float getMax(float[][] v) {
    float result =  -Float.MAX_VALUE;
    for (int i=0; i<v.length; ++i) {
      float max = getMax(v[i]);
      if (result < max) result = max;
    }
    return result;
  }

  /** Return the maximum value of an array
      @param v array to examine
      @return the maximum values.
  */
  public static float getMax(float[][][] v) {
    float result =  -Float.MAX_VALUE;
    for (int i=0; i<v.length; ++i) {
      float max = getMax(v[i]);
      if (result < max) result = max;
    }
    return result;
  }

  /** Return the minimum value of an array
      @param v array to examine
      @return the minimum
  */
  public static double getMin(double[] v) {
    double result = Float.MAX_VALUE;
    for (int i=0; i<v.length; ++i) {
      if (result > v[i]) result = v[i];
    }
    return result;
  }

  /** Return the maximum value of an array
      @param v array to examine
      @return the maximum values.
  */
  public static double getMax(double[] v) {
    double result =  -Float.MAX_VALUE;
    for (int i=0; i<v.length; ++i) {
      if (result < v[i]) result = v[i];
    }
    return result;
  }

  /** Return the minimum and maximum values of an array
      @param v array to examine
      @return a float[2] containing minimum and maximum values.
  */
  public static float[] getRange(float[] v) {
    float[] result = new float[]{Float.MAX_VALUE, -Float.MAX_VALUE};
    updateRange(result, v);
    return result;
  }

  /** Return the minimum and maximum values of an array
      @param v array to examine
      @return a float[2] containing minimum and maximum values.
  */
  public static float[] getRange(float[][] v) {
    float[] result = new float[]{Float.MAX_VALUE, -Float.MAX_VALUE};
    for (float[] row: v) {
      updateRange(result, row);
    }
    return result;
  }

  /** Set the minimum and maximum values of an array
      @param range Update this range.  Should already be initialized
      to previous range.  Dimension of 2.
      @param v array to examine
  */
  public static void updateRange(float[] range, float[] v) {
    for (int i=0; i<v.length; ++i) {
      if (range[0] > v[i]) range[0] = v[i];
      if (range[1] < v[i]) range[1] = v[i];
    }
  }

  /** Return the minimum and maximum values of an array
      @param v array to examine
      @return a double[2] containing minimum and maximum values.
  */
  public static double[] getRange(double[] v) {
    double[] result = new double[]{Float.MAX_VALUE, -Float.MAX_VALUE};
    for (int i=0; i<v.length; ++i) {
      if (result[0] > v[i]) result[0] = v[i];
      if (result[1] < v[i]) result[1] = v[i];
    }
    return result;
  }

  /** Return the average value of an array
      @param v array to examine
      @return The average value
  */
  public static float getAverage(float[] v) {
    return getAverage(v, v.length);
  }

  /** Return the average value of an array
      @param v array to examine
      @return The average value
  */
  public static float getAverage(float[][] v) {
    double sum = 0;
    for (float[] a: v) {
      sum += getAverage(a, a.length);
    }
    return (float)(sum/v.length);
  }

  /** Return the average value of an array
      @param v array to examine
      @return The average value
  */
  public static double getAverage(double[] v) {
    return getAverage(v, v.length);
  }

  /** Return the average value of an array
      @param v array to examine
      @param length Number of elements to average
      @return The average value
  */
  public static float getAverage(float[] v, int length) {
    if (length == 0) return 0.f;
    double sum = 0;
    for (int i=0; i<length; ++i) {
      sum += v[i];
    }
    return (float)(sum/length);
  }

  /** Return the average value of an array
      @param v array to examine
      @param length Number of elements to average
      @return The average value
  */
  public static double getAverage(double[] v, int length) {
    if (length == 0) return 0.f;
    double sum = 0;
    for (int i=0; i<length; ++i) {
      sum += v[i];
    }
    return sum/length;
  }

  /** Get the number of unique values in an array,
      assuming float precision
      @param values array of values to examine, unsorted.
      @return number of unique values in the array.
  */
  public static int getNumberUniqueValues(final double[] values) {
    IndexSorter sorter = new IndexSorter(values);
    int[] indices = sorter.getSortedIndices();
    IndexGrouper grouper = new IndexGrouper(new IndexGrouper.Grouper() {
        public boolean inSameGroup(int index1, int index2) {
          return Almost.FLOAT.equal(values[index1],
                                    values[index2]);
        }}, indices);
    int number = grouper.getNumberGroups();
    return number;
  }

  /** Interpolate zero values in array with
      linearly interpolated values from neighboring
      non-zero values
      @param array Input and output array.
  */
  public static void fillZeros(float[] array) {
    double[] copy = new double[array.length];
    for (int i=0; i<copy.length; ++i) {
      copy[i] = array[i];
    }
    fillZeros(copy);
    for (int i=0; i<copy.length; ++i) {
      array[i] = (float) copy[i];
    }
  }

  /** Interpolate zero values in array with
      linearly interpolated values from neighboring
      non-zero values
      @param array Input and output array.
  */
  public static void fillZeros(double[] array) {
    int left = 0;
    int n = array.length;

    // fill in constant values at front
    while (left < n && array[left] == 0) left++;
    if (left >=n ) return; // all zeros
    for (int i=0; i<left; ++i) {array[i] = array[left];}

    // fill in constant values at end
    int right = n-1;
    while (array[right] == 0) --right; // cannot go out of bounds
    for (int i=n-1; i>right; --i) {array[i] = array[right];}

    // in-fill between non-zero left and right values
    while (true) {
      right = left + 1;
      while (right < n && array[right] == 0) ++right;
      if (right >= n) break;
      for (int i=left+1; i<right; ++i) {
        double fraction = (double)(i-left)/(right-left);
        array[i] =  (1-fraction)*array[left] + fraction*array[right];
      }
      left = right;
    }
  }

  //////////////////// 3D vector math ////////////////

  /** Make an efficient copy of a 3D float array. (Faster than a clone())
      @param arr Copy this array.
      @return new instance of array with same contents as arr
   */
  public static float[] copy3(float[] arr) {
    if (arr == null) return null;
    float[] result = new float[3];
    result[0] = arr[0]; result[1] = arr[1]; result[2] = arr[2];
    return result;
  }

  /** Make an efficient copy of a 3D float array. (Faster than a clone())
      @param result output array with same contents as arr
      @param arr Copy this array.
   */
  public static void copy3(float[] result, float[] arr) {
    result[0] = arr[0]; result[1] = arr[1]; result[2] = arr[2];
  }

  /** Get magnitude of vector
      @param v Input vector.  Not changed.
      @return magnitude
  */
  public static float magnitude3(float[] v) {
    return (float) sqrt(v[0]*v[0]+ v[1]*v[1]+ v[2]*v[2]);
  }

  /** Get dot product of two 3D vectors
      @param v Input vector.  Not changed.
      @param w Input vector.  Not changed.
      @return dot product
  */
  public static float dot3(float[] v, float[] w) {
    return v[0]*w[0]+ v[1]*w[1]+ v[2]*w[2];
  }

  /** Get cross product of two 3D vectors
      Assumes LEFT-HANDED coordinate system.
      @param v Input vector.  Not changed.
      @param w Input vector.  Not changed.
      @param result cross product
  */
  public static void cross3(float[] result, float[] v, float[] w) {
    result[0] = -(v[1]*w[2] - v[2]*w[1]);
    result[1] =  (v[0]*w[2] - v[2]*w[0]);
    result[2] = -(v[0]*w[1] - v[1]*w[0]);
  }

  /** Get the cosine of the angle between two vectors.
      @param v First vector.  Magnitude ignored, if non-zero.
      @param w First vector.  Magnitude ignored, if non-zero.
      @return Cosine of angle between two vectors.
 * @throws IllPosedException bad geometry
   */
  public static float cos3(float[] v, float[] w) throws IllPosedException {
    double vv = magnitude3(v);
    double ww = magnitude3(w);
    if (Almost.FLOAT.zero(vv*ww)) {
      throw new IllPosedException("Vector has zero magnitude v="+
                                  StringUtil.toString(v)+" w="+
                                  StringUtil.toString(w));
    }
    return (float) (Almost.FLOAT.divide(dot3(v,w), (vv*ww), 1.));
  }

  /** Get the sine of the angle between two vectors
      Always positive.
      @param v First vector.  Magnitude ignored, if non-zero.
      @param w First vector.  Magnitude ignored, if non-zero.
      @return Sine of angle between two vectors, always positive.
 * @throws IllPosedException bad geometry
   */
  public static float sin3(float[] v, float[] w) throws IllPosedException {
    double c = cos3(v,w);
    if (c > 1) return 0; // from bad roundoff only
    return (float) sqrt(1. - c*c);
  }

  /** Scale a 3D vector by a constant scalar.
      @param v Input and output vector.
      @param c scalar
  */
  public static void scale3(float[] v, float c) {
    v[0] *= c; v[1] *= c; v[2] *= c;
  }

  /** Scale a 3D vector by a constant scalar and add to another vector
      @param out add result to this vector
      @param v Input vector.
      @param c Multiply input vector v by this value before adding.
  */
  public static void scaleAdd3(float[] out, float[] v, float c) {
    out[0] += v[0]*c; out[1] += v[1]*c; out[2] += v[2]*c;
  }

  /** Make scaled sum of two vectors
      @param out Output sum of two scaled input vectors.
      @param scale1 Scale v1 by this factor
      @param v1 First input vector
      @param scale2 Scale v2 by this factor
      @param v2 Second input vector
  */
  public static void scaleAdd3(float[] out,  float scale1, float[] v1,
                               float scale2, float[] v2) {
    out[0] = v1[0]*scale1 + v2[0]*scale2;
    out[1] = v1[1]*scale1 + v2[1]*scale2;
    out[2] = v1[2]*scale1 + v2[2]*scale2;
  }

  /** Add two 3D vectors.
      @param result addition of two vectors
      Can be the same as v or w.
      @param v Input vector.  Not changed.
      @param w Input vector.  Not changed.
  */
  public static void add3(float[] result, float[] v, float[] w) {
    result[0] = v[0] + w[0];
    result[1] = v[1] + w[1];
    result[2] = v[2] + w[2];
  }

  /** Subtract two 3D vectors.
      @param result subtraction of two vectors
      Can be the same as v or w.
      @param v Input vector.  Not changed.
      @param w Input vector to be subtracted.  Not changed.
  */
  public static void subt3(float[] result, float[] v, float[] w) {
    result[0] = v[0] - w[0];
    result[1] = v[1] - w[1];
    result[2] = v[2] - w[2];
  }

  /** Get Cartesian distance between two 3D points.
      @param p1 First point
      @param p2 Second point.
      @return distance
  */
  public static float distance3(float[] p1, float[] p2) {
    return (float) sqrt
      ((p1[0]-p2[0])*(p1[0]-p2[0]) +
       (p1[1]-p2[1])*(p1[1]-p2[1]) +
       (p1[2]-p2[2])*(p1[2]-p2[2]));
  }

  /** Create a vector pointing from one point to another.
      @param p1 The returned vector begins at this point.
      @param p2 The returned vector ends at this point.
      @return New vector pointing from p1 to p2, with the magnitude
      of the distance between the two points.
   */
  public static float[] direction3(float[] p1, float[] p2) {
    float[] result = new float[3];
    subt3(result, p2, p1);
    return result;
  }

  /** Scale vector to desired magnitude.
      @param v Input vector and output vector
      @param magnitude New magnitude
   */
  public static void setMagnitude3(float[] v, double magnitude) {
    scale3(v, (float) Almost.FLOAT.divide(magnitude,magnitude3(v),1.));
  }

  /** Scale vector to desired magnitude.
      @param v Input vector.  Not changed.
      @param magnitude New magnitude
      @return Same direction as v, with adjusted magnitude.
   */
  public static float[] newMagnitude3(float[] v, double magnitude) {
    v = ArrayUtil.copy3(v);
    setMagnitude3(v, magnitude);
    return v;
  }

  /** Reverse the direction of a vector to point up or down,
      according to the sign of the third dimension.
      @param v Vector to flip
      @param verticalDimension This
      @param up If true, then ensure v[verticalDimension] is negative, so
      that vector points up toward decreasing depths.
      If false, ensure v[verticalDimension] is positive,
      pointing toward increasing depths.
   */
  public static void flip3(float[] v, int verticalDimension, boolean up) {
    if ((v[verticalDimension] > 0. && up) ||
        (v[verticalDimension] < 0. && !up)) {
      v[0] = -v[0]; v[1] = -v[1]; v[2] = -v[2];
    }
  }

  private static Almost s_mostly = new Almost(0.0001);

  /** Specify a line in 3D with a point and a direction. */
  public static class Line3 {
    private float[] _point = null;
    private float[] _direction = null;

    /** Specify point on line and direction of line
        @param point Point on the line.
        @param direction Direction of line
        (Insensitive to magnitude and polarity)
     * @throws IllPosedException bad geometry
    */
    public Line3(float[] point, float[] direction) throws IllPosedException {
      _point = copy3(point);
      if (Almost.FLOAT.zero(magnitude3(direction))) {
        throw new IllPosedException("direction has zero magnitude");
      }
      _direction = newMagnitude3(direction, 1);
    }

    /** Project a point onto the line.
        @param p The point to project.
        @return The projected point.
     */
    public float[] project(float[] p) {
      float[] delta = new float[3];
      subt3(delta, p, _point);
      float[] result = newMagnitude3(_direction, dot3(delta, _direction));
      add3(result, _point, result);
      return result;
    }

    /** Find the intersection of the line with the plane
        @param plane intersect with this plane
        @return Point at which this line intersects with the plane.
     * @throws IllPosedException bad geometry
     */
    public float[] intersect(Plane3 plane) throws IllPosedException {
      float[] a = plane.project(_point);
      float pa = distance3(a, _point); // distance from _point to plane

      if (Almost.FLOAT.zero(pa)) {return copy3(_point);} // already intersects
      if (abs(dot3(plane._normal, _direction)) < 0.000001) {
        throw new IllPosedException("Plane and line are parallel");
      }

      float[] b = project(a); // B is projection of A on line
      a = null;
      subt3(b, b, _point); // vector from _point to B
      float pb = magnitude3(b);
      if (Almost.FLOAT.zero(pb)) {return copy3(_point);} // impossible

      /* pb/pa = pa/pc,
         pa is the distance from _point to the nearest point A on the plane.
         point B is the projection of point A onto the line.
         pb is the distance from _point to B.
         pc is the distance from _point to the desired intersection point
       */

      scale3(b, (float) Almost.FLOAT.divide(pa*pa, pb*pb, 1.));
      add3(b, _point, b);
      return b;
    }
  }

  /** Remember and apply rotation around an arbitrary axis. */
  public static class Rotate3 {
    private float[] _normal = new float[3];
    private float _cos = 0;
    private float _sin = 0;

    /** Remember rotation of one vector into the other.
        @param v1 Rotate this vector into v2 along plane containing both.
        @param v2 Rotate v1 into this vector along plane containing both.
     * @throws IllPosedException bad geometry
    */
    public Rotate3(float[] v1, float[] v2) throws IllPosedException {
      _cos = cos3(v1, v2);
      _sin = (float) sqrt(1. - _cos*_cos); // sign is in _normal
      cross3(_normal, v1, v2);
      setMagnitude3(_normal, 1);
    }

    /** Get a rotated version of a vector.
     * @param v vector to rotate
        @return Rotated vector.
    */
    public float[] apply(float[] v) {
      float[] result = new float[3];
      cross3(result, _normal, v);
      scale3(result, _sin);

      scaleAdd3(result, v, _cos);
      scaleAdd3(result, _normal, (1 - _cos)*dot3(_normal, v));

      return result;
    }
  }

  /** Specifies a plane in 3D  */
  public static class Plane3 {
    private float[] _normal = null;
    private float[] _point = null;

    /** Specify point on plane and normal to the plane
        @param point Point on the plane.
        @param normal Normal to the plane.
        (Insensitive to magnitude and polarity)
     * @throws IllPosedException bad geometry
    */
    public Plane3(float[] point, float[] normal) throws IllPosedException {
      _point = copy3(point);
      if (Almost.FLOAT.zero(magnitude3(normal))) {
        throw new IllPosedException("normal has zero magnitude");
      }
      _normal = newMagnitude3(normal, 1);
    }

    /** Project a point on the the plane.
        @param point Project this point onto plane.  Not modified.
        @return Projected point.
    */
    public float[] project(float[] point) {
      float[] a = new float[3];
      subt3(a, point, _point);     // a = point - _point
      float[] b = copy3(_normal);
      scale3(b, dot3(a, _normal)); // b <= a projected onto _normal
      subt3(a, a, b);              // a <= a minus b
      add3(a, _point, a);
      return a;
    }

    /** Get the line at the intersection of two planes.
        @param plane Intersect with this plane.
        @return Line at the intersection
     * @throws IllPosedException bad geometry
    */
    public Line3 intersect(Plane3 plane) throws IllPosedException {
      float[] direction = new float[3]; // direction of intersecting line
      cross3(direction, plane._normal, _normal); // parallels both planes
      if (Almost.FLOAT.zero(magnitude3(direction))) {
        throw new IllPosedException("planes are parallel");
      }
      // find a line on this plane that is perpendicular to intersection line
      float[] angle = new float[3];
      cross3(angle, direction, _normal);
      Line3 line = new Line3(_point, angle);
      float[] point = line.intersect(plane); // point on both planes
      return new Line3(point, direction);
    }
  }

  /**  Thrown by bad intersections and projections of
       geometric objects.
  */
  public static class IllPosedException extends Exception {
    private static final long serialVersionUID = 1L;

    /** Constructor
        @param reason Description of this Exception
    */
    public IllPosedException(String reason) {super (reason);}

    /** Constructor
        @param reason Description of this Exception
        @param cause The cause of this Exception
    */
    public IllPosedException(String reason, Exception cause) {
      super (reason);
      initCause(cause);
    }

    /** Constructor
        @param cause The cause of this Exception
    */
    public IllPosedException(Exception cause) {
      super (cause.toString());
      initCause(cause);
    }

  }

  ///////////////////// RANDOM ARRAYS /////////////////////

  /** Create an array of integers in random order,
      from range of 0 to length-1.
      @param length Length of new array.
      @return Array of integers in random order, with unique values
      from 0 to length-1.
  */
  public static int[] shuffledInts(int length) {
    int[] result = new int[length];
    for (int i=0; i<result.length; ++i) {
      result[i] = i;
    }
    shuffle(result);
    return result;
  }

  /** Rearrange all elements of an int array, in place.
      @param array Rearrange these.
  */
  public static void shuffle(int[] array) {
    shuffle(array, array.length);
  }

  /** Rearrange all elements of an object array, in place.
  @param array Rearrange these.
  */
  public static void shuffle(Object[] array) {
    int n=array.length;
    Object temp;
    for (int i=1; i<n; ++i) {
      int swap = s_random.nextInt(i+1);
      temp = array[swap];
      array[swap] = array[i];
      array[i] = temp;
    }
  }

  /** Rearrange first elements of an int array, in place.
      @param array Rearrange these.
      @param n Rearrange the first n elements of the array.
  */
  public static void shuffle(int[] array, int n) {
    for (int i=1; i<n; ++i) {
      int swap = s_random.nextInt(i+1);
      int temp = array[swap];
      array[swap] = array[i];
      array[i] = temp;
    }
  }

  /** Get first non-zero sample of array.
      If all samples are zero, returns the length of the array.
      @param data Array of data to check
      @return Index of first sample of data that is not zero.
      If all samples are zero, returns the length of the array.
   */
  public static int firstNonzeroSample(float[] data) {
    int result = 0;
    while (result < data.length && data[result] == 0) {
      ++result;
    }
    return result;
  }

  ///////////////////// TESTS /////////////////////

  /** Test code
 * @param args command line
 * @throws Exception test failures */
  public static void main(String[] args) throws Exception {
    try {
      assert false;
      throw new IllegalStateException("Enable exceptions with -ea");
    } catch (AssertionError e) {}

    assert null != new ArrayUtil();
    { // check magnitude
      float[] v = {3, 6.6f, -7.2f};
      double magnitude = 42.;
      float[] w = ArrayUtil.newMagnitude3(v, magnitude);
      assert Almost.FLOAT.equal(ArrayUtil.magnitude3(w), magnitude);
      assert Almost.FLOAT.equal(sqrt(3)*ArrayUtil.rms(w), magnitude);

      double m = 0.; for (int i=0; i<w.length; ++i) {m +=w[i]*w[i];}
      assert Almost.FLOAT.equal(sqrt(m), magnitude);

    }
    { // test projection
      Random random = new Random(52626);
      for (int severalTimes=0; severalTimes<7; ++severalTimes) {
        for (int i=0; i<3; ++i) {
          float[] origin = randomPoint(random);
          float[] point = randomPoint(random);
          float[] normal = new float[3];
          normal[i] = 77*random.nextFloat();
          Plane3 plane = new Plane3(origin, normal);
          float[] a = plane.project(point);
          for (int k=0; k<3; ++k) {
            if (k==i)
              assert Almost.FLOAT.equal(a[k], origin[k]) ;
            else
              assert Almost.FLOAT.equal(a[k], point[k]);
          }
        }
      }
    }

    { // intersect line with plane
      Random random = new Random(267676);
      for (int severalTimes=0; severalTimes<7; ++severalTimes) {
        float[] originPlane = randomPoint(random);
        float[] originLine = randomPoint(random);
        float[] normalPlane = randomPoint(random);
        float[] normalLine = randomPoint(random);
        Plane3 plane = new Plane3(originPlane, normalPlane);
        Line3 line   = new Line3(originLine, normalLine);
        float[] intersect = line.intersect(plane);
        float[] i1 = plane.project(intersect);
        float[] i2 = line.project(intersect);
        assertEqual3(intersect, i1);
        assertEqual3(intersect, i2);
        assertEqual3(i1, i2);
      }
    }

    { // intersect line with plane
      Random random = new Random(267676);
      for (int severalTimes=0; severalTimes<7; ++severalTimes) {
        float[] originPlane1 = randomPoint(random);
        float[] normalPlane1 = randomPoint(random);
        float[] originPlane2 = randomPoint(random);
        float[] normalPlane2 = randomPoint(random);
        Plane3 plane1 = new Plane3(originPlane1, normalPlane1);
        Plane3 plane2 = new Plane3(originPlane2, normalPlane2);
        Line3 line = plane1.intersect(plane2);
        float[] origin = line.project(new float[3]);
        float[] point = line.project(randomPoint(random));
        assertNotEqual3(origin, point); // check line is not a point
        float[] p1 = plane1.project(point);
        float[] p2 = plane1.project(point);
        assertEqual3(p1, p2);
      }
    }

    { // test simple rotation
      Rotate3 rotate = new Rotate3(new float[] {0, 0, 9},
                                   new float[] {0, 13, 0});
      float[] v = new float[] {3, 0, 7};
      v = rotate.apply(v);
      assertEqual3(v, new float[] {3, 7, 0});
      v = rotate.apply(v);
      assertEqual3(v, new float[] {3, 0, -7});
      v = rotate.apply(v);
      assertEqual3(v, new float[] {3, -7, 0});
      v = rotate.apply(v);
      assertEqual3(v, new float[] {3, 0, 7});
    }
    { // test simple rotation
      Rotate3 rotate = new Rotate3(new float[] {0, 9, 0},
                                   new float[] {13, 0, 0});
      float[] v = new float[] {3, 0, 7};
      v = rotate.apply(v);
      assertEqual3(v, new float[] {0, -3, 7});
      v = rotate.apply(v);
      assertEqual3(v, new float[] {-3, 0, 7});
      v = rotate.apply(v);
      assertEqual3(v, new float[] {0, 3, 7});
      v = rotate.apply(v);
      assertEqual3(v, new float[] {3, 0, 7});
    }
    { // test random rotation
      Random random = new Random(7575339);
      for (int severalTimes=0; severalTimes<7; ++severalTimes) {
        float[] v1 = randomPoint(random);
        float[] v2 = randomPoint(random);
        Rotate3 rotate = new Rotate3(v1, v2);
        float[] vnew = rotate.apply(v1);
        assert Almost.FLOAT.equal(magnitude3(v1), magnitude3(vnew));
        setMagnitude3(vnew, magnitude3(v2));
        assertEqual3(vnew, v2);
      }
    }

    testExceptions();
    testFillZeros();
    testShuffleString();

  }

  private static void testShuffleString() {
    Random r = new Random(1111);

    int n = r.nextInt(100);
    String[] ss = new String[n];
    for (int i = 0; i < n; ++i) {
      ss[i] = "a-" + r.nextInt();
    }
    shuffle(ss);
    assert ss.length == n;
  }

  private static void testExceptions() {
    try {
      Plane3 plane1 = new Plane3(new float[] {0, 0, 0}, new float[] {1, 2, 3});
      Plane3 plane2 = new Plane3(new float[] {1, 1, 1}, new float[] {2, 4, 6});
      plane1.intersect(plane2);
      assert false : "unreachable";
    } catch (IllPosedException e) {}
    try {
      Plane3 plane = new Plane3(new float[] {0, 0, 0}, new float[] {1, 2, 3});
      float[] parallel = new float[3];
      cross3(parallel, new float[] {6,6,6}, new float[] {1, 2, 3});
      Line3 line = new Line3(new float[] {1, -1, 1}, parallel);
      dot3(parallel, new float[] {1, 2, 3});
      float[] point = line.intersect(plane);
      LOG.severe("Unreachable: "+point[0]+","+point[1]+","+point[2]);
      assert false : "unreachable";
    } catch (IllPosedException e) {}
    try {
      new Plane3(new float[] {1, 1, 1}, new float[3]);
      assert false : "unreachable";
    } catch (IllPosedException e) {}
    try {
      new Line3(new float[] {1, 1, 1}, new float[3]);
      assert false : "unreachable";
    } catch (IllPosedException e) {}
    try {
      cos3(new float[] {1, 1, 1}, new float[3]);
      assert false : "unreachable";
    } catch (IllPosedException e) {}
    try {
      new Rotate3(new float[] {1, 1, 1}, new float[3]);
      assert false : "unreachable";
    } catch (IllPosedException e) {}
  }

  private static void testFillZeros() {
    float[] test1 = new float[] { 1, 0, 3, 0, 5, 6, 0, 0, 0, 10, 0};
    float[] test2 = new float[] { 0, 0, 3, 0, 5, 6, 0, 0, 0, 10, 11};
    int n = test1.length;
    ArrayUtil.fillZeros(test1);
    ArrayUtil.fillZeros(test2);
    for (int i=0; i<n-1; ++i) {
      assert Almost.FLOAT.equal(test1[i], i+1) :
        "i+1="+(i+1)+" test1["+i+"]="+test1[i];
    }
    assert Almost.FLOAT.equal(test1[n-1], 10);
    assert Almost.FLOAT.equal(test2[0], 3);
    assert Almost.FLOAT.equal(test2[1], 3);
    for (int i=2; i<n; ++i) {
      assert Almost.FLOAT.equal(test2[i], i+1) :
        "i+1="+(i+1)+" test2["+i+"]="+test2[i];
    }
  }

  // for test code
  private static float[] randomPoint(Random random) {
    float[] result = new float[3];
    for (int i=0; i<3; ++i) {
      result[i] = 77*(random.nextFloat()-0.5f);
    }
    return result;
  }

  private static void assertEqual3(float[] v, float[] w) {
    assert (equal(v, w)) : (StringUtil.toString(v)+" == "+
                            StringUtil.toString(w));
  }

  private static void assertNotEqual3(float[] v, float[] w) {
    assert (!equal(v, w)) : (StringUtil.toString(v)+" != "+
                             StringUtil.toString(w));
  }
}

