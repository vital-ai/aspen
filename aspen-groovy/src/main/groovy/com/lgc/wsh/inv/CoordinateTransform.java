package com.lgc.wsh.inv;

import com.lgc.wsh.util.Almost;
import com.lgc.wsh.util.ArrayUtil;

import java.util.Arrays;
import java.util.logging.*;

/** Find a best linear combination of input
    coordinates to fit output coordinates.
    Finds a_io (a with subscripts i and o)
    to best approximate the linear transform.
    <pre>
      out_o = sum_i ( a_io * in_i )
    </pre>
    where in_i are input coordinates,<br>
    out_o are output coordinates,<br>
    i is the index of each input dimension,<br>
    and o is the index of each output dimension.<br>

    <p> The optimum coefficients minimize this least
    squares error:
    <pre>
    sum_oj [ sum_i ( a_io * in_ij ) - out_oj ]^2
    </pre>
    where in_ij is an input array of different coordinates,<br>
    out_oj is an output array of corresponding coordinates,<br>
    and j is the index of pairs of coordinates to be fit
    in a least-squares sense.

    <p> Normal equations (indexed by k) are solved independently for each o:
    <pre>
    sum_ij ( in_kj * in_ij * a_io ) = sum_j ( in_kj * out_oj )
    </pre>
    The Hessian is <code> H = sum_j ( in_kj * in_ij ) </code>
    and the gradient <code> b = - sum_j ( in_kj * out_oj ) </code>

    <p> The solution is undamped and may not behave as you
    want for degenerate solutions.

    <p> If the linear transform needs a translation shift,
    then include a constant as one of the input coordinates.
*/
public class CoordinateTransform {
  @SuppressWarnings("unused")
    private static final Logger LOG = Logger.getLogger("com.lgc.wsh.inv");

  private int _nout = 0;
  private int _nin = 0;
  private double [][] _hessian;
  private double [][] _b;
  private double [][] _a;
  private double [] _in0 = null;
  private double [] _out0 = null;
  private double [] _inScr = null;
  private double [] _outScr = null;

  /** Constructor sets number of input and output coordinates.
      @param dimensionOut Number of output coordinates.
      @param dimensionIn Number of input coordinates.
  */
  public CoordinateTransform(int dimensionOut, int dimensionIn) {
    _nout = dimensionOut;
    _nin = dimensionIn;
    _hessian =  new double[_nin][_nin];
    _b = new double[_nout][_nin];
    _inScr = new double[_nin];
    _outScr = new double[_nout];
  }

  /** Add an observation of a set of input and output coordinates
      You should add enough of these to determine (or overdetermine)
      a unique linear mapping.
      To allow translation, include a constant 1 as an input coordinate.
      @param out A set of observered output coordinates
      with an unknown linear relationship to input coordinates.
      @param in A set of observered input coordinates
      that should be linearly combined to calculate each of the
      output coordinates.
      To allow translation, include a constant 1.
  */
  public void add(double[] out, double[] in) {
    _a = null; // must redo any previous solution

    if (in.length != _nin) {
      throw new IllegalArgumentException("in must have dimension "+_nin);
    }

    if (out.length != _nout) {
      throw new IllegalArgumentException("out must have dimension "+_nout);
    }

    if (_in0  == null) {_in0  = in.clone();}
    if (_out0 == null) {_out0 = out.clone();}

    for (int i=0; i<_nin; ++i) {_inScr[i] = in[i] - _in0[i];}
    for (int i=0; i<_nout; ++i) {_outScr[i] = out[i] - _out0[i];}

    for (int k=0; k< _nin; ++k) {
      for (int i=0; i< _nin; ++i) {
        _hessian[k][i] += _inScr[k] *_inScr[i];
      }
      for (int o=0; o<_nout; ++o) {
        _b[o][k] -= _outScr[o] * _inScr[k];
      }
    }
  }

  /** For a given set of input coordinates,
      return the linearly predicted output coordinates.
      @param in A set of input coordinates
      @return A computed set of output coordinates.
  */
  public double[] get(double[] in) {
    for (int i=0; i<in.length; ++i) {_inScr[i] = in[i] - _in0[i];}
    in = null;

    if (_a == null) {
      _a = new double[_nout][_nin];
      for (int o=0; o <_nout; ++o) {
        LinearQuadratic lq = new LinearQuadratic(o);
        QuadraticSolver qs = new QuadraticSolver(lq);
        ArrayVect1 solution = (ArrayVect1) qs.solve(_nin+4, null);
        double[] data = solution.getData();
        for (int i=0; i< _nin; ++i) {
          _a[o][i] = data[i];
        }
        solution.dispose();
      }
    }
    double[] result = new double[_nout];
    for (int o=0; o<_nout; ++o) {
      for (int i=0; i<_nin; ++i) {
        result[o] += _a[o][i]*_inScr[i];
      }
    }
    for (int i=0; i<result.length; ++i) {result[i] = result[i] + _out0[i];}
    return result;
  }

  // describes normal equations.
  private class LinearQuadratic implements Quadratic {
    int _o = -1;

    /** Constructor for normal equations
        @param o Index of output dimension
    */
    public LinearQuadratic (int o) {_o = o;}

    public void multiplyHessian(Vect x) {
      ArrayVect1 m = (ArrayVect1) x;
      double[] data = m.getData();
      double[] oldData = data.clone();
      java.util.Arrays.fill(data,0.);
      for (int i=0; i< data.length; ++i) {
        for (int j=0; j< data.length; ++j) {
          data[i] += _hessian[i][j]*oldData[j];
        }
      }
    }

    public Vect getB() {
      return new ArrayVect1(_b[_o].clone(), 1.);
    }

    public void inverseHessian(Vect x) {} // not necessary
  }

  /** run tests
   * @param args command line
   * @throws Exception test failures
   */
  public static void main(String[] args) throws Exception {
    double[][] in = new double[][] {
      {1.,1.},
      {2.,2.},
      {3.,4.},
    };
    double[][] out = new double[][] {
      {2.},
      {4.},
      {7.},
    };
    double[][] inCopy = ArrayUtil.copy(in);
    double[][] outCopy = ArrayUtil.copy(out);

    CoordinateTransform ls = new CoordinateTransform(1, 2);
    for (int j=0; j<out.length; ++j) {
      ls.add(out[j], in[j]);
    }
    Almost almost = new Almost();
    double a, b;

    a = 1.; b = 1.;
    assert almost.equal(a+b, ls.get(new double[]{a,b})[0]):
      a+"+"+b+"!="+ls.get(new double[]{a,b})[0] ;

    a = 2.; b = 2.;
    assert almost.equal(a+b, ls.get(new double[]{a,b})[0]) :
      a+"+"+b+"!="+ls.get(new double[]{a,b})[0];

    a = 3.; b = 4.;
    assert almost.equal(a+b, ls.get(new double[]{a,b})[0]) :
      a+"+"+b+"!="+ls.get(new double[]{a,b})[0];

    a = 1.; b = 3.;
    assert almost.equal(a+b, ls.get(new double[]{a,b})[0]) :
      a+"+"+b+"!="+ls.get(new double[]{a,b})[0];

    a = 3.; b = 7;
    assert almost.equal(a+b, ls.get(new double[]{a,b})[0]) :
      a+"+"+b+"!="+ls.get(new double[]{a,b})[0];

    assert Arrays.deepEquals(in,inCopy) :
      Arrays.deepToString(in)+" "+Arrays.deepToString(inCopy);
    assert Arrays.deepEquals(out,outCopy):
      Arrays.deepToString(out)+" "+Arrays.deepToString(outCopy);
  }
}
