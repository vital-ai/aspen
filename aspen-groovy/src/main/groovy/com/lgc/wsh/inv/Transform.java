package com.lgc.wsh.inv;

/** Implement a non-linear transform and its linearizations
    for a non-linear optimization.
*/

public interface Transform {
  /** Non-linear transform: data = f(model).
      @param data Output.  Initial values are ignored.
      @param model Input. Unchanged.
   */
  public void forwardNonlinear(Vect data, VectConst model);

  /** A linearized approximation of the forward transform
      for a small perturbation (model) to a reference model (modelReference).
      The output data must be a linear function of the model perturbation.
      Linearized transform:
        data = F model ~= f(model + modelReference) - f(modelReference)
      [Do not add results to the existing model.]
      @param data Output.  Initial values are ignored.
      @param model Perturbation to reference model.
      @param modelReference The reference model for the linearized operator.
   */
  public void forwardLinearized(Vect data, VectConst model,
                                VectConst modelReference);

  /** The transpose of the linearized approximation of the forward transform
      for a small perturbation (model) to a reference model (modelReference):
      model = F' data.  Add the result to the existing model.
      [This transpose assumes a simple dot product, without the
      inverse covariance.  I.e. data'F model = F' data model,
      for any arbitrary data or model.]
      @param data Input for transpose operation.
      @param model Output.  The transpose will be added to this vector.
      @param modelReference The reference model for the linearized operator.
   */
  public void addTranspose(VectConst data, Vect model,
                           VectConst modelReference);

  /** To speed convergence multiple a model by an approximate inverse
      Hessian.  An empty implementation is equivalent to an identity
      and is also okay.
      The Hessian is equivalent to multiplying once by the linearized
      forward operation and then by the transpose.  Your approximate
      inverse can greatly speed convergence by trying to diagonalize
      this Hessian, or at least balancing the diagonal.
      If this operation depends only on the model, then you may
      prefer to implement Vect.postCondition() on the model.
      @param model The model to be multiplied.
      @param modelReference The reference model for the linearized operators.
  */
  public void inverseHessian(Vect model, VectConst modelReference) ;

  /** Apply any robust trimming of outliers, or
      scale all errors for an approximate L1 norm when squared.
      This method should do nothing if you want a standard
      least-squares solution.
      Do not change the overall variance of the errors more than necessary.
      @param dataError This is the original data minus the modeled data.
   */
  public void adjustRobustErrors(Vect dataError);
}

