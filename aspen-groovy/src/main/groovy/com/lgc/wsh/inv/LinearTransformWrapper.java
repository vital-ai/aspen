package com.lgc.wsh.inv;

/** Wrap a LinearTransform as a non-linear Transform,
    by ignoring reference model.
*/
public class LinearTransformWrapper implements Transform {
  private LinearTransform _linearTransform = null;

  /** Constructor.
      @param linearTransform Wrap this as a general Transform
  */
  public LinearTransformWrapper(LinearTransform linearTransform) {
    _linearTransform = linearTransform;
  }

  public void forwardNonlinear(Vect data, VectConst model) {
    _linearTransform.forward(data, model);
  }

  public void forwardLinearized(Vect data,
                                VectConst model,
                                VectConst modelReference) {
    _linearTransform.forward(data, model);
  }

  public void addTranspose(VectConst data,
                           Vect model,
                           VectConst modelReference) {
    _linearTransform.addTranspose(data, model);
  }

  public void inverseHessian(Vect model, VectConst modelReference) {
    _linearTransform.inverseHessian(model);
  }

  public void adjustRobustErrors(Vect dataError) {
    _linearTransform.adjustRobustErrors(dataError);
  }
}
