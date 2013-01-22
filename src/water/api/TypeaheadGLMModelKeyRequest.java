package water.api;

import hex.GLMSolver.GLMModel;
import water.*;

public class TypeaheadGLMModelKeyRequest  extends TypeaheadKeysRequest {

  public TypeaheadGLMModelKeyRequest() {
    super("Provides a simple JSON array of filtered keys known to the "
        + "current node that are GLMModels at the time of calling.");
  }

  @Override
  protected boolean shouldIncludeKey(Key k) {
    return k.toString().startsWith(GLMModel.KEY_PREFIX);
  }
}