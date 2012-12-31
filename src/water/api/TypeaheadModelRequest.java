
package water.api;

import water.H2O;
import water.Key;

public class TypeaheadModelRequest extends TypeaheadKeysRequest {

  public TypeaheadModelRequest() {
    super("Provides a simple JSON array of filtered keys known to the "
        + "current node that are model keys.");
  }

  @Override
  protected boolean shouldIncludeKey(Key k) {
    return H2O.get(k) != null;
    // TODO make sure that we only show model keys, but I would have to create
    // a model key for each key now which would be painfully slow, so I am
    // just returning all keys, leaving the refinement for future generations
  }
}
