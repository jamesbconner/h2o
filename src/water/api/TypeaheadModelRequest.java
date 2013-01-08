
package water.api;

import hex.rf.Model;
import water.*;

public class TypeaheadModelRequest extends TypeaheadKeysRequest {

  public TypeaheadModelRequest() {
    super("Provides a simple JSON array of filtered keys known to the "
        + "current node that are model keys.");
  }

  @Override
  protected boolean shouldIncludeKey(Key k) {
    Value v = UKV.get(k);
    if( v == null ) return false;
    try {
      v.get(new Model());
      return true;
    } catch( Throwable t ) {
      return false;
    }
  }
}
