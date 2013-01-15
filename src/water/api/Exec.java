package water.api;

import water.Key;
import water.ValueArray;

public class Exec extends Request {
  private final Str _exec = new Str(EXPRESSION);

  @Override
  protected Response serve() {
    String s = _exec.value();
    try {
      Key k = water.exec.Exec.exec(s);
      ValueArray va = ValueArray.value(k);
      return new Inspect(k).serveValueArray(va);
    } catch( Exception e ) {
      return Response.error(e.getMessage());
    }
  }
}
