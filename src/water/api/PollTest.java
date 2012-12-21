
package water.api;

import com.google.gson.JsonObject;

/**
 *
 * @author peta
 */
public class PollTest extends Request {

  // not thread safe I do not care
  public static int _counter = 0;

  public final Str _haha = new Str("hoho");

  @Override protected Response serve() {
    ++_counter;
    JsonObject resp = new JsonObject();
    resp.addProperty("hoho",_haha.value());
    resp.addProperty("counter",_counter);
    if (_counter == 10) {
      _counter = 0;
      return Response.done(resp);
    } else {
      return Response.poll(resp,_counter,10);
    }
  }

}
