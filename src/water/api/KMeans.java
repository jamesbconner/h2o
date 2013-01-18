package water.api;

import jsr166y.RecursiveAction;
import water.*;

import com.google.gson.JsonObject;

public class KMeans extends Request {

  protected final H2OHexKey       _source  = new H2OHexKey(SOURCE_KEY);
  protected final Int             _k       = new Int(K);
  protected final Real            _epsilon = new Real(EPSILON, 1e-6);
  protected final HexIgnoreColumnSelect _columns = new HexIgnoreColumnSelect(COLS, _source);
  protected final H2OKey          _dest    = new H2OKey(DEST_KEY, (Key) null);

  @Override
  protected Response serve() {
    final ValueArray va = _source.value();
    final Key source = va._key;
    final int k = _k.value();
    final double epsilon = _epsilon.value();
    final int[] cols = _columns.value();
    Key dest = _dest.value();

    if( dest == null ) {
      String n = source.toString();
      int dot = n.lastIndexOf('.');

      if( dot > 0 )
        n = n.substring(0, dot);

      dest = Key.make(n + ".kmeans");
    }

    try {
      final Key dest_ = dest;
      UKV.put(dest, new hex.KMeans.Res());

      H2O.FJP_NORM.submit(new RecursiveAction() {
        @Override
        protected void compute() {
          hex.KMeans.run(dest_, va, k, epsilon, cols);
        }
      });

      JsonObject response = new JsonObject();
      response.addProperty(RequestStatics.DEST_KEY, dest.toString());

      Response r = KMeansProgress.redirect(response, dest);
      r.setBuilder(RequestStatics.DEST_KEY, new KeyElementBuilder());
      return r;
    } catch( IllegalArgumentException e ) {
      return Response.error(e.getMessage());
    } catch( Error e ) {
      return Response.error(e.getMessage());
    }
  }
}
