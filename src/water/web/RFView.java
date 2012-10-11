package water.web;
import com.google.gson.JsonObject;
import hex.rf.Confusion;
import hex.rf.Model;
import hex.rf.Tree;
import java.util.Properties;
import water.*;

public class RFView extends H2OPage {

  @Override public String[] requiredArguments() {
    return new String[] { "dataKey", "modelKey" };
  }

  @Override public JsonObject serverJson(Server s, Properties p, String sessionID) throws PageError {
    //final int depth = getAsNumber(p,"depth", 30);
    //Key key = ServletUtil.check_key(p,"Key");
    //Confusion confusion = Confusion.fromKey(key);
    //confusion.refresh(); // Refresh it, in case new Trees have appeared
    //
    //Key treekeys[] = confusion._treeskey.flatten();
    //// Compute a few stats over trees
    //final int ntrees = treekeys.length;
    //int tdepth=0;
    //int tleavs=0;
    //for( int i=0; i<ntrees; i++ ) {
    //  long dl = Tree.depth_leaves(DKV.get(treekeys[i]).get());
    //  tdepth += (int)(dl>>>32);
    //  tleavs += (dl&0xFFFFFFFFL);
    //}
    //
    //JsonObject res = new JsonObject();
    //addProperty(res, "origKey", confusion._data._key);
    //res.addProperty("got",ntrees);
    //res.addProperty("valid",confusion._model.size());
    //res.addProperty("computedTrees",confusion._model.size());
    //res.addProperty("maxdepth",depth);
    //res.addProperty( "depth",(double)tdepth/ntrees);
    //res.addProperty("leaves",(double)tleavs/ntrees);
    //return res;
    return null;
  }

  @Override protected String serveImpl(Server s, Properties args, String sessionID) throws PageError {
    RString response = new RString(html());

    // The dataset is required
    ValueArray ary = ServletUtil.check_array(args,"dataKey");

    // The model is required
    final Key modelKey = ServletUtil.check_key(args,"modelKey");
    final Value modelVal = UKV.get(modelKey);
    if( modelVal == null ) throw new PageError("Model key is missing");
    Model model = new Model();
    model.read(new Stream(modelVal.get()));

    // Ntrees & treeskey are optional.
    final int ntrees = getAsNumber(args,"ntrees",model.size());
    String skey = args.getProperty("treesKey");
    Key treeskey = null;
    try {
      treeskey = H2OPage.decode(skey);
    } catch( IllegalArgumentException e ) {
      throw new PageError("Not a valid key: "+ skey);
    }

    // If we have a treesKey, update the model (as required) until all ntrees
    // have appeared based on the available trees.  If we don't have a treeskey
    // then just display the model as-is.
    if( treeskey != null ) {
      Key[] trees = treeskey.flatten();
      if( model.size() < trees.length ) {
        model = new Model(modelKey,treeskey,model._features,model._classes);
        UKV.put(modelKey,model);
      }
    }

    // Blocks here, until classification of all trees against all data is complete.
    Confusion confusion = Confusion.make( model, ary._key );

    // Display the confusion-matrix table here
    // First the title line
    int cmin = (int)ary.col_min(ary.num_cols()-1);
    StringBuilder sb = new StringBuilder();
    sb.append("<th>Actual \\ Predicted");
    for( int i=0; i<confusion._N; i++ )
      sb.append("<th>").append("class "+(i+cmin));
    sb.append("<th>Error");
    response.replace("chead",sb.toString());

    // Now the confusion-matrix body lines
    long ctots[] = new long[confusion._N]; // column totals
    long terrs = 0;
    for( int i=0; i<confusion._N; i++ ) {
      RString row = response.restartGroup("CtableRow");
      sb = new StringBuilder();
      sb.append("<td>").append("class "+(i+cmin));
      long tot=0;
      long err=0;
      for( int j=0; j<confusion._N; j++ ) {
        long v = confusion._matrix==null ? 0 : confusion._matrix[i][j];
        tot += v;               // Line totals
        ctots[j] += v;          // Column totals
        if( i==j ) sb.append("<td style='background-color:LightGreen'>");
        else { sb.append("<td>"); err += v; }
        sb.append(v);
      }
      terrs += err;             // Total errors
      sb.append("<td>");
      if( tot != 0 )
        sb.append(String.format("%5.3f = %d / %d",(double)err/tot,err,tot));
      row.replace("crow",sb.toString());
      row.append();
    }
    // Last the summary line
    RString row = response.restartGroup("CtableRow");
    sb = new StringBuilder();
    sb.append("<td>").append("Totals");
    long ttots= 0;
    for( int i=0; i<confusion._N; i++ ) {
      ttots += ctots[i];
      sb.append("<td>").append(ctots[i]);
    }
    sb.append(String.format("<td>%5.3f = %d / %d",(double)terrs/ttots,terrs,ttots));
    row.replace("crow",sb.toString());
    row.append();

    // Report on the basic model info
    response.replace("origKey",ary._key);
    response.replace("numtrees",model.size());
    _refresh = model.size() < ntrees ? 5 : 0; // Refresh in 5sec if not all trees yet

    // Compute a few stats over trees
    response.replace( "depth",model.depth());
    response.replace("leaves",model.leaves());

    int limkeys = Math.min(ntrees,100);
    for( int i=0; i<limkeys; i++ ) {
      RString trow = response.restartGroup("trees");
      trow.replace("modelKey",modelKey);
      trow.replace("n",i);
      trow.append();
    }

    return response.toString();
  }

  // use a function instead of a constant so that a debugger can live swap it
  private String html() {
    return "\nRandom Forest of <a href='Inspect?Key=%origKeyHref'>%origKey</a>, %numtrees trees\n<p>"
      + "<h2>Confusion Matrix</h2>"
      + "<table class='table table-striped table-bordered table-condensed'>"
      + "<thead>%chead</thead>\n"
      + "<tbody>\n"
      + "%CtableRow{<tr>%crow</tr>}\n"
      + "</tbody>\n"
      + "</table>\n"
      + "<p><p>\n"
      + "<h2>Random Decision Trees</h2>"
      + "min/avg/max depth=%depth, leaves=%leaves<p>\n"
      + "Click to view individual trees:<p>"
      + "%trees{\n"
      + "  <a href='/RFTreeView?modelKey=%modelKeyHref&n=%n'>%n</a> "
      + "}\n"
      ;
  }
}
