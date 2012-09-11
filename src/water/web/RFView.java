package water.web;

import hexlytics.rf.Confusion;
import hexlytics.rf.Tree;

import java.util.Properties;

import com.google.gson.JsonObject;

import water.DKV;
import water.Key;

public class RFView extends H2OPage {

  @Override public String[] requiredArguments() {
    return new String[] { "Key" };
  }

  @Override public JsonObject serverJson(Server s, Properties p) throws PageError {
    final int depth = getAsNumber(p,"depth", 30);
    Key key = ServletUtil.check_key(p,"Key");
    Confusion confusion = Confusion.fromKey(key);
    confusion.refresh(); // Refresh it, in case new Trees have appeared

    Key treekeys[] = confusion._treeskey.flatten();
    // Compute a few stats over trees
    final int ntrees = treekeys.length;
    int tdepth=0;
    int tleavs=0;
    for( int i=0; i<ntrees; i++ ) {
      long dl = Tree.depth_leaves(DKV.get(treekeys[i]).get());
      tdepth += (int)(dl>>>32);
      tleavs += (dl&0xFFFFFFFFL);
    }

    JsonObject res = new JsonObject();
    res.addProperty("origKey",encode(confusion._ary._key));
    res.addProperty("got",ntrees);
    res.addProperty("valid",confusion._ntrees);
    res.addProperty("maxtrees",confusion._maxtrees);
    res.addProperty("maxdepth",depth);
    res.addProperty( "depth",(double)tdepth/ntrees);
    res.addProperty("leaves",(double)tleavs/ntrees);
    return res;
  }

  @Override protected String serveImpl(Server s, Properties args) throws PageError {
    final int depth = getAsNumber(args,"depth", 30);
    RString response = new RString(html());

    // Get the Confusion Matrix from the Confusion Key
    Key key = ServletUtil.check_key(args,"Key");
    Confusion confusion = Confusion.fromKey(key);
    confusion.refresh(); // Refresh it, in case new Trees have appeared

    // Display the confusion-matrix table here
    // First the title line
    int cmin = (int)confusion._ary.col_min(confusion._ary.num_cols()-1);
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

    // Get the Tree keys
    Key treekeys[] = confusion._treeskey.flatten();
    final int ntrees = treekeys.length;
    response.replace("origKey",confusion._ary._key);
    response.replace("got",ntrees);
    response.replace("valid",confusion._ntrees);
    response.replace("maxtrees",confusion._maxtrees);
    response.replace("maxdepth",depth);
    _refresh = ntrees < confusion._ntrees ? 5 : 0; // Refresh in 5sec if no keys yet

    // Compute a few stats over trees
    int tdepth=0;
    int tleavs=0;
    for( int i=0; i<ntrees; i++ ) {
      long res = Tree.depth_leaves(DKV.get(treekeys[i]).get());
      tdepth += (int)(res>>>32);
      tleavs += (res&0xFFFFFFFFL);
    }
    response.replace( "depth",(double)tdepth/ntrees);
    response.replace("leaves",(double)tleavs/ntrees);

    int limkeys = Math.min(ntrees,100);
    for( int i=0; i<limkeys; i++ ) {
      RString trow = response.restartGroup("TtableRow");
      trow.replace("treekey_u",treekeys[i]);
      trow.replace("treekey_s",treekeys[i].toString());
      trow.replace("torigKey",confusion._ary._key);
      trow.append();
    }

    return response.toString();
  }

  // use a function instead of a constant so that a debugger can live swap it
  private String html() {
    return "\nRandom Forest of %origKey\n<p>"
      +"Validated %valid trees of %got computed of %maxtrees total trees, depth limit of %maxdepth\n<p>"
      + "<h2>Confusion Matrix</h2>"
      + "<table class='table table-striped table-bordered table-condensed'>"
      + "<thead>%chead</thead>\n"
      + "<tbody>\n"
      + "%CtableRow{<tr>%crow</tr>}\n"
      + "</tbody>\n"
      + "</table>\n"
      + "<p><p>\n"
      + "<h2>Random Decision Trees</h2>"
      + "Avg depth=%depth, Avg leaves=%leaves<p>\n"
      + "<table class='table table-striped table-bordered table-condensed'>"
      + "<tbody>\n"
      + "%TtableRow{\n"
      + "  <tr><td><a href='/RFTreeView?Key=%treekey_u&origKey=%torigKey'>%treekey_s</a></tr>\n"
      + "}\n"
      + "</tbody>\n"
      + "</table>\n"
      ;
  }
}
