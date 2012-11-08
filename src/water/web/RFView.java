package water.web;
import com.google.gson.JsonObject;
import hex.rf.Confusion;
import hex.rf.Model;
import hex.rf.Tree;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Properties;
import water.*;

public class RFView extends H2OPage {

  @Override public String[] requiredArguments() {
    return new String[] { "dataKey", "modelKey" };
  }

  @Override public JsonObject serverJson(Server s, Properties p, String sessionID) throws PageError {
    // The dataset is required
    ValueArray ary = ServletUtil.check_array(p,"dataKey");

    // The model is required
    final Key modelKey = ServletUtil.check_key(p,"modelKey");
    Model model = UKV.get(modelKey, new Model());
    if( model == null ) throw new PageError("Model key is missing");

    // Class is optional
    int classcol = getAsNumber(p,"class",ary.num_cols()-1);
    if( classcol < 0 || classcol >= ary.num_cols() )
      throw new PageError("Class out of range");

    // Atree & Ntree are optional.
    // Atree - number of trees to display, if not all are available.
    // Ntree - final number of trees that will eventually be built.
    //   0 <= atree <= model.size() <= ntree
    int atree = getAsNumber(p,"atree",0);
    int ntree = getAsNumber(p,"ntree",model.size());

    // Validation is moderately expensive, so do not run validation unless
    // asked-for or all trees are finally available.  "atrees" is the number of
    // trees for which validation has been asked-for.  Only validate up to this
    // limit, unless all trees have finally arrived.
    if( model.size() == ntree ) atree = ntree;


    JsonObject res = new JsonObject();
    addProperty(res, "dataKey", ary._key);
    addProperty(res, "modelKey", modelKey);
    res.addProperty("class",classcol);
    res.addProperty("ntree",ntree); // asked-for trees
    res.addProperty("atree",atree); // displayed trees
    res.addProperty("modelSize",model.size()); // how many we got

    // only create conf matrix if asked for
    if (p.getProperty("noCM","0").equals("0")) {
      // Make or find a C.M. against the model.  If the model has had a prior
      // C.M. run, we'll find it via hashing.  If not, we'll block while we build
      // the C.M.
      Confusion confusion = Confusion.make( model, ary._key, classcol );
      addProperty(res, "confusionKey", confusion.keyFor());
    }
    return res;
  }

  @Override public String serveImpl(Server s, Properties p, String sessionID) throws PageError {
    // Update the Model.
    // Compute the Confusion.
    JsonObject json = serverJson(s, p, sessionID);
    if( json.has("error") )
      return H2OPage.error(json.get("error").toString());

    // The dataset is required
    ValueArray ary = ServletUtil.check_array(p,"dataKey");
    final int classcol = json.get("class").getAsInt();

    // The model is required
    final Key modelKey = ServletUtil.check_key(p,"modelKey");
    Model model = UKV.get(modelKey, new Model());
    int atree = json.get("atree").getAsInt();
    int ntree = json.get("ntree").getAsInt();
    json.addProperty("modelSize",model.size()); // how many we got
    if( model.size() == ntree ) atree = ntree;

    // Since the model has already been run on this dataset (in the serverJson
    // above), and Confusion.make caches - calling it again a quick way to
    // de-serialize the Confusion from the H2O Store.
    Confusion confusion = Confusion.make( model, ary._key, classcol );

    // Display the confusion-matrix table here
    // First the title line
    final int N = model._classes;
    RString response = new RString(html());
    int cmin = (int)ary.col_min(classcol);
    StringBuilder sb = new StringBuilder();
    sb.append("<th>Actual \\ Predicted");
    for( int i=0; i<N; i++ )
      sb.append("<th>").append("class "+(i+cmin));
    sb.append("<th>Error");
    response.replace("chead",sb.toString());

    // Now the confusion-matrix body lines
    long ctots[] = new long[N]; // column totals
    long terrs = 0;
    for( int i=0; i<N; i++ ) {
      RString row = response.restartGroup("CtableRow");
      sb = new StringBuilder();
      sb.append("<td>").append("class "+(i+cmin));
      long tot=0;
      long err=0;
      for( int j=0; j<N; j++ ) {
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
    for( int i=0; i<N; i++ ) {
      ttots += ctots[i];
      sb.append("<td>").append(ctots[i]);
    }
    sb.append("<td>");
    if( ttots != 0 )
      sb.append(String.format("%5.3f = %d / %d",(double)terrs/ttots,terrs,ttots));
    row.replace("crow",sb.toString());
    row.append();

    // Report on the basic model info
    if( atree < model.size() ) {
      RString button = new RString(htmlButton);
      button.replace(json);
      response.replace("validateMore", button.toString());
    } else {
      response.replace("validateMore", "");
    }
    response.replace(json);
    _refresh = model.size() < ntree ? 5 : 0; // Refresh in 5sec if not all trees yet

    // Compute a few stats over trees
    response.replace( "depth",model.depth());
    response.replace("leaves",model.leaves());

    int limkeys = Math.min(model.size(),1000);
    for( int i=0; i<limkeys; i++ ) {
      RString trow = response.restartGroup("trees");
      trow.replace("modelKey",modelKey);
      trow.replace("n",i);
      trow.replace("dataKey",ary._key);
      trow.replace("class",classcol);
      trow.append();
    }

    confusion.report();  // Print on std out...

    try {
      response.replace("validateOther","RFViewQuery?modelKey="+URLEncoder.encode(p.getProperty("modelKey"),"UTF8")+"&class="+p.getProperty("class",""));
    } catch (UnsupportedEncodingException e) {
      // pass
    }

    return response.toString();
  }

  // use a function instead of a constant so that a debugger can live swap it
  private String html() {
    return "\nRandom Forest of <a href='Inspect?Key=%$dataKey'>%dataKey</a>\n"
      +"<table><tbody>"
      + "<tr><td>Showing %atree of %ntree trees, with %modelSize trees built</td></tr>"
      + "<tr><td>%validateMore</td></tr>"
      + "</tbody></table>\n"
      + "<p><a href=\"%validateOther\">Validate another data</a></p>"

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
      + "  <a href='/RFTreeView?modelKey=%$modelKey&n=%n&dataKey=%$dataKey&class=%class'>%n</a> "
      + "}\n"
      ;
  }
  private String htmlButton =
    "<a href='RFView?dataKey=%$dataKey&modelKey=%$modelKey&ntree=%ntree&atree=%modelSize&class=%class'><button class='btn btn-primary btn-mini'>Validate with %modelSize trees</button></a>";
}
