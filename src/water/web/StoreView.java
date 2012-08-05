package water.web;

import java.util.Arrays;
import java.util.Properties;

import water.H2O;
import water.Key;
import water.Value;

/**
 *
 * @author peta
 */
public class StoreView extends H2OPage {
  
  public static final int KEYS_PER_PAGE = 25;

  public StoreView() {
    // No thanks on the refresh, it's hard to use.
    //_refresh = 5;
  }
  
  @Override protected String serve_impl(Properties args) {
    RString response = new RString(html);
    // get the offset index
    int offset = 0;
    try {
      offset = Integer.valueOf(args.getProperty("o", "0"));
    } catch( NumberFormatException e ) { /* pass */ }
    // write the response
    H2O cloud = H2O.CLOUD;         // Current eldest Cloud
    Key[] keys = new Key[1024];    // Limit size of what we'll display on this page
    int len = 0;
    String filter = args.getProperty("Filter");
    String html_filter = (filter==null? "" : "?Filter="+filter);

    // Gather some keys that pass all filters
    for( Key key : H2O.keySet() ) {
      if( filter != null &&     // Have a filter?
          key.toString().indexOf(filter) == -1 )
        continue;               // Ignore this filtered-out key
      if( !key.user_allowed() ) // Also filter out for user-keys
        continue;
      if( H2O.get(key) == null ) continue; // Ignore misses
      keys[len++] = key;        // Capture the key
      if( len == keys.length ) break; // List is full; stop
    }

    // sort the keys, for pretty display & reliable ordering
    Arrays.sort(keys,0,len);
    // Pagination, if the list is long
    formatPagination(offset,len, html_filter, response);
    offset *= KEYS_PER_PAGE;

    for( int i=offset; i<offset+KEYS_PER_PAGE; i++ ) {
      if( i >= len ) break;
      Value val = H2O.get(keys[i]);
      formatKeyRow(cloud,keys[i],val,response);
    }

    response.replace("noOfKeys",len);
    response.replace("cloud_name",H2O.CLOUD.NAME);
    response.replace("node_name",H2O.SELF.toString());
    if( filter!=null )
      response.replace("pvalue","value='"+filter+"'");
    return response.toString();
  }

  private void formatPagination(int offset, int size, String prefix, RString response) {
    if (size<=KEYS_PER_PAGE)
      return;
    StringBuilder sb = new StringBuilder();
    sb.append("<div class='pagination pagination-centered' style='margin:0px auto'><ul>");
    if (offset!=0) {
      sb.append("<li><a href='?o=0"+prefix+"'>First</li>");
      sb.append("<li><a href='?o="+(offset-1)+prefix+"'>&lt;&lt;</a></li>");
    }
    int j = 0;
    int i = offset - 5;
    while (j<10) {
      if (++i<0)
        continue;
      if (i>size/KEYS_PER_PAGE)
        break;
      if (i==offset)
        sb.append("<li class='active'><a href=''>"+i+"</li>");
      else 
        sb.append("<li><a href='?o="+i+prefix+"'>"+i+"</li>");
      ++j;
    }
    if (offset < (size/KEYS_PER_PAGE)) {
      sb.append("<li><a href='?o="+(offset+1)+prefix+"'>&gt;&gt;</a></li>");
      sb.append("<li><a href='?o="+(size/KEYS_PER_PAGE)+prefix+"'>Last</a></li>");
    }
    sb.append("</ul></div>");
    String nav = sb.toString();
    response.replace("navup",nav);
  }
  
  private void formatKeyRow(H2O cloud, Key key, Value val, RString response) {
    RString row = response.restartGroup("tableRow");
    // Dump out the Key
    String ks = key.toString();
    row.replace("keyHref",urlEncode(new String(key._kb)));
    row.replace("key",key.user_allowed() ? ks : "<code>"+key.toString()+"</code>");
    //if (val instanceof ValueCode) {
    //  row.replace("execbtn","&nbsp;&nbsp;<a href='ExecQuery?Key="+urlEncode(key.toString())+"'><button class='btn btn-primary btn-mini'>Execute</button></a>");
    //}
    // Dump out the current replication info: Mem/Disk/Replication_desired
    String vs = val.getString(100); // First, get the string which might force mem loading
    vs = vs.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;");
    vs = vs.replace("\n","<br>");
    int r = key.desired();
    int repl = key.replica(cloud);
    if( repl < r ) { // If we should be replicating, then report what replication we know of
      int d = key.count_disk_replicas();
      if( val.is_persisted() ) d++; // One more for self
      if( d < r )
        row.replace("replicationStyle","background-color:#ffc0c0;color:#ff0000;");
      row.replace("r1",d);
      row.replace("r2",r);
    } else {                // Else not tracking replications, so cannot report
      row.replace("r1","");
      row.replace("r2","");
    }
    row.replace("home",cloud._memary[key.home(cloud)]);
    // Dump out the 2nd replica
    int idx2 = cloud.D(key,1);
    if( idx2 != -1 )
      row.replace("home2",cloud._memary[idx2]);
    row.replace("replica",(repl==255?"":("r"+repl)));
    // Now the first 100 bytes of Value as a String
    row.replace("value",vs);
    row.replace("ktr",urlEncode(ks));
    row.append();
  }

  final static String html =
    "<div class='alert alert-success'>"
    + "You are connected to cloud <strong>%cloud_name</strong> and node <strong>%node_name</strong>."
    + "</div>"
    + "<form class='well form-inline' action='StoreView'>"
    + "  <input type='text' class='input-small span10' placeholder='filter prefix' name='Filter' id='Filter' %pvalue maxlength='512'>"
    + "  <button type='submit' class='btn btn-primary'>Filter keys!</button>"
    + "</form>"
    + "<p>Displaying %noOfKeys keys"
    + "<p>%navup</p>"
    + "<table class='table table-striped table-bordered table-condensed'>"
    + "<colgroup><col/><col/><col style=\"text-align:center\"/><col/></colgroup>\n"
    + "<thead><th>Key<th>D/R<th>1st<th>2nd<th>replica#<th>Value</thead>\n"
    + "<tbody>"
    + "%tableRow{"
    + "  <tr>"
    + "    <td><a style='%delBtnStyle' href='RemoveAck?Key=%ktr'><button class='btn btn-danger btn-mini'>X</button></a>&nbsp;&nbsp;<a href='/Get?Key=%keyHref'>%key</a>%execbtn</td>"
    + "    <td style='%replicationStyle'>%r1/%r2</td>"
    + "    <td>%home</td>"
    + "    <td>%home2</td>"
    + "    <td>%replica</td>"
    + "    <td>%value</td>"
    + "  </tr>\n"
    + "}"
    + "</tbody>"
    + "</table>\n"
    ;
}
