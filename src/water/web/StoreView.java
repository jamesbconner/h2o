package water.web;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import water.H2O;
import water.Key;
import water.Value;
import water.ValueArray;
import water.csv.CSVParserKV;
import water.csv.ValueCSVRecords;
import water.csv.CSVParser.*;

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

    // Now the first 100 bytes of Value as a String
    byte[] b = new byte[100];   // Amount to read
    int len=0;
    try {
      len = val.openStream().read(b); // Read, which might force loading.
    } catch( IOException e ) {}
    StringBuilder sb = new StringBuilder();
    for( int i=0; i<len; i++ ) {
      byte c = b[i];
      if( c == '&' ) sb.append("&amp;");
      else if( c == '<' ) sb.append("&lt;");
      else if( c == '>' ) sb.append("&gt;");
      else if( c == '\n' ) sb.append("<br>");
      else if( c == ',' && i+1<len && b[i+1]!=' ' )
        sb.append(", ");
      else sb.append((char)c);
    }
    if( val.length() > len ) sb.append("...");
    row.replace("value",sb);
    row.replace("size",val.length());
    row.replace("ktr",urlEncode(ks));

    // See if this is a structured ValueArray.  Report results from a total parse.
    if( val instanceof ValueArray &&
        ((ValueArray)val).num_cols() > 0 ) {
      ValueArray ary = (ValueArray)val;
      row.replace("rows",ary.num_rows());
      int cols = ary.num_cols();
      row.replace("cols",cols);
      for( int i=0; i<Math.min(cols,5); i++ ) {
        sb = new StringBuilder();
        double min = ary.col_min(i);
        if( ary.col_size(i) > 0 && ary.col_scale(i) == 1 ) sb.append((long)min); else sb.append(min);
        sb.append(" / - / ");
        double max = ary.col_max(i);
        if( ary.col_size(i) > 0 && ary.col_scale(i) == 1 ) sb.append((long)max); else sb.append(max);
        row.replace("col"+i,sb);
      }
      row.append();
      return;
    }

    // ---
    // Do an initial parse of the 1st meg of the dataset
    try {
      float[] fs = new float[100]; // First few columns only      
      CSVParserKV.ParserSetup setup = new CSVParserKV.ParserSetup();
      setup.whiteSpaceSeparator = true;
      setup.collapseWhiteSpaceSeparators = true;
      CSVParserKV<float[]> csv = new CSVParserKV<float[]>(key,1,fs,null, setup);
      float sums[] = new float[fs.length];
      float mins[] = new float[fs.length];
      float maxs[] = new float[fs.length];
      for( int i=0; i<fs.length; i++ ) {
        mins[i] = Float.MAX_VALUE;
        maxs[i] = Float.MIN_VALUE;
      }
      int rows = 0;
      int cols = 0;
      for( float[] fs2 : csv ) {
        rows++;
        for( int i=0; i<fs2.length; i++ ) {
          if( Float.isNaN(fs2[i]) )
            break;
          // Skipping any 1st record, try to count columns in the 2nd record
          if( (rows == 2) && i+1>cols ) cols = i+1;
          sums[i] += fs2[i];
          if( fs2[i] < mins[i] ) mins[i] = fs2[i];
          if( fs2[i] > maxs[i] ) maxs[i] = fs2[i];
        }
      }
      // Inject into the HTML
      if( cols > 0 && rows > 0 ) {
        row.replace("rows",rows);
        row.replace("cols",cols);
        for( int i=0; i<Math.min(cols,5); i++ ) {
          String s = String.format("%4.1f / %4.1f / %4.1f",mins[i],sums[i]/rows,maxs[i]);
          row.replace("col"+i,s);
        }
      }
    } catch( SecurityException se ) {
      System.out.println("SecurityException thrown");
    } catch( IllegalArgumentException iae ) {
      System.out.println("IllegalArgumentException thrown");
    }

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
    + "<colgroup><col/><col/><col/><col/><col colspan=5 align=center/></colgroup>\n"
    + "<thead><tr><th> <th> <th> <th> <th align=center colspan=5>Min / Average / Max <th> </tr>\n"
    + "       <tr><th>Key<th>Size<th>Rows parsed<th>Cols<th>Col 0<th>Col 1<th>Col 2<th>Col 3<th>Col 4<th>Value</tr></thead>\n"
    + "<tbody>\n"
    + "%tableRow{\n"
    + "  <tr>"
    + "    <td><a style='%delBtnStyle' href='RemoveAck?Key=%ktr'><button class='btn btn-danger btn-mini'>X</button></a>&nbsp;&nbsp;<a href='/Inspect?Key=%keyHref'>%key</a>%execbtn</td>"
    + "    <td>%size</td>"
    + "    <td>%rows</td>"
    + "    <td>%cols</td>"
    + "    <td>%col0</td>"
    + "    <td>%col1</td>"
    + "    <td>%col2</td>"
    + "    <td>%col3</td>"
    + "    <td>%col4</td>"
    + "    <td>%value</td>"
    + "  </tr>\n"
    + "}\n"
    + "</tbody>\n"
    + "</table>\n"
    ;
}
