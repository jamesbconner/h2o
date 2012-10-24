package water.web;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Properties;

import com.google.gson.*;

import water.DKV;
import water.Key;
import water.Value;
import water.ValueArray;
import water.H2O;

public class Inspect extends H2OPage {

  static DecimalFormat dformat = new DecimalFormat("###.####");

  static String format(double d){
    if(Double.isNaN(d))return "";
    return dformat.format(d);
  }
  public Inspect() {
    // No thanks on the refresh, it's hard to use.
    //_refresh = 5;
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Key" };
  }

  @Override public JsonObject serverJson(Server server, Properties parms, String sessionID) throws PageError {

    Key key = ServletUtil.check_key(parms,"Key");
    Value val = DKV.get(key);
    if( val == null )
      throw new PageError("Key not found: " + key.toString());

    JsonObject result = new JsonObject();
    addProperty(result, "key", key);
    if (val instanceof ValueArray) {
      result.addProperty("type", "ary");
      ValueArray ary = (ValueArray) val;
      result.addProperty("rows", ary.num_rows());
      result.addProperty("cols", ary.num_cols());
      result.addProperty("rowsize",ary.row_size());
      result.addProperty("size",ary.length());
      result.addProperty("priorKey",ary.prior_key().toString());
      JsonArray columns = new JsonArray();
      for( int i=0; i<ary.num_cols(); i++ ) {
        JsonObject col = new JsonObject();
        col.addProperty("name",  ary.col_name(i));
        col.addProperty("off",   ary.col_off(i));
        if (ary.col_has_enum_domain(i)) {
          col.addProperty("type",  "enum");
          JsonArray enums = new JsonArray();
          for (String e : ary.col_enum_domain(i)) {
            enums.add(new JsonPrimitive(e));
          }
          col.add("enumdomain", enums);
        } else {
          col.addProperty("type",  ary.col_size(i) > 0 ? "int" : "float");
        }
        col.addProperty("size",  Math.abs(ary.col_size(i)));
        col.addProperty("base",  ary.col_base(i));
        col.addProperty("scale", ary.col_scale(i));
        col.addProperty("min",   ary.col_min(i));
        col.addProperty("max",   ary.col_max(i));
        col.addProperty("badat", ary.col_badat(i));
        col.addProperty("mean",  ary.col_mean(i));
        col.addProperty("var",  ary.col_sigma(i));
        columns.add(col);
      }
      result.add("columns", columns);
    } else {
      result.addProperty("type", "value");
    }
    return result;
  }

  @Override protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    Key key = ServletUtil.check_key(args,"Key");
    String ks = key.toString();

    // Distributed get
    Value val = DKV.get(key);
    if( val == null )
      return wrap(error("Key not found: "+ ks));

    if( val instanceof ValueArray &&
        ((ValueArray)val).num_cols() > 0 )
      return structured_array(key,(ValueArray)val);

    RString response = new RString(html);

    formatKeyRow(key,val,response);

    response.replace("key",key);

    if(H2O.OPT_ARGS.hdfs != null && !val.onHDFS()){
      RString hdfs = new RString("<a href='Store2HDFS?Key=%$key'><button class='btn btn-primary btn-mini'>store on HDFS</button></a>");
      hdfs.replace("key", key);
      response.replace("storeHdfs", hdfs.toString());
    } else {
      response.replace("storeHdfs", "");
    }

    // ASCII file?  Give option to do a binary parse
    String p_keys = ks;
    int idx = ks.lastIndexOf('.');
    if( idx != -1 )
      p_keys = ks.substring(0,idx);
    p_keys += ".hex";
    if(p_keys.startsWith("hdfs://"))
      p_keys = p_keys.substring(7);
    else if (p_keys.startsWith("nfs:"))
      p_keys = p_keys.substring(4);
    if( p_keys.equals(ks) ) p_keys += "2";

    Key p_key = Key.make(p_keys);
    boolean missed = DKV.get(p_key) == null;

    RString r = new RString(html_parse);
    r.replace("key", missed ? key : p_key);
    r.replace("parseKey", p_key);
    r.replace("pfunc", missed ? "Parse" : "Inspect");
    response.replace("parse", r.toString());

    return response.toString();
  }

  private void formatKeyRow(Key key, Value val, RString response) {
    RString row = response.restartGroup("tableRow");

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

    ServletUtil.createBestEffortSummary(key, row);
  }


  final static String html =
      "<h1><a style='%delBtnStyle' href='RemoveAck?Key=%$key'><button class='btn btn-danger btn-mini'>X</button></a>&nbsp;&nbsp;<a href='/Get?Key=%$key'>%key</a>%execbtn</h1>"
    + "%storeHdfs"
    + "<table class='table table-striped table-bordered table-condensed'>"
    + "<colgroup><col/><col/><col/><col/><col colspan=5 align=center/></colgroup>\n"
    + "<thead><tr><th>    <th>    <th>    <th align=center colspan=5>Min / Average / Max <th>   </tr>\n"
    + "       <tr><th>Size<th>Rows<th>Cols<th>Col 0<th>Col 1<th>Col 2<th>Col 3<th>Col 4<th>Value</tr></thead>\n"
    + "<tbody>\n"
    + "%tableRow{\n"
    + "  <tr>"
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
    + "%parse";

  final static String html_parse =
    "<a href='/%pfunc?Key=%$key&Key2=%$parseKey'>Basic Text-File Parse into %parseKey</a>\n";

  // ---------------------
  // Structured Array / Dataset display

  String structured_array( Key key, ValueArray ary ) {
    RString response = new RString(html_ary);
    if(H2O.OPT_ARGS.hdfs != null && !ary.onHDFS()){
      RString hdfs = new RString("<a href='Store2HDFS?Key=%$key'><button class='btn btn-primary btn-mini'>store on HDFS</button></a>");
      hdfs.replace("key", key);
      response.replace("storeHdfs", hdfs.toString());
    } else {
      response.replace("storeHdfs", "");
    }
    // Pretty-print the key
    response.replace("key",key);
    response.replace("priorKey",ary.prior_key());
    response.replace("size",ary.length());
    response.replace("rows",ary.num_rows());
    response.replace("rowsize",ary.row_size());
    response.replace("ncolumns",ary.num_cols());
    response.replace("xform",ary.xform());

    // Header row
    StringBuilder sb = new StringBuilder();
    final int num_col = Math.min(255,ary.num_cols());
    String[] names = ary.col_names();
    for( int i=0; i<num_col; i++ )
      sb.append("<th>").append(names[i]);
    response.replace("head_row",sb);

    // Data layout scheme
    sb = new StringBuilder();
    for( int i=0; i<num_col; i++ )
      sb.append("<td> +").append(ary.col_off(i)).append("</td>");
    response.replace("offset_row",sb);

    sb = new StringBuilder();
    for( int i=0; i<num_col; i++ )
      sb.append("<td>").append(Math.abs(ary.col_size(i))).append("b</td>");
    response.replace("size_row",sb);
    sb = new StringBuilder();
    for( int i=0; i<num_col; i++ )
      sb.append("<td>").append(format(ary.col_mean(i))).append("</td>");
    response.replace("mean_row",sb);
    sb = new StringBuilder();
    for( int i=0; i<num_col; i++ )
      sb.append("<td>").append(format(ary.col_sigma(i))).append("</td>");
    response.replace("sigma_row",sb);
    // Compression/math function: Ax+B
    sb = new StringBuilder();
    for( int i=0; i<num_col; i++ ) {
      sb.append("<td>");
      int sz = ary.col_size(i);
      if( sz != 0 ) {
        sb.append("(X");
        int base = ary.col_base(i);
        if( base != 0 ) {
          if( base > 0 ) sb.append('+');
          sb.append(base);
        }
        sb.append(")");
        if( sz == 1 || sz == 2 ) {
          int s = ary.col_scale(i);
          if( s != 1.0 ) sb.append("/").append(s);
        }
      }
      sb.append("</td>");
    }
    response.replace("math_row",sb);

    // Min & max
    sb = new StringBuilder();
    for( int i=0; i<num_col; i++ ) {
      sb.append("<td>");
      int sz = ary.col_size(i);
      if( sz != 0 ) {
        double min = ary.col_min(i);
        if( sz > 0 && ary.col_scale(i) == 1 ) sb.append((long)min); else sb.append(min);
        sb.append(" - ");
        double max = ary.col_max(i);
        if( sz > 0 && ary.col_scale(i) == 1 ) sb.append((long)max); else sb.append(max);
      }
      sb.append("</td>");
    }
    response.replace("min_max_row",sb);

    // Missing data
    boolean found=false;
    for( int i=0; i<num_col; i++ )
      if( ary.col_badat(i) != 0 ) {
        found=true;
        break;
      }
    if( found ) {
      RString row = response.restartGroup("tableRow");
      sb = new StringBuilder();
      sb.append("<td>Rows missing data</td>");
      for( int i=0; i<num_col; i++ ) {
        sb.append("<td>");
        int sz = ary.col_badat(i);
        sb.append(sz != 0 ? sz : "");
        sb.append("</td>");
      }
      row.replace("data_row",sb);
      row.append();
    }

    // If we have more than 7 rows, display the first & last 3 rows, else
    // display all the rows.
    long num_rows = ary.num_rows();
    if( num_rows > 7 ) {
      display_row(ary,0,response,num_col);
      display_row(ary,1,response,num_col);
      display_row(ary,2,response,num_col);
      display_row(ary,-1,response,num_col); // Placeholder view
      display_row(ary,num_rows-3,response,num_col);
      display_row(ary,num_rows-2,response,num_col);
      display_row(ary,num_rows-1,response,num_col);
    } else {
      for( int i=0; i<num_rows; i++ )
        display_row(ary,i,response,num_col);
    }

    return response.toString();
  }

  static private void display_row(ValueArray ary, long r, RString response, int num_col) {
    RString row = response.restartGroup("tableRow");
    try {
      StringBuilder sb = new StringBuilder();
      sb.append("<td>Row ").append(r==-1 ? "..." : r).append("</td>");
      for( int i=0; i<num_col; i++ ) {
        if( r == -1 || ary.valid(r,i) ) {
          sb.append("<td>");
          int sz = ary.col_size(i);
          if( sz != 0 ) {
            if( r == -1 ) sb.append("...");
            else {
              if( ary.col_has_enum_domain(i) ) sb.append(ary.col_enum_domain_val(i, (int)ary.data(r,i)));
              else if( ary.col_size(i) >= 0 && ary.col_scale(i)==1 ) sb.append(ary.data(r,i)); // int/long
              else sb.append(ary.datad(r,i)); // float/double
            }
          }
          sb.append("</td>");
        } else {
          sb.append("<td style='background-color:IndianRed'>NA</td>");
        }
      }
      row.replace("data_row",sb);
    } catch( IOException e ) {
      // do not display this row
      row.replace("data_row","<td>Row "+r+"</td><td>IOError</td>");
    }
    row.append();
  }

  final static String html_ary =
      "<h1><a style='%delBtnStyle' href='RemoveAck?Key=%$key'><button class='btn btn-danger btn-mini'>X</button></a>&nbsp;&nbsp;<a href='/Get?Key=%$key'>%key</a>%execbtn</h1>"
    + "%storeHdfs"
    + "<p>Generated from <a href=/Inspect?Key=%$priorKey>%priorKey</a> by '%xform'<p>"
    + "<b><font size=+1>%rowsize Bytes-per-row * %rows Rows = Totalsize %size</font></b></em><br>"
    + "Parsed %ncolumns columns<br>"
    + "<table class='table table-striped table-bordered table-condensed'>"
    + "<thead><tr><th>Column %head_row</tr></thead>\n"
    + "<tbody>\n"
    + "  <tr><td>Record offset</td>%offset_row</tr>\n"
    + "  <tr><td>Column bytes</td>%size_row</tr>\n"
    + "  <tr><td>Internal scaling</td>%math_row</tr>\n"
    + "  <tr><td>Min/Max</td>%min_max_row</tr>\n"
    + "  <tr><td>&mu;</td>%mean_row</tr>\n"
    + "  <tr><td>&sigma;</td>%sigma_row</tr>\n"
    + "%tableRow{\n"
    + "  <tr>%data_row</tr>\n"
    + "}\n"
    + "</tbody>\n"
    + "</table>\n";
}
