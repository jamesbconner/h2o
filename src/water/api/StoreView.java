
package water.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import water.*;
import water.parser.CsvParser;
import water.web.RString;
import water.web.ServletUtil;

/**
 *
 * @author peta
 */
public class StoreView extends Request {

  public static final String JSON_MORE = "more";

  public static final String JSON_NUM_KEYS = "num_keys";

  public static final String JSON_KEY_ROWS = "rows";
  public static final String JSON_KEY_COLS = "cols";
  public static final String JSON_COL_MIN = "min";
  public static final String JSON_COL_AVG = "avg";
  public static final String JSON_COL_MAX = "max";


  protected Str _filter = new Str(JSON_FILTER, "");
  protected final Int _offset = new Int(JSON_OFFSET,0,0,1024);
  protected final Int _view = new Int(JSON_VIEW, 20, 0, 1024);

  @Override protected Response serve() {
    JsonObject result = new JsonObject();
    // get the offset index
    int offset = _offset.value();
    // write the response
    H2O cloud = H2O.CLOUD; // Current eldest Cloud
    Key[] keys = new Key[_view.value()]; // Limit size of what we'll display on this page
    int len = 0;
    String filter = _filter.value();
    // Gather some keys that pass all filters
    for( Key key : H2O.keySet() ) {
      if( filter != null && // Have a filter?
          key.toString().indexOf(filter) == -1 )
        continue; // Ignore this filtered-out key
      if( !key.user_allowed() ) // Also filter out for user-keys
        continue;
      if( H2O.get(key) == null ) continue; // Ignore misses
      keys[len++] = key; // Capture the key
      if( len == keys.length ) {
        // List is full; stop
        result.addProperty(JSON_MORE,true);
        break;
      }
    }
    // sort the keys, for pretty display & reliable ordering
    Arrays.sort(keys,0,len);
    if ((offset>=len) && (offset != 0))
      return Response.error("Only "+len+" keys available");
    JsonArray ary = new JsonArray();
    for( int i=offset; i<offset+_view.value(); i++ ) {
      if( i >= len ) break;
      Value val = H2O.get(keys[i]);
      ary.add(formatKeyRow(cloud,keys[i],val));
    }

    result.add(JSON_KEYS,ary);
    result.addProperty(JSON_NUM_KEYS, len);
    result.addProperty(JSON_CLOUD_NAME, H2O.NAME);
    result.addProperty(JSON_NODE_NAME, H2O.SELF.toString());
    Response r = Response.done(result);
    r.setBuilder(JSON_KEYS, new PaginatedTable(argumentsToJson(),offset,_view.value(), len, false));
    r.setBuilder(JSON_KEYS+"."+JSON_KEY, new KeyCellBuilder());
    r.setBuilder(JSON_KEYS+".col_0", new KeyMinAvgMaxBuilder());
    r.setBuilder(JSON_KEYS+".col_1", new KeyMinAvgMaxBuilder());
    r.setBuilder(JSON_KEYS+".col_2", new KeyMinAvgMaxBuilder());
    r.setBuilder(JSON_KEYS+".col_3", new KeyMinAvgMaxBuilder());
    r.setBuilder(JSON_KEYS+".col_4", new KeyMinAvgMaxBuilder());
    return r;
  }

  @Override protected String buildResponseHeader(Response response) {
    StringBuilder sb = new StringBuilder();
    sb.append(super.buildResponseHeader(response));
    sb.append("<form class='well form-inline' action='StoreView.html'>");
    sb.append(" <input type='text' class='input-small span10' placeholder='filter' name='filter' id='filter' value='"+_filter.value()+"' maxlength='512'>");
    sb.append(" <button type='submit' class='btn btn-primary'>Filter keys!</button>");
    sb.append("</form>");
    return sb.toString();
  }



  public class KeyCellBuilder extends ArrayRowElementBuilder {
    @Override public String elementToString(JsonElement element) {
      String str = element.getAsString();
      try {
        return "<a href='Inspect.html?key="+URLEncoder.encode(str,"UTF-8")+"'>"+str+"</a>";
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public class KeyMinAvgMaxBuilder extends ArrayRowElementBuilder {
    @Override public String objectToString(JsonObject element) {
      if (!element.has(JSON_COL_MIN))
        return "";
      return element.get(JSON_COL_MIN).getAsString() + " / "
             + element.get(JSON_COL_AVG).getAsString() + " / "
             + element.get(JSON_COL_MAX).getAsString();
    }
  }

  private JsonObject formatKeyRow(H2O cloud, Key key, Value val) {
    JsonObject result = new JsonObject();
    result.addProperty(JSON_KEY, key.toString());
    result.addProperty(JSON_VALUE_SIZE,val.length());

    createBestEffortSummary(key, result, val.length());

    // See if this is a structured ValueArray. Report results from a total parse.
    if( val._isArray != 0 ) {
      ValueArray ary = ValueArray.value(val);
      if( ary._cols.length > 1 || ary._cols[0]._size != 1 ) {
        result.addProperty(JSON_KEY_ROWS,ary._numrows);
        int cols = ary._cols.length;
        result.addProperty(JSON_KEY_COLS,cols);
        for (int i = 0; i < 5; ++i) {
          JsonObject col = new JsonObject();
          if (i <= cols) {
            ValueArray.Column c = ary._cols[i];
            if (c._size!=0) {
              col.addProperty(JSON_COL_MIN, c._min);
              col.addProperty(JSON_COL_AVG, c._mean);
              col.addProperty(JSON_COL_MAX, c._max);
            }
          }
          result.add("col_"+i,col);
        }
      }
    } else {
      JsonObject col = new JsonObject();
      result.add("col_0",col);
      result.add("col_1",col);
      result.add("col_2",col);
      result.add("col_3",col);
      result.add("col_4",col);
    }

    // Now the first 100 bytes of Value as a String
    byte[] b = new byte[100]; // Amount to read
    int len=0;
    try {
      len = val.openStream().read(b); // Read, which might force loading.
    } catch( IOException e ) {}
    StringBuilder sb = new StringBuilder();
    int newlines=0;
    for( int i=0; i<len; i++ ) {
      byte c = b[i];
      if( c == '&' ) sb.append("&amp;");
      else if( c == '<' ) sb.append("&lt;");
      else if( c == '>' ) sb.append("&gt;");
      else if( c == '\n' ) { sb.append("<br>"); if( newlines++ > 5 ) break; }
      else if( c == ',' && i+1<len && b[i+1]!=' ' )
        sb.append(", ");
      else sb.append((char)c);
    }
    if( val.length() > len ) sb.append("...");
    result.addProperty(JSON_VALUE,sb.toString());

    return result;
  }

  // TODO maybe put to helpers???
  public static void createBestEffortSummary(Key key, JsonObject row, long len) {
    final int maxCols = 100;
    // Guess any separator
    byte[] bs = DKV.get(key).getFirstBytes();
    int[] rows_cols = CsvParser.inspect(bs);
    // Inject into the HTML
    if (rows_cols != null) {
      int rows = rows_cols[0];  // Rows in this first bit of data
      double bytes_per_row = (double)bs.length/rows_cols[0]; // Estimated bytes/row
      row.addProperty(JSON_KEY_ROWS,(long)((double)len/bytes_per_row));
      row.addProperty(JSON_KEY_COLS,rows_cols[1]);
    }
  }

}
