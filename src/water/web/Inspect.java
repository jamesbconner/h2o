package water.web;
import java.io.IOException;
import java.util.Properties;
import water.*;
import water.csv.ValueCSVRecords;
import water.csv.CSVParser.*;

/**
 *
 * @author cliffc@
 */
public class Inspect extends H2OPage {
  
  public Inspect() {
    // No thanks on the refresh, it's hard to use.
    //_refresh = 5;
  }
  
  @Override protected String serve_impl(Properties args) {
    String key_s = args.getProperty("Key");
    Key key = null;
    try { 
      key = Key.make(key_s);      // Get a Key from a raw byte array, if any
    } catch( IllegalArgumentException e ) {
      return H2OPage.wrap(H2OPage.error("Not a valid key: "+ key_s));
    }
    if (!key.user_allowed())
      return H2OPage.wrap(H2OPage.error("Not a user key: "+ key_s));
    // Distributed get
    Value val = DKV.get(key);

    if( val == null )
      return H2OPage.wrap(H2OPage.error("Key not found: "+ key_s));

    RString response = new RString(html);

    formatKeyRow(key,val,response);

    // Dump out the Key
    String ks = key.toString();
    response.replace("keyHref",urlEncode(new String(key._kb)));
    response.replace("key",ks);
    response.replace("ktr",urlEncode(ks));

    // ASCII file?  Give option to do a binary parse
    if( !(val instanceof ValueArray) || ((ValueArray)val).num_cols() == 0 ) {
      String s = html_parse.replace("%keyHref",urlEncode(key_s));
      String p_key = key_s;
      int idx = key_s.lastIndexOf('.');
      if( idx != -1 )
        p_key = key_s.substring(0,idx);
      p_key += ".dat";
      if( p_key.equals(key_s) ) p_key += "2";
      s = s.replace("%parsekey",urlEncode(p_key));
      response.replace("parse",s);
    }

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

    // Do an initial parse of the 1st meg of the dataset
    try {
      float[] fs = new float[100]; // First few columns only
      CSVParserSetup setup = new CSVParserSetup();
      setup._parseColumnNames = false;
      setup._partialRecordPolicy = CSVParserSetup.PartialRecordPolicy.fillWithDefaults;
      setup._ignoreAdditionalColumns = true;
      ValueCSVRecords<float[]> csv = new ValueCSVRecords(key,1,fs,null,setup);
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
    } catch( NoSuchFieldException nsfe ) {
      System.out.println("NoSuchFieldEx thrown");
    } catch( SecurityException se ) {
      System.out.println("SecurityException thrown");
    } catch( IllegalArgumentException iae ) {
      System.out.println("IllegalArgumentException thrown");
    } catch( IllegalAccessException iae ) {
      System.out.println("IllegalAccessException thrown");
    } catch( CSVParseException cpe ) {
      System.out.println("CSVParseException thrown");
      cpe.printStackTrace();
    } catch( IOException ie ) {
      System.out.println("IOException thrown");
    }

    row.append();
  }

  final static String html =
      "<h1><a style='%delBtnStyle' href='RemoveAck?Key=%ktr'><button class='btn btn-danger btn-mini'>X</button></a>&nbsp;&nbsp;<a href='/Get?Key=%keyHref'>%key</a>%execbtn</h1>"
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
    "<a href='/Parse?Key=%keyHref&Key2=%parsekey'>Basic Text-File Parse into %parsekey</a>\n";
}
