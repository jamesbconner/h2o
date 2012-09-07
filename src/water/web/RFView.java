package water.web;

import java.util.Properties;
import water.*;

// @author cliffc
public class RFView extends H2OPage {
  @Override protected String serve_impl(Properties args) {
    Object o = ServletUtil.check_key(args,"Key");
    if( o instanceof String ) return (String)o;
    Key key = (Key)o;
    Value val = DKV.get(key);
    if( val == null )
      return wrap(error("Key not found: "+ key));
    int ntrees = getAsNumber(args,"ntrees", 5);
    int depth = getAsNumber(args,"depth", 30);
    String orig = args.getProperty("origKey");
    Key korig = orig==null?null:Key.make(decode(orig));

    // Get the Tree keys
    byte[] bits = val.get();
    int off = 0;
    int nkeys = UDP.get4(bits,(off+=4)-4);
    Key treekeys[] = new Key[nkeys];
    for( int i=0; i<nkeys; i++ )
      off += (treekeys[i] = Key.read(bits,off)).wire_len();

    RString response = new RString(html);
    response.replace("origKey",korig);
    response.replace("got",nkeys);
    response.replace("ntrees",ntrees);
    response.replace("depth",depth);
    _refresh = nkeys < ntrees ? 5 : 0;

    int limkeys = Math.min(nkeys,100);
    for( int i=0; i<limkeys; i++ ) {
      RString row = response.restartGroup("tableRow");
      row.replace("treekey_u",encode(treekeys[i]._kb));
      row.replace("treekey_s",treekeys[i]);
      row.append();
    }
    return response.toString();
  }
  final static String html = "\nRandom Forest of %origKey\n<p>"
    +"%got of %ntrees trees, depth limit of %depth\n<p>"
    + "<table class='table table-striped table-bordered table-condensed'>"
    + "<tbody>\n"
    + "%tableRow{\n"
    + "  <tr><td><a href='/Inspect?Key=%treekey_u'>%treekey_s</a></tr>\n"
    + "}\n"
    + "</tbody>\n"
    + "</table>\n"
    ;
}
