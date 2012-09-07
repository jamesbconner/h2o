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

    // Get the Tree keys
    byte[] bits = val.get();
    int off = 0;
    int nkeys = UDP.get4(bits,(off+=4)-4);
    Key treekeys[] = new Key[nkeys];
    for( int i=0; i<nkeys; i++ )
      off += (treekeys[i] = Key.read(bits,off)).wire_len();

    RString response = new RString(html);
    response.replace("origKey",orig==null?"unknown":orig);
    response.replace("got",nkeys);
    response.replace("ntrees",ntrees);
    response.replace("depth",depth);
    //if( nkeys < ntrees )
    //  response.replace("refresh","<META HTTP-EQUIV='refresh' CONTENT='"+5+"'>");

    int limkeys = Math.min(nkeys,100);
    for( int i=0; i<limkeys; i++ ) {
      RString row = response.restartGroup("tableRow");
      String skey = treekeys[i].toString();
      row.replace("treekey_u",urlEncode(skey));
      row.replace("treekey_s",skey);
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
