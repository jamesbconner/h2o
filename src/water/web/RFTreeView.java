package water.web;

import hexlytics.rf.Tree;

import java.util.Properties;
import water.*;

public class RFTreeView extends H2OPage {
  @Override protected String serve_impl(Properties args) {
    Object o = ServletUtil.check_key(args,"Key");
    if( o instanceof String ) return (String)o;

    Key key = (Key)o;
    if( DKV.get(key) == null ) return wrap(error("Key not found: "+ key));
    Tree tree = Tree.fromKey(key);

    RString response = new RString(html);
    response.replace("key", encode(key));
    response.replace("nodeCount", tree._tree.nodes());
    response.replace("leafCount", tree._tree.leaves());
    response.replace("depth",     tree._tree.depth());
    return response.toString();
  }
  final static String html =
      "\nTree View of %key\n<p>"
    + "%depth depth with %nodeCount nodes and %leafCount leaves."
    ;
}
