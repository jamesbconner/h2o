package water.web;

import hexlytics.rf.Tree;
import hexlytics.rf.TreePrinter;

import java.io.IOException;
import java.util.Properties;
import water.*;

public class RFTreeView extends H2OPage {
  @Override protected String serve_impl(Properties args) {
    Object o = ServletUtil.check_key(args,"Key");
    if( o instanceof String ) return (String)o;

    Key key = (Key)o;
    if( DKV.get(key) == null ) return wrap(error("Key not found: "+ key));
    Tree tree = Tree.fromKey(key);

    RString response = new RString(html());
    int nodeCount = tree._tree.nodes();
    response.replace("key", encode(key));
    response.replace("nodeCount", nodeCount);
    response.replace("leafCount", tree._tree.leaves());
    response.replace("depth",     tree._tree.depth());
    if( nodeCount < 1000 ) {
      RString graph = response.restartGroup("graph");
      StringBuilder sb = new StringBuilder();
      try {
        new TreePrinter(sb).printTree(tree);
      } catch( IOException e ) {
        throw new RuntimeException(e);
      }
      graph.replace("graphviz", sb.toString());
      graph.append();
    }
    return response.toString();
  }

  //use a function instead of a constant so that a debugger can live swap it
  private String html() {
    return
        "\nTree View of %key\n<p>" +
        "%depth depth with %nodeCount nodes and %leafCount leaves.<p>" +
        "%graph{<pre><code>%graphviz</code></pre>}" +
    "";
  }
}
