package water.web;

import hexlytics.rf.CodeTreePrinter;
import hexlytics.rf.GraphvizTreePrinter;
import hexlytics.rf.Tree;

import java.io.File;
import java.io.PrintWriter;
import java.util.Properties;

import org.apache.commons.codec.binary.Base64;

import water.DKV;
import water.Key;
import water.ValueArray;

import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;

public class RFTreeView extends H2OPage {
  private static final String DOT_PATH;
  static {
    File f = new File("/usr/local/bin/dot");
    if( !f.exists() ) f = new File("/usr/bin/dot");
    DOT_PATH = f.exists() ? f.getAbsolutePath() : null;
  }

  @Override protected String serve_impl(Properties args) {
    Object o = ServletUtil.check_key(args,"Key");
    if( o instanceof String ) return (String)o;
    Key key = (Key)o;

    Object orig = ServletUtil.check_array(args, "origKey");
    if( orig instanceof String ) return (String)orig;
    ValueArray va = (ValueArray)orig;

    if( DKV.get(key) == null ) return wrap(error("Key not found: "+ key));
    Tree tree = Tree.fromKey(key,null);

    RString response = new RString(html());
    int nodeCount = tree.leaves()*2-1; // funny math: total nodes is always 2xleaves-1
    response.replace("key", key);
    response.replace("nodeCount", nodeCount);
    response.replace("leafCount", tree.leaves());
    response.replace("depth",     tree.depth());

    String graph;
    if( DOT_PATH == null ) {
      graph = "Install <a href=\"http://www.graphviz.org/\">graphviz</a> to " +
      		"see visualizations of small trees<p>";
    } else if( nodeCount < 1000 ) {
      graph = dotRender(va, tree);
    } else {
      graph = "Tree is too large to graph.<p>";
    }
    String code;
    if( nodeCount < 10000 ) {
      code = codeRender(va, tree);
    } else {
      code = "Tree is too large to print pseudo code.<p>";
    }
    return response.toString() + graph + code;
  }

  private String codeRender(ValueArray va, Tree t) {
    try {
      StringBuilder sb = new StringBuilder();
      sb.append("<pre><code>");
      new CodeTreePrinter(sb, va.col_names()).printTree(t);
      sb.append("</code></pre>");
      return sb.toString();
    } catch( Exception e ) {
      return errorRender(e);
    }

  }

  private String dotRender(ValueArray va, Tree t) {
    try {
      RString img = new RString("<img src=\"data:image/svg+xml;base64,%rawImage\" width='80%%' ></img><p>");
      Process exec = Runtime.getRuntime().exec(new String[] { DOT_PATH, "-Tsvg" });
      new GraphvizTreePrinter(exec.getOutputStream(), va.col_names()).printTree(t);
      exec.getOutputStream().close();
      byte[] data = ByteStreams.toByteArray(exec.getInputStream());

      img.replace("rawImage", new String(Base64.encodeBase64(data), "UTF-8"));
      return img.toString();
    } catch( Exception e ) {
      return errorRender(e);
    }
  }

  private String errorRender(Exception e) {
    StringBuilder sb = new StringBuilder();
    sb.append("Error Generating Dot file:\n");
    e.printStackTrace(new PrintWriter(CharStreams.asWriter(sb)));
    return sb.toString();
  }

  //use a function instead of a constant so that a debugger can live swap it
  private String html() {
    return
        "\nTree View of %key\n<p>" +
        "%depth depth with %nodeCount nodes and %leafCount leaves.<p>" +
    "";
  }
}
