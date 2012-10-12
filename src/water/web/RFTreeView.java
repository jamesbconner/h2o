package water.web;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import hex.rf.*;
import java.io.File;
import java.io.PrintWriter;
import java.util.Properties;
import org.apache.commons.codec.binary.Base64;
import water.*;

public class RFTreeView extends H2OPage {
  private static final String DOT_PATH;
  static {
    File f = new File("/usr/local/bin/dot");
    if( !f.exists() ) f = new File("/usr/bin/dot");
    if( !f.exists() ) f = new File("C:\\Program Files (x86)\\Graphviz 2.28\\bin\\dot.exe");
    DOT_PATH = f.exists() ? f.getAbsolutePath() : null;
  }

  @Override public String[] requiredArguments() {
    return new String[] { "modelKey" };
  }

  @Override protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    Key modelKey = ServletUtil.check_key(args,"modelKey");
    final Value modelVal = UKV.get(modelKey);
    if( modelVal == null ) throw new PageError("Model key is missing");
    Model model = new Model();
    model.read(new Stream(modelVal.get()));

    // Which tree?
    final int n = getAsNumber(args,"n",0);
    if( !(0 <= n && n < model.size()) ) return wrap(error("Tree number of out bounds"));

    byte[] tbits = model._trees[n];

    long dl = Tree.depth_leaves(tbits);
    int depth = (int)(dl>>>32);
    int leaves= (int)(dl&0xFFFFFFFFL);

    RString response = new RString(html());
    int nodeCount = leaves*2-1; // funny math: total nodes is always 2xleaves-1
    response.replace("modelKey", modelKey);
    response.replace("n", n);
    response.replace("nodeCount", nodeCount);
    response.replace("leafCount", leaves);
    response.replace("depth",     depth);

    ValueArray ary = ServletUtil.check_array(args,"dataKey");
    int clz = getAsNumber(args,"class",ary.num_cols()-1);
    String[]names = ary.col_names();
    String[]clz_names = ary.col_enum_domain(clz);

    String graph;
    if( DOT_PATH == null ) {
      graph = "Install <a href=\"http://www.graphviz.org/\">graphviz</a> to " +
      		"see visualizations of small trees<p>";
    } else if( nodeCount < 1000 ) {
      graph = dotRender(names, clz_names, tbits);
    } else {
      graph = "Tree is too large to graph.<p>";
    }
    String code;
    if( nodeCount < 10000 ) {
      code = codeRender(names, clz_names, tbits);
    } else {
      code = "Tree is too large to print pseudo code.<p>";
    }
    return response.toString() + graph + code;
  }

  private String codeRender(String[] col_names, String[] clz_names, byte[] tbits) {
    try {
      StringBuilder sb = new StringBuilder();
      sb.append("<pre><code>");
      new CodeTreePrinter(sb, col_names, clz_names).walk_serialized_tree(tbits);
      sb.append("</code></pre>");
      return sb.toString();
    } catch( Exception e ) {
      return errorRender(e);
    }
  }

  private String dotRender(String[] col_names, String[] clz_names, byte[] tbits) {
    try {
      RString img = new RString("<img src=\"data:image/svg+xml;base64,%rawImage\" width='80%%' ></img><p>");
      Process exec = Runtime.getRuntime().exec(new String[] { DOT_PATH, "-Tsvg" });
      new GraphvizTreePrinter(exec.getOutputStream(), col_names, clz_names).walk_serialized_tree(tbits);
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
        "\n%modelKey view of Tree %n\n<p>" +
        "%depth depth with %nodeCount nodes and %leafCount leaves.<p>" +
    "";
  }
}
