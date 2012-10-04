package water.web;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import init.Boot;
import java.io.*;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import water.H2O;
import water.NanoHTTPD;
import water.web.Page.PageError;

/** This is a simple web server. */
public class Server extends NanoHTTPD {

  // cache of all loaded resources
  private static final ConcurrentHashMap<String,byte[]> _cache = new ConcurrentHashMap();
  private static final HashMap<String,Page> _pages = new HashMap();

  // initialization ------------------------------------------------------------
  static {
    // initialize pages
    _pages.put("",new Cloud());
    _pages.put("Cloud",new Cloud());
    _pages.put("COV",new Covariance());
    _pages.put("Covariance",new Covariance());
    _pages.put("cor",new Covariance());
    _pages.put("cov",new Covariance());
    _pages.put("var",new Covariance());
    _pages.put("DebugView",new DebugView());
    _pages.put("Get",new Get());
    _pages.put("GetQuery",new GetQuery());
    _pages.put("ImportFolder",new ImportFolder());
    _pages.put("ImportQuery",new ImportQuery());
    _pages.put("ImportUrl",new ImportUrl());
    _pages.put("Inspect",new Inspect());
    _pages.put("LR",new LinearRegression());
    _pages.put("LinearRegression",new LogisticRegression());
    _pages.put("LGR",new LogisticRegression());
    _pages.put("GLM",new GLM());
    _pages.put("LogisticRegression",new LogisticRegression());
    _pages.put("Network",new Network());
    _pages.put("NodeShuffle",new NodeShuffle());
    _pages.put("Parse",new Parse());
    _pages.put("PR",new ProgressReport());
    _pages.put("ProgressReport",new ProgressReport());
    _pages.put("ProgressView",new ProgressView());
    _pages.put("PutFile",new PutFile());
    _pages.put("Put",new PutQuery());
    _pages.put("PutValue",new PutValue());
    _pages.put("RFView",new RFView()); // View random-forest output
    _pages.put("RFTreeView",new RFTreeView());
    _pages.put("RF",new RandomForestPage());
    _pages.put("RandomForest",new RandomForestPage());
    _pages.put("Remote",new Remote());
    _pages.put("Remove",new Remove());
    _pages.put("RemoveAck",new RemoveAck());
    _pages.put("Shutdown",new Shutdown());
    _pages.put("StoreView",new StoreView());
    _pages.put("Test",new Test());
    _pages.put("Timeline",new Timeline());
    _pages.put("Store2HDFS",new Store2HDFS());
  }


  // Keep spinning until we get to launch the NanoHTTPD
  public static void start() {
    new Thread( new Runnable() {
        public void run()  {
          while( true ) {
            try {
              // Try to get the NanoHTTP daemon started
              new Server(H2O.WEB_PORT);
              break;
            } catch ( Exception ioe ) {
              System.err.println("Launching NanoHTTP server got "+ioe);
              try { Thread.sleep(1000); } catch( InterruptedException e ) { } // prevent denial-of-service
            }
          }
        }
      }).start();
  }

  // uri serve -----------------------------------------------------------------
  @Override public Response serve( String uri, String method, Properties header, Properties parms, Properties files ) {
    if (uri.isEmpty()) uri = "/";

    Page page = _pages.get(uri.substring(1));
    boolean json = uri.endsWith(".json");
    if (json && page == null) page = _pages.get(uri.substring(1, uri.length()-5));

    String mime = json ? MIME_JSON : MIME_HTML;

    // if we cannot handle it, then it might be a resource
    if (page==null) return getResource(uri);

    // unify GET and POST arguments
    parms.putAll(files);
    // check that required arguments are present
    String[] reqArgs = page.requiredArguments();
    if (reqArgs!=null) {
      for (String s : reqArgs) {
        if (!parms.containsKey(s) || parms.getProperty(s).isEmpty()) {
          if (json) return new Response(HTTP_BADREQUEST, mime, "{}");
          return new Response(HTTP_OK, mime,
              H2OPage.wrap(H2OPage.error("Not all required parameters were supplied to page <strong>"+uri+"</strong><br/>Argument <strong>"+s+"</strong> is missing.")));
        }
      }
    }
    Object result;
    try {
      result = json ? page.serverJson(this,parms) : page.serve(this,parms);
    } catch( PageError e ) {
      result = e._msg;
    }
    if (result == null) return http404(uri);
    if (result instanceof Response) return (Response)result;
    if (result instanceof InputStream)
      return new Response(NanoHTTPD.HTTP_OK, mime, (InputStream) result);
    return new Response(NanoHTTPD.HTTP_OK, mime, result.toString());
  }

  public static Page getPage(String uri) {
    return _pages.get(uri);
  }

  private Server( int port ) throws IOException {
    super(port,null);
  }

  // Resource loading ----------------------------------------------------------

  // Returns the response containing the given uri with the appropriate mime
  // type.
  private Response getResource(String uri) {
    byte[] bytes = _cache.get(uri);
    if( bytes == null ) {
      InputStream resource = Boot._init.getResource2(uri);
      if (resource != null) {
        try {
          bytes = ByteStreams.toByteArray(resource);
        } catch( IOException e ) { }
        byte[] res = _cache.putIfAbsent(uri,bytes);
        if( res != null ) bytes = res; // Racey update; take what is in the _cache
      }
      Closeables.closeQuietly(resource);
    }
    if (bytes==null)
      return http404(uri);
    String mime = NanoHTTPD.MIME_DEFAULT_BINARY;
    if (uri.endsWith(".css"))
      mime = "text/css";
    else if (uri.endsWith(".html"))
      mime = "text/html";
    return new Response(NanoHTTPD.HTTP_OK,mime,new ByteArrayInputStream(bytes));
  }

  // others --------------------------------------------------------------------

  private Response http404(String uri) {
    return new Response(NanoHTTPD.HTTP_NOTFOUND,NanoHTTPD.MIME_PLAINTEXT,"Location "+uri+" not found.");
  }

}
