package water.web;

import init.Loader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

import water.Log;
import water.H2O;
import water.NanoHTTPD;

/** This is a simple web server.
 * 
 * It is here so that I do not clash with the commits of others and they do not
 * clash with me.

* @author peta
 */
public class Server extends NanoHTTPD {

  // initialization ------------------------------------------------------------

  // Keep spinning until we get to launch the NanoHTTPD
  public static void start() {
    new Thread( new Runnable() {
        public void run()  {
          while( true ) {
            try {
              // Try to get the NanoHTTP daemon started
              new Server(H2O.WEB_PORT);
              //System.out.println("[web] Listening on http:/"+H2O.SELF+":"+H2O.WEB_PORT+"/");
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
  
  /** Serves the client. 
   * 
   * @param uri
   * @param method
   * @param header
   * @param parms
   * @param files
   * @return 
   */ 
  @Override public Response serve( String uri, String method, Properties header, Properties parms, Properties files ) {
    if (uri.isEmpty())
      uri = "/";
    Page page = _pages.get(uri.substring(1));
    // if we cannot handle it, then it might be a resource
    if (page==null)
      return getResource(uri);
    // unify GET and POST arguments
    parms.putAll(files);
    // check that required arguments are present
    String[] reqArgs = page.requiredArguments();
    if (reqArgs!=null) {
      for (String s : reqArgs) {
        if (!parms.containsKey(s) || parms.getProperty(s).isEmpty())
          return new Response(NanoHTTPD.HTTP_OK, NanoHTTPD.MIME_HTML, H2OPage.wrap(H2OPage.error("Not all required parameters were supplied to page <strong>"+uri+"</strong><br/>Argument <strong>"+s+"</strong> is missing.")));
      }
    }
    Object result = page.serve(this,parms);
    if (result == null)
      return http404(uri);
    if (result instanceof Response)
      return (Response)result;
    return new Response(NanoHTTPD.HTTP_OK, NanoHTTPD.MIME_HTML, result.toString());
  }
  
  public static Page getPage(String uri) {
    return _pages.get(uri);
  }

  
  // constructor ---------------------------------------------------------------

  private Server( int port ) throws IOException {
    // No file system arg
    super(port,null);
    // initialize pages
    registerPage(new Cloud(),"");
    //registerPage(new Compile(),"Compile");
    registerPage(new DebugView(),"DebugView");
    //registerPage(new Exec(),"Exec");
    //registerPage(new ExecQuery(),"ExecQuery");
    registerPage(new Get(),"Get");
    registerPage(new GetQuery(),"GetQuery");
    registerPage(new ImportFolder(),"ImportFolder");
    registerPage(new ImportQuery(),"ImportQuery");
    registerPage(new ImportUrl(),"ImportUrl");
    registerPage(new Inspect(),"Inspect");
    //registerPage(new PutExec(),"PutExec");
    //registerPage(new PutExecQuery(),"PutExecQuery");
    registerPage(new PutFile(),"PutFile");
    registerPage(new PutQuery(),"Put");
    registerPage(new PutValue(),"PutValue");
    registerPage(new Remote(),"Remote");
    registerPage(new Remove(),"Remove");
    registerPage(new RemoveAck(),"RemoveAck");
    registerPage(new Shutdown(),"Shutdown");
    registerPage(new StoreView(),"StoreView");
    registerPage(new Test(),"Test");
    registerPage(new Timeline(),"Timeline");
    registerPage(new Append(),"Append");
    registerPage(new AppendQuery(),"AppendQuery");
    registerPage(new KeysView(),"KeysView");
  }


  // Resource loading ----------------------------------------------------------
  
  // a shortcut to the loader
  private Loader _loader = Loader.instance();
  
  // cache of all loaded resources
  private HashMap<String,byte[]> _cache = new HashMap();

  // reads the given stream to the memory and returns its contents as a byte
  // array
  private byte[] readStreamToBytes(InputStream is) {
    if (is==null)
      return null;
    try {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      int n;
      byte[] data = new byte[4096];
      while ((n = is.read(data, 0, data.length)) != -1) {
        buffer.write(data, 0, n);
      }
      buffer.flush();
      return buffer.toByteArray();    
    } catch (IOException e) {
      return null;
    } finally {
      try {
        is.close();
      } catch (IOException e) {
        // pass
      }
    }
  }
  
  // Returns the response containing the given uri with the appropriate mime
  // type.
  private Response getResource(String uri) {
    byte[] bytes = _cache.get(uri);
    if (bytes == null) {
      if (_loader.runningFromJar()) {
        InputStream is = _loader.getResourceAsStream("resources"+uri);  
        bytes = readStreamToBytes(is);
      } else { // to allow us to read things not only from the loader
        try {
          InputStream is = new FileInputStream(new File("lib/resources"+uri));
          bytes = readStreamToBytes(is);
        } catch (FileNotFoundException e) {
          // pass
        }
      }
      _cache.put(uri,bytes);
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
  
  // Pages ---------------------------------------------------------------------
  
  private static HashMap<String,Page> _pages = new HashMap();
  
  
  public static void registerPage(Page page, String name) {
    //if (_pages.containsKey(name))
    //  Log.say("[webserver] Page "+name+" already exists. Hiding old page object.");
    _pages.put(name,page);
    //Log.debug("[webserver] Page "+name+" registered.");
  }
  
  // others --------------------------------------------------------------------
  
  private Response http404(String uri) {
    return new Response(NanoHTTPD.HTTP_NOTFOUND,NanoHTTPD.MIME_PLAINTEXT,"Location "+uri+" not found.");
  }
  
}
