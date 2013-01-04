
package water.api;

import init.Boot;

import java.io.*;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import water.H2O;
import water.NanoHTTPD;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;

/** This is a simple web server. */
public class RequestServer extends NanoHTTPD {

  // cache of all loaded resources
  private static final ConcurrentHashMap<String,byte[]> _cache = new ConcurrentHashMap();
  protected static final HashMap<String,Request> _requests = new HashMap();

  private static final Request _http404;
  private static final Request _http500;

  // initialization ------------------------------------------------------------
  static {
    _http404 = registerRequest(new HTTP404());
    _http500 = registerRequest(new HTTP500());


    Request.addToNavbar(registerRequest(new Inspect()),     "Inspect",      "Data");
    Request.addToNavbar(registerRequest(new Parse()),       "Parse",        "Data");
    Request.addToNavbar(registerRequest(new ImportFiles()), "Import Files", "Data");
    Request.addToNavbar(registerRequest(new ImportUrl()),   "Import URL",   "Data");
    Request.addToNavbar(registerRequest(new PutFile()),     "Upload",       "Data");
    Request.addToNavbar(registerRequest(new Get()),         "Download",     "Data");
    Request.addToNavbar(registerRequest(new StoreView()),   "View All",     "Data");

    Request.addToNavbar(registerRequest(new RF()),  "Random Forest", "Model");
    Request.addToNavbar(registerRequest(new GLM()), "GLM",           "Model");
    Request.addToNavbar(registerRequest(new GLMGrid()),"GLMGrid",    "Model");
    Request.addToNavbar(registerRequest(new RFView()),"Random Forest","Views");

    Request.addToNavbar(registerRequest(new Cloud()),    "Cloud Status", "Admin");
    Request.addToNavbar(registerRequest(new Network()),  "Network",      "Admin");
    Request.addToNavbar(registerRequest(new Timeline()), "Timeline",     "Admin");
    Request.addToNavbar(registerRequest(new JStack()),   "Stack Dump",   "Admin");
    Request.addToNavbar(registerRequest(new PutValue()), "Put Value",    "Admin");
    Request.addToNavbar(registerRequest(new Shutdown()), "Shutdown",     "Admin");

    Request.addToNavbar(registerRequest(new Tutorials()),           "View All",      "Tutorials");
    Request.addToNavbar(registerRequest(new TutorialRFIris()),      "Random Forest", "Tutorials");
    Request.addToNavbar(registerRequest(new TutorialGLMProstate()), "GLM",           "Tutorials");

    // internal handlers
    registerRequest(new GLMGridProgress());
    registerRequest(new ParseProgress());
    registerRequest(new RFTreeView());
    registerRequest(new TypeaheadKeysRequest());
    registerRequest(new TypeaheadFileRequest());
    registerRequest(new TypeaheadHexKeyRequest());
    registerRequest(new TypeaheadModelRequest());
    registerRequest(new WWWFileUpload());
    registerRequest(new PutVector());
    registerRequest(new GetVector());

    // testing hooks
    registerRequest(new TestPoll());
    registerRequest(new TestRedirect());
    Request.initializeNavBar();
  }

  /** Registers the request with the request server.
   *
   * returns the request so that it can be further updated.
   */

  protected static Request registerRequest(Request req) {
    String href = req.getClass().getSimpleName();
    assert (! _requests.containsKey(href)) : "Request with href "+href+" already registered";
    _requests.put(href,req);
    return req;

  }

  // Keep spinning until we get to launch the NanoHTTPD
  public static void start() {
    new Thread( new Runnable() {
        public void run()  {
          while( true ) {
            try {
              // Try to get the NanoHTTP daemon started
              new RequestServer(H2O._apiSocket);
              break;
            } catch ( Exception ioe ) {
              System.err.println("Launching NanoHTTP server got "+ioe);
              try { Thread.sleep(1000); } catch( InterruptedException e ) { } // prevent denial-of-service
            }
          }
        }
      }, "Request Server launcher").start();
  }

  // uri serve -----------------------------------------------------------------

  @Override public NanoHTTPD.Response serve( String uri, String method, Properties header, Properties parms, Properties files ) {
    // Jack priority for user-visible requests
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY-1);
    // update arguments and determine control variables
    if( uri.isEmpty() || uri.equals("/") ) uri = "/Tutorials.html";
    // determine the request type
    Request.RequestType type = Request.RequestType.requestType(uri);
    String requestName = type.requestName(uri);
    try {
      // determine if we have known resource
      Request request = _requests.get(requestName);
      // if the request is not know, treat as resource request, or 404 if not
      // found
      if (request == null)
        return getResource(uri);
      // otherwise unify get & post arguments
      parms.putAll(files);
      // call the request
      return request.serve(this,parms,type);
    } catch (Exception e) {
      e.printStackTrace();
      // make sure that no Exception is ever thrown out from the request
      parms.setProperty(Request.ERROR,e.getClass().getSimpleName()+": "+e.getMessage());
      return _http500.serve(this,parms,type);
    }
  }

  private RequestServer( ServerSocket socket ) throws IOException {
    super(socket,null);
  }

  // Resource loading ----------------------------------------------------------

  // Returns the response containing the given uri with the appropriate mime
  // type.
  private NanoHTTPD.Response getResource(String uri) {
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
    if ((bytes == null) || (bytes.length == 0)) {
      // make sure that no Exception is ever thrown out from the request
      Properties parms = new Properties();
      parms.setProperty(Request.ERROR,uri);
      return _http404.serve(this,parms,Request.RequestType.www);
    }
    String mime = NanoHTTPD.MIME_DEFAULT_BINARY;
    if (uri.endsWith(".css"))
      mime = "text/css";
    else if (uri.endsWith(".html"))
      mime = "text/html";
    return new NanoHTTPD.Response(NanoHTTPD.HTTP_OK,mime,new ByteArrayInputStream(bytes));
  }

}
