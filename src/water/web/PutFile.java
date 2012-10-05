package water.web;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.util.*;

import org.apache.http.*;
import org.apache.http.entity.HttpEntityWrapper;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.DefaultHttpServerConnection;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

import water.*;

import com.google.gson.*;
import com.sun.corba.se.impl.copyobject.JavaStreamObjectCopierImpl;
import com.sun.media.sound.JavaSoundAudioClip;

public class PutFile extends H2OPage {

    public static final int ACCEPT_CLIENT_TIMEOUT = 2*60*1000; // = 2mins

    public static int uploadFile(String filename, String key, byte rf) throws PageError {
      // Open a new port to listen by creating a server socket to permit upload.
      // The socket is closed by the uploader thread.
      ServerSocket serverSocket;
      try {
        // Setup server socket and get it port.
        serverSocket = new ServerSocket(0, 1); // 0 = find an empty port, 1 = maximum length of queue
        serverSocket.setSoTimeout(ACCEPT_CLIENT_TIMEOUT);
        serverSocket.setReuseAddress(true);
        int port = serverSocket.getLocalPort();
        System.err.format("JAVA: Opening server socket at port %d\n", port);
        // Launch uploader thread which retrieve a byte stream from client
        // and store it to key.
        // If the client is not connected withing a specifed timeout, the
        // thread is destroyed.
        new UploaderThread(serverSocket, filename, key, rf).start();

        return port;

      } catch( IOException e ) {
        throw new PageError("Cannot create server socket - please try one more time.");
      }
    }

  @Override
  public JsonObject serverJson(Server server, Properties args) throws PageError {
    // Get parameters: Key, file name, replication factor
    String key   = args.getProperty("Key",UUID.randomUUID().toString());
    if( key.isEmpty()) key = UUID.randomUUID().toString(); // additional check for empty Key-field since the Key-field can be returned as a part of form
    String fname = args.getProperty("File", "file"); // TODO: send file name
    int    rf    = getAsNumber(args, "RF", Key.DEFAULT_DESIRED_REPLICA_FACTOR);
    if( rf < 0 || 127 < rf) throw new PageError("Replication factor must be from 0 to 127.");

    int port = uploadFile(fname, key, (byte) rf);
    JsonObject res = new JsonObject();
    res.addProperty("port", port);
    return res;
  }

  @Override protected String serveImpl(Server server, Properties args) throws PageError {
    JsonObject json = serverJson(server, args);

    RString response = new RString(html()); // FIXME: delete
    response.replace(json);
    return response.toString();
  }

  //FIXME: delete
  private String html() {
    return "<div class='alert alert-success'>"
    + "Key <a href='Inspect?Key=%keyHref'>%key</a> has been put to the store with replication factor %rf, value size <strong>%vsize</strong>."
    + "</div>"
    + "<p><a href='StoreView'><button class='btn btn-primary'>Back to Node</button></a>&nbsp;&nbsp;"
    + "<a href='Put'><button class='btn'>Put again</button></a>"
    + "</p>"
    ;
  }

  private static class UploaderThread extends Thread {

    // Server socket
    ServerSocket ssocket;
    // Key properties
    String filename;
    String keyname;
    byte   rf;

    public UploaderThread(ServerSocket ssocket, String filename, String keyname, byte rf) {
      super("Uploader thread for: " + filename);
      this.ssocket  = ssocket;
      this.filename = filename;
      this.keyname  = keyname;
      this.rf       = rf;
    }

    @Override
    public void run() {

      try {
        while(true) {
          // Wait for 1st connection and handle connection in this thread.
          DefaultHttpServerConnection conn = new DefaultHttpServerConnection();
          conn.bind(ssocket.accept(), new BasicHttpParams()); // TODO: setup here socket properties like SO_TIMEOUT?
          HttpRequest request           = conn.receiveRequestHeader();
          RequestLine requestLine       = request.getRequestLine();
          System.err.println("REQUEST: " + request);
          try {
            if (requestLine.getMethod().equals("OPTIONS")) {
              handleOPTIONS(conn, request);
            } else if (requestLine.getMethod().equals("POST")) {
              handlePOST(conn, request);
              break;
            }
          } finally { // shutdown connection
            try { conn.close(); } catch( IOException e ) { }
          }
        }
      } catch (SocketTimeoutException ste) {
        // The client does not connect during the socket timeout => it is not interested in upload.
        ste.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (HttpException e) {
      } finally {
        // shutdown server
        try { if (ssocket != null) ssocket.close(); } catch( IOException e ) { }
      }
    }

    protected void handleOPTIONS(HttpServerConnection conn, HttpRequest request) throws HttpException, IOException {
      HttpResponse response = new BasicHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK");
      response.addHeader("Access-Control-Allow-Origin", "*");
      response.addHeader("Access-Control-Allow-Methods", "PUT,POST");
      for (Header header: request.getHeaders("Access-Control-Request-Headers")) {
        if (header.getValue().contains("x-file-name")) {
          response.addHeader("Access-Control-Allow-Headers", "x-file-name,x-file-size,x-file-type");
        }
      }
      conn.sendResponseHeader(response);
      conn.flush();
    }

    protected void handlePOST(HttpServerConnection conn, HttpRequest request) throws HttpException, IOException {
      Header contentTypeHeader = request.getFirstHeader("Content-Type");
      if (contentTypeHeader == null || !contentTypeHeader.getValue().startsWith("multipart/form-data")) { // File is not received
        // TODO: send error and return
        return;
      }

      String boundary = null; // Get file boundary.
      for(HeaderElement el : contentTypeHeader.getElements()) {
        NameValuePair nvp = el.getParameterByName("boundary");
        if (nvp != null) { boundary = nvp.getValue(); break; }
      }
      if (boundary == null) { /* TODO: send error and return */ return; }

      // Get http entity.
      conn.receiveRequestEntity((HttpEntityEnclosingRequest)request);
      System.err.println(boundary);
      HttpMultipartEntity entity = new HttpMultipartEntity( ((HttpEntityEnclosingRequest)request).getEntity(), boundary.getBytes() );
//      HttpEntity entity = ((HttpEntityEnclosingRequest)request).getEntity();
//      System.err.println(EntityUtils.toString(entity));

      // Read directly from stream and create a key
      Key key                = ValueArray.read_put_stream(keyname, entity.getContent(), rf);
 //     EntityUtils.consume(entity);
      JsonElement jsonResult = getJsonResult(key);

      HttpResponse response = new BasicHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK");
      response.setEntity(new StringEntity(jsonResult.toString(), NanoHTTPD.MIME_JSON, HTTP.DEFAULT_CONTENT_CHARSET));
      response.addHeader("Access-Control-Allow-Origin", "*");
      response.addHeader("Content-Type", NanoHTTPD.MIME_JSON);
      response.addHeader("Content-Length", String.valueOf(jsonResult.toString().length()));
      conn.sendResponseHeader(response);
      conn.sendResponseEntity(response);
      conn.flush();
    }

    protected JsonElement getJsonResult(Key key) {
      Value val = DKV.get(key);
      // The returned JSON object should follow structure of jquery-upload plugin
      JsonArray jsonResult = new JsonArray();
      JsonObject jsonFile   = new JsonObject();
      jsonFile.addProperty("name", filename);
      jsonFile.addProperty("size", val.length());
      jsonFile.addProperty("url", "/Get?key=" + encode(key));
      addProperty(jsonFile, "key", key);
      jsonResult.add(jsonFile);

      return jsonResult;
    }
  }

  static class HttpMultipartEntity extends HttpEntityWrapper {
    private static final byte[] BOUNDARY_PREFIX = { '\r', '\n', '-', '-' };
    byte[] boundary;

    public HttpMultipartEntity(HttpEntity wrapped, byte[] boundary) {
      super(wrapped);
      this.boundary = Arrays.copyOf(BOUNDARY_PREFIX, BOUNDARY_PREFIX.length + boundary.length);
      System.arraycopy(boundary, 0, this.boundary, BOUNDARY_PREFIX.length, boundary.length);
    }

    private void skipContentDispositionHeader(InputStream is) throws IOException {
      byte mode = 0; // 0 = nothing, 1=\n, 2=\n\n, 11=\r, 12=\r\n, 13=\r\n\r, 14=\r\n\r\n

      int c;
      while ((c = is.read()) != -1) {
        switch( mode ) {
        case 0 : if (c=='\n') mode= 1; else if (c=='\r') mode=11; else mode = 0; break;
        case 1 : if (c=='\n') return;  else if (c=='\r') mode= 0; else mode = 0; break;
        case 11: if (c=='\n') mode=12; else if (c=='\r') mode=11; else mode = 0; break;
        case 12: if (c=='\n') mode= 0; else if (c=='\r') mode=13; else mode = 0; break;
        case 13: if (c=='\n') return;  else if (c=='\r') mode=11; else mode = 0; break;
        }
      }
    }

    @Override
    public InputStream getContent() throws IOException {
      InputStream is = super.getContent();
      // Skip the content disposition header
      skipContentDispositionHeader(is);

      return new InputStreamWrapper(wrappedEntity.getContent());
    }

    class InputStreamWrapper extends InputStream {
      InputStream wrappedIs;

      byte[] lookAheadBuf;
      int    lookAheadLen;

      public InputStreamWrapper(InputStream is) {
        this.wrappedIs   = is;
        this.lookAheadBuf = new byte[boundary.length];
        this.lookAheadLen = 0;
      }

      @Override public void    close()      throws IOException  { wrappedIs.close();                }
      @Override public int     available()  throws IOException  { return wrappedIs.available();     }
      @Override public long    skip(long n) throws IOException  { return wrappedIs.skip(n);         }
      @Override public void    mark(int readlimit)              { wrappedIs.mark(readlimit);        }
      @Override public void    reset()      throws IOException  { wrappedIs.reset();                }
      @Override public boolean markSupported()                  { return wrappedIs.markSupported(); }

      @Override public int     read()         throws IOException { throw new UnsupportedOperationException(); }
      @Override public int     read(byte[] b) throws IOException { return read(b, 0, b.length); }
      @Override public int     read(byte[] b, int off, int len) throws IOException {
        int readLen = readInternal(b, off, len); System.err.println("READ: " + readLen);
        if (readLen != -1) {
          int pos     = findBoundary(b, off, readLen);
          if (pos != -1) {
            System.err.println("BOUNDARY FOUND");
            while (wrappedIs.read()!=-1) ; // read the rest of stream
            return pos - off;
          }
        }
        return readLen;
      }

      private int readInternal(byte b[], int off, int len) throws IOException {
        if (lookAheadLen > 0) {
          System.arraycopy(lookAheadBuf, 0, b, off, lookAheadLen);
          off += lookAheadLen;
          len -= lookAheadLen;
        }
        int readLen  = wrappedIs.read(b, off, len) + lookAheadLen;
        lookAheadLen = 0;
        return readLen;
      }

      // Find boundary in read buffer
      private int findBoundary(byte[] b, int off, int len) throws IOException {
        int bidx = -1; // start index of boundary
        int idx  = 0;  // actual index in boundary[]
        for(int i = off; i < off+len; i++) {
          if (boundary[idx] != b[i]) { // reset
            idx  = 0;
            bidx = -1;
          }
          if (boundary[idx] == b[i]) {
            if (idx == 0) bidx = i;
            if (++idx == boundary.length) return bidx; // boundary found
          }
        }
        if (bidx != -1) { // it seems that there is boundary but we did not match all boundary length
          assert lookAheadLen == 0; // There should not be not read lookahead
          lookAheadLen = boundary.length - idx;
          int readLen  = wrappedIs.read(lookAheadBuf, 0, lookAheadLen);
          if (readLen < boundary.length - idx) { // There is not enough data to match boundary
            lookAheadLen = readLen;
            return -1;
          }
          for (int i = 0; i < boundary.length - idx; i++) {
            if (boundary[i+idx] != lookAheadBuf[i]) return -1; // There is not boundary => preserve lookahed buffer
          }
          // Boundary found => do not care about lookAheadBuffer since all remaining data are ignored
        }

        return bidx;
      }
    }
  }
}
