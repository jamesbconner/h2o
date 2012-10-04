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

import water.*;

import com.google.gson.JsonObject;

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

    RString response = new RString(html());
    response.replace(json);
    return response.toString();
  }

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
    String keyname;
    byte   rf;

    public UploaderThread(ServerSocket ssocket, String filename, String keyname, byte rf) {
      super("Uploader thread for: " + filename);
      this.ssocket = ssocket;
      this.keyname = keyname;
      this.rf      = rf;
    }

    @Override
    public void run() {
      // Connection representation.
      DefaultHttpServerConnection conn = new DefaultHttpServerConnection();

      try {
        // Wait for 1st connection and handle connection in this thread.
        conn.bind(ssocket.accept(), new BasicHttpParams()); // TODO: setup here socket properties like SO_TIMEOUT?
        HttpRequest request           = conn.receiveRequestHeader();
        Header      contentTypeHeader = request.getFirstHeader("Content-Type");
        if (contentTypeHeader == null || !contentTypeHeader.getValue().startsWith("multipart/form-data")) { // File is not received
          // TODO: send error and return
          return;
        }

        String boundary = null; // Get file boundary.
        for(HeaderElement el : contentTypeHeader.getElements()) {
          NameValuePair nvp = el.getParameterByName("boundary");
          if (nvp!=null) {
            boundary = nvp.getValue();
            break;
          }
        }
        if (boundary == null) { /* TODO: send error and return */ return; }

        // Get http entity.
        conn.receiveRequestEntity((HttpEntityEnclosingRequest)request);
        HttpMultipartEntity entity = new HttpMultipartEntity( ((HttpEntityEnclosingRequest)request).getEntity(), boundary.getBytes() );

        // Read directly from stream and create a key
        Key key = ValueArray.read_put_stream(keyname, entity.getContent(), rf);
        System.err.println("JAVA: Key = " + key);

        HttpResponse response = new BasicHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_CREATED, "CREATED");
        JsonObject result = new JsonObject();
        result.addProperty("size", entity.getContentLength());

        response.setEntity(new StringEntity(result.toString(), NanoHTTPD.MIME_JSON, "UTF-8"));
        conn.sendResponseHeader(response);
        conn.sendResponseEntity(response);
        conn.flush();
        System.err.println("JAVA: finished");
      } catch (SocketTimeoutException ste) {
        // The client does not connect during the socket timeout => it is not interested in upload.
        ste.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (HttpException e) {
      } finally {
        // shutdown connection
        try { conn.close(); }                         catch( IOException e ) { }
        // shutdown server
        try { if (ssocket != null) ssocket.close(); } catch( IOException e ) { }
      }
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

      public InputStreamWrapper(InputStream is)             { this.wrappedIs = is; }
      @Override public int available() throws IOException   { return wrappedIs.available(); }
      @Override public int read() throws IOException        { return wrappedIs.read(); }
      @Override public long skip(long n) throws IOException { return wrappedIs.skip(n); }
      @Override public void mark(int readlimit)             { wrappedIs.mark(readlimit); }
      @Override public void reset() throws IOException      { wrappedIs.reset();   }
      @Override public boolean markSupported()              { return wrappedIs.markSupported(); }
      @Override public void close() throws IOException      { wrappedIs.close(); }
      @Override public int read(byte[] b) throws IOException { return read(b, 0, b.length); }
      @Override public int read(byte[] b, int off, int len) throws IOException {
        int readLen = wrappedIs.read(b, off, len);
        int pos     = findBoundary(b, off, readLen);
        if (pos != -1)
          return pos - off;
        else
          return readLen;
      }

      // find boundary in read buffer (little bit tricky since it does not handle buffer boundaries
      private int findBoundary(byte[] b, int off, int len) {
        int bidx = -1; // start index of boundary
        int idx  = 0;  // index in boundary[]
        for(int i = off; i < off+len; i++) {
          if (boundary[idx] == b[i]) {
            if (idx == 0) bidx = i;
            if (++idx == boundary.length) return bidx;
          } else {
            bidx = -1;
          }
        }

        return bidx;
      }
    }
  }
}
