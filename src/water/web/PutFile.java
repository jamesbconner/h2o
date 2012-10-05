package water.web;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import water.Key;
import water.ValueArray;

import com.google.common.io.Closeables;
import com.google.gson.JsonObject;

public class PutFile extends H2OPage {

  public static Key uploadFile(String fname, String key, int rf) throws PageError {
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(fname);
      return ValueArray.read_put_file(key, fis, (byte) rf);
    } catch (FileNotFoundException e) {
      throw new PageError("File not found or unable to transfer the file.  Maybe the file is too big for the network to transfer. Check your network settings.");
    } catch( IllegalArgumentException e ) {
      throw new PageError("Not a valid key: "+ key);
    } catch( IOException e ) {
      throw new PageError(e.toString());
    } catch( OutOfMemoryError e ) {
      throw new PageError("OutOfMemory during file upload - please try smaller files or large Java heap memory");
    } finally {
      Closeables.closeQuietly(fis);
    }
  }

  @Override
  public JsonObject serverJson(Server server, Properties args) throws PageError {
    String key   = args.getProperty("Key",UUID.randomUUID().toString());
    if( key.isEmpty()) key = UUID.randomUUID().toString(); // additional check for empty Key-field since the Key-field can be returned as a part of form
    String fname = args.getProperty("File");
    int    rf    = getAsNumber(args, "RF", Key.DEFAULT_DESIRED_REPLICA_FACTOR);
    if( rf < 0 || 127 < rf) throw new PageError("Replication factor must be from 0 to 127.");        
    
    Key k = uploadFile(fname, key, rf);
    JsonObject res = new JsonObject();
    addProperty(res, "key", k);
    res.addProperty("rf", rf);
    res.addProperty("vsize", new File(fname).length());
    return res;
  }

  @Override protected String serveImpl(Server server, Properties args) throws PageError {
    JsonObject json = serverJson(server, args);

    RString response = new RString(html());
    response.replace(json);
    return response.toString();
  }

  @Override public String[] requiredArguments() {
    return new String[] { "File" };
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
}
