
package water.web;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.Properties;
import water.DKV;
import water.Key;
import water.hdfs.PersistHdfs;

/**
 *
 * @author peta
 */
public class PutHDFS extends H2OPage {

  
  public static Key importHDFSFile(String path) throws IOException {
    if (PersistHdfs.getHDFSRoot()==null)
      throw new IOException("HDFS is not initialized. Use the -hdfs argument when starting up H2O.");
    Key k = PersistHdfs.getKeyForPath(path);
    if (DKV.get(k)!=null)
      throw new IOException("File is already imported.");
    return PersistHdfs.importPath(path,""); // PersistHdfs.getHDFSRoot());
  }
  
  
  
  @Override public JsonObject serverJson(Server server, Properties parms, String sessionID) throws PageError {
    JsonObject result = new JsonObject();
    try {
      Key k = importHDFSFile(parms.getProperty("path"));
      result.addProperty("Key",k.toString());
    } catch (IOException e) {
      result.addProperty("Error",e.toString());
    }
    return result;
  }
  
  @Override protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    try {
      Key k = importHDFSFile(args.getProperty("path"));
      args.put("Key",k.toString());
      return new Inspect().serveImpl(server,args,sessionID);
    } catch (IOException e) {
      return error(e.toString());
    }
  }
  
  @Override public String[] requiredArguments() {
    return new String[] { "path" };
  }

}
