/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water.web;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import water.Key;
import water.ValueArray;

/**
 *
 * @author peta
 */
public class PutFile extends H2OPage {

  public static Object uploadFile(Properties args) {
    String key_s = args.getProperty("Key",UUID.randomUUID().toString());
    if (key_s.isEmpty())
      key_s = UUID.randomUUID().toString();
    String fname = args.getProperty("File");
    int rf = getAsNumber(args, "RF", Key.DEFAULT_DESIRED_REPLICA_FACTOR);
    if ((rf<0) || (rf>127))
      return error("Replication factor must be from 0 to 127.");
    return uploadFile(fname,key_s,(byte)rf);
  }
  
  public static Object uploadFile(String fname,String key_s, byte rf) {
    try {
      FileInputStream fis = new FileInputStream(fname);
      try {
       // long start = System.currentTimeMillis();
       // System.err.println("uploading...");
        // Read the entire file, and put into the Store
        Key key = ValueArray.read_put_file(key_s,fis,rf);
        // Report Key string to user
        return key;
      } catch( IllegalArgumentException e ) {
        return error("Not a valid key: "+ key_s);
      } catch (IOException e) {
        return error(e.toString());
      } catch (OutOfMemoryError e) {
        return error("OutOfMemory during file upload - please try smaller files or large Java heap memory");
      } finally {
        try { fis.close(); } catch( IOException e ) { }
      }
    } catch (FileNotFoundException e) {
      return error("Unable to transfer the file. Maybe the file is too big for the network to transfer. Check your network settings.");
      //return e.toString();
    }
    
  }
  
  @Override protected String serve_impl(Properties args) {
    String key_s = args.getProperty("Key",UUID.randomUUID().toString());
    if (key_s.isEmpty())
      key_s = UUID.randomUUID().toString();
    String fname = args.getProperty("File");
    int rf = getAsNumber(args, "RF", Key.DEFAULT_DESIRED_REPLICA_FACTOR);
    if ((rf<0) || (rf>127))
      return error("Replication factor must be from 0 to 127.");
    Object result = uploadFile(fname,key_s,(byte)rf);
    if (result instanceof String)
      return (String)result; // error
    RString response = new RString(html);
//    response.clear();
    response.replace("key",key_s);
    response.replace("rf",rf);
    response.replace("vsize",new File(fname).length());
    return response.toString();
  }
  
  
  @Override public String[] requiredArguments() {
    return new String[] { "File" };
  }
  
  public static final String html = 
            "<div class='alert alert-success'>"
          + "Key <strong>%key</strong> has been put to the store with replication factor %rf, value size <strong>%vsize</strong>."
          + "</div>"
          + "<p><a href='StoreView'><button class='btn btn-primary'>Back to Node</button></a>&nbsp;&nbsp;"
          + "<a href='Put'><button class='btn'>Put again</button></a>"
          + "</p>"
          ;
  
//  public static final RString response = new RString(html);

  
}
