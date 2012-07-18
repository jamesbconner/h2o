/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water.web;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;
import water.Key;
import water.ValueArray;

/**
 *
 * @author peta
 */
public class ImportUrl extends H2OPage {
  @Override protected String serve_impl(Properties args) {
/*     try {
        URL oracle = new URL(args.getProperty("Url"));
        BufferedReader in = new BufferedReader(
        new InputStreamReader(oracle.openStream()));

        String inputLine;
        while ((inputLine = in.readLine()) != null)
            System.out.println(inputLine);
        in.close();    
        
     } catch (Exception e) {
     }  */
    
    
    
    URL url;
    try {
      url = new URL(args.getProperty("Url"));
    } catch( MalformedURLException ex ) {
      return error("Malformed url: "+args.getProperty("Url"));
    }
    String key_s = args.getProperty("Key",args.getProperty("Url"));
    if (key_s.isEmpty())
      key_s = args.getProperty("Url");
    int rf = getAsNumber(args, "RF", Key.DEFAULT_DESIRED_REPLICA_FACTOR);
    if ((rf<0) || (rf>127))
      return error("Replication factor must be from 0 to 127.");
    Key key;
    try { 
      key = Key.make(key_s);      // Get a Key from a raw byte array, if any
    } catch( IllegalArgumentException e ) {
      return error("Not a valid key: "+ key_s);
    }
    try {
      InputStream s =  url.openStream();
      if (s==null)
        return error("Unable to open stream to URL "+url.toString());
      Key k = ValueArray.read_put_stream(key_s,s, (byte)rf);
      s.close();
      return success("Url "+url.toString()+" imported as key <strong>"+key_s+"</strong>");
    } catch (IOException e) {
      return error("Unable to import url "+url.toString()+" due to the following error:<br />"+e.toString());
    }
  }
  
  @Override public String[] requiredArguments() {
    return new String[] { "Url" };
  }
  
  
}
