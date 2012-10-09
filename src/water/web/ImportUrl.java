package water.web;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import com.google.common.io.Closeables;

import water.Key;
import water.ValueArray;

public class ImportUrl extends H2OPage {
  @Override protected String serveImpl(Server server, Properties args, String sessionID) {
    URL url;
    try {
      url = new URL(args.getProperty("Url"));
    } catch( MalformedURLException ex ) {
      return error("Malformed url: "+args.getProperty("Url"));
    }
    String key_s = args.getProperty("Key",args.getProperty("Url"));
    if (key_s.isEmpty()) key_s = args.getProperty("Url");

    int rf = getAsNumber(args, "RF", Key.DEFAULT_DESIRED_REPLICA_FACTOR);
    if( rf < 0 || rf > 127) return error("Replication factor must be from 0 to 127.");

    try {
       Key.make(key_s);
    } catch( IllegalArgumentException e ) {
      return error("Not a valid key: "+ key_s);
    }
    InputStream s = null;
    try {
      s = url.openStream();
      if( s==null ) return error("Unable to open stream to URL "+url.toString());
      ValueArray.read_put_stream(key_s, s, (byte)rf);
      return success("Url "+url.toString()+" imported as key <strong>"+key_s+"</strong>");
    } catch (IOException e) {
      return error("Unable to import url "+url.toString()+" due to the following error:<br />"+e.toString());
    } finally {
      Closeables.closeQuietly(s);
    }
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Url" };
  }
}
