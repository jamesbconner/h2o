package water.web;
import java.util.Properties;
import java.util.UUID;

import water.DKV;
import water.Key;
import water.Value;
import water.parser.ParseDataset;

import com.google.gson.JsonObject;

public class Parse extends H2OPage {
  @Override public String[] requiredArguments() { return new String[] { "Key" }; }

  @Override
  public JsonObject serverJson(Server s, Properties p) throws PageError {
    String k = p.getProperty("Key");
    String rk = p.getProperty("Key",UUID.randomUUID().toString());

    Key key = decode(k);
    Key resKey = Key.make(rk);

    JsonObject res = new JsonObject();

    if( DKV.get(resKey) == null ) { // Key not parsed?  Parse it
      long start = System.currentTimeMillis();
      Value dataset = DKV.get(key);  // Get the source dataset root key
      if( dataset == null ) throw new PageError(key.toString()+" not found");
      ParseDataset.parse(resKey, dataset);
      long now = System.currentTimeMillis();
      res.addProperty("Key", encode(resKey));
      res.addProperty("TimeMS", now - start);
    } else {
      res.addProperty("Key", encode(resKey));
      res.addProperty("TimeMS", 0);
    }
    return res;

  }

  @Override protected String serveImpl(Server s, Properties p) throws PageError {
    JsonObject json = serverJson(s, p);

    int timeMS = json.get("TimeMS").getAsInt();
    if( timeMS > 0 ) {
      RString res = new RString("Parsed into <a href='/Inspect?Key=%Key'>%Key</a> in %TimeMS msec");
      res.replace(json);
      return res.toString();
    } else {
      return "Already parsed into " + json.get("Key");
    }
  }
}
