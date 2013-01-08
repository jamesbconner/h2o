package water.api;

import java.io.*;
import java.net.URL;

import water.Key;
import water.ValueArray;

import com.google.common.io.Closeables;
import com.google.gson.JsonObject;

public class Exec extends Request {
  private final Str _exec = new Str("Exec");

  @Override
  protected Response serve() {
    String s = _exec.value();
    JsonObject res = new JsonObject();
    res.addProperty("Expr", s);
    try {
      long time = System.currentTimeMillis();
      Key k = water.exec.Exec.exec(s);
      time = System.currentTimeMillis() - time;
      ValueArray va = ValueArray.value(k);
      return new Inspect(k).serveValueArray(va);
    } catch( Exception e ) {
      res.addProperty("Error", e.toString());
      return Response.done(res);
    }
  }
}
