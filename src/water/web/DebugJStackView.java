/**
 *
 */
package water.web;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import water.*;

import com.google.gson.*;

/**
 * @author michal
 *
 */
public class DebugJStackView extends H2OPage {

  public static class JStackCollectorTask extends DRemoteTask {

    /* OUT */ String[] result; /* for each node in the cloud it contains all threads stack traces */

    public JStackCollectorTask() {
      result = new String[H2O.CLOUD._memary.length];
    }

    @Override
    public void reduce(DRemoteTask drt) {
      JStackCollectorTask another = (JStackCollectorTask) drt;
      for (int i=0; i<result.length; ++i) {
        if (result[i] == null)
          result[i] = another.result[i];
      }
    }

    @Override
    public void compute() {
      Map<Thread, StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
      StringBuilder sb = new StringBuilder();
      for (Entry<Thread,StackTraceElement[]> el : allStackTraces.entrySet()) {
        append(sb, el.getKey());
        append(sb, el.getValue());
        sb.append('\n');
      }

      result[H2O.SELF.index()] = sb.toString();

      tryComplete();
    }

    private void append(final StringBuilder sb, final Thread t) {
      sb.append(t); sb.append('\n');
    }

    private void append(final StringBuilder sb, final StackTraceElement[] trace) {
      for (int i=0; i < trace.length; i++) {
        sb.append("\tat "); sb.append(trace[i]); sb.append('\n');
      }
    }

    @Override public int wire_len() {
      int len = 4 /* 4 bytes for array length */ ;
      for (int i=0; i<result.length; ++i) {
        len += 2 /* 2 bytes for string length */ + (result[i] != null ? result[i].length() : 0);
      }
      return len;
    }

    @Override public void read(DataInputStream is) throws IOException {
      result = TCPReceiverThread.readStrAry(is);
    }
    @Override public void write(DataOutputStream os) throws IOException {
      TCPReceiverThread.writeAry(os, result);
    }
    @Override public void read(Stream s) {
      int len = s.get4();
      result = len!=-1 ? new String[len] : null;
      for (int i = 0; i<len; i++) result[i] = s.getLen2Str();
    }
    @Override public void write(Stream s) {
      int len = result != null ? result.length : -1;
      s.set4(len);
      for (int i = 0; i<len; i++) s.setLen2Str(result[i]);
    }
  }

  @Override
  public JsonObject serverJson(Server server, Properties parms, String sessionID) throws PageError {
    JStackCollectorTask collector = new JStackCollectorTask();
    collector.invokeOnAllNodes();

    JsonObject result = new JsonObject();
    JsonArray nodes = new JsonArray();
    result.add("nodes", nodes);
    for (int i=0; i<collector.result.length; ++i) {
      JsonObject el = new JsonObject();
      el.addProperty("id", i);
      el.addProperty("node", H2O.CLOUD._memary[i].toString());
      el.addProperty("traces", collector.result[i]);
      nodes.add(el);
    }

    return result;
  }

  @Override
  protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    JsonObject result = serverJson(server, args, sessionID);
    RString r = new RString(html());

    JsonArray jary = result.getAsJsonArray("nodes");
    int i = 0;
    for (JsonElement el : jary) {
      formatNodesTabs((JsonObject) el, r, i);
      i++;
    }
    i = 0;
    for (JsonElement el : jary) {
      formatNodesTraces((JsonObject) el, r, i);
      i++;
    }

    return r.toString();
  }

  private void formatNodesTabs(JsonObject el, RString response, int idx) {
    RString row = response.restartGroup("nodeTab");
    if (idx==0) row.replace("active", "active"); else row.replace("active", "");
    row.replace(el);
    row.append();
  }
  private void formatNodesTraces(JsonObject el, RString response, int idx) {
    RString row = response.restartGroup("nodeContent");
    if (idx==0) row.replace("active", " active"); else row.replace("active", "");
    row.replace(el);
    row.append();
  }

  @Override
  protected String[] additionalScripts() {
    return new String[] { "bootstrap/js/bootstrap.min.js" };
  }

  private String html() {
    return
    	 "<ul class='nav nav-tabs'>"
         + " <li class='active'><a href='DbgJStack'>JStack</a></li>\n"
         + " <li class='disabled'> <a href='DbgJStat'>JStat</a></li>\n"
         + "</ul>\n"
         + "<div class='tabbable tabs-left'>\n"
         + " <ul class='nav nav-tabs' id='nodesTab'>\n"
         + "%nodeTab{"
         + "   <li class='%active'>\n"
         + "    <a href='#tab%id' data-toggle='tab'>%node</a>\n"
         + "   </li>\n"
         + "}"
         + " </ul>\n"
         + " <div class='tab-content' id='nodesTabContent'>\n"
         + "%nodeContent{"
         + "  <div class='tab-pane%active' id='tab%id'>\n"
         + "   <pre>%traces</pre>\n"
         + "  </div>\n"
         + "}"
         + "</div>\n"
         + "</div>\n"
         + "<script type='text/javascript'>"
         + "$(document).ready(function() {"
         + "  $('#nodesTab a').click(function(e) {"
         + "    e.preventDefault(); $(this).tab('show');"
         + "  });"
         + "});"
         + "</script>";
  }

}
