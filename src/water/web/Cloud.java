package water.web;

import com.google.gson.JsonObject;
import java.util.Properties;
import water.H2O;
import water.H2ONode;
import water.HeartBeatThread;
import water.Paxos;

public class Cloud extends H2OPage {

  public Cloud() {
    _refresh = 5;
  }

  @Override
  public JsonObject serverJson(Server server, Properties parms, String sessionID) {
    JsonObject res = new JsonObject();
    final H2O cloud = H2O.CLOUD;
    final H2ONode self = H2O.SELF;
    res.addProperty("cloud_name", H2O.NAME);
    res.addProperty("node_name", self.toString());
    res.addProperty("cloud_size",cloud._memary.length);
    res.addProperty("consensus",Paxos._commonKnowledge); // Cloud is globally accepted
    res.addProperty("locked",Paxos._cloud_locked); // Cloud is locked against changes

    res.addProperty("fjthrds_hi",self.get_fjthrds_hi());
    res.addProperty("fjqueue_hi",self.get_fjqueue_hi());
    res.addProperty("fjthrds_lo",self.get_fjthrds_lo());
    res.addProperty("fjqueue_lo",self.get_fjqueue_lo());
    res.addProperty("rpcs"      ,self.get_rpcs()      );

    return res;
  }

  @Override protected String serveImpl(Server server, Properties args, String sessionID) {
    RString response = new RString(html);
    response.replace("cloud_name",H2O.NAME);
    response.replace("node_name",H2O.SELF.toString());
    final H2O cloud = H2O.CLOUD;
    for( H2ONode h2o : cloud._memary ) {
      // restart the table line
      RString row = response.restartGroup("tableRow");
      // This hangs on ipv6 name resolution
      //String name = h2o._inet.getHostName();
      row.replace("host",h2o);
      row.replace("node",h2o);
      row.replace("num_cpus" ,            h2o.get_num_cpus () );
      row.replace("free_mem" ,toMegabytes(h2o.get_free_mem ()));
      row.replace("tot_mem"  ,toMegabytes(h2o.get_tot_mem  ()));
      row.replace("max_mem"  ,toMegabytes(h2o.get_max_mem  ()));
      row.replace("num_keys" ,           (h2o.get_keys     ()));
      row.replace("val_size" ,toMegabytes(h2o.get_valsz    ()));
      row.replace("free_disk",toMegabytes(h2o.get_free_disk()));
      row.replace("max_disk" ,toMegabytes(h2o.get_max_disk ()));

      row.replace("cpu_util" ,pos_neg(h2o.get_cpu_util()));

      double [] cpu_load = h2o.get_cpu_load();
      row.replace("cpu_load_1" ,pos_neg(cpu_load[0]));
      row.replace("cpu_load_5" ,pos_neg(cpu_load[1]));
      row.replace("cpu_load_15",pos_neg(cpu_load[2]));

      int fjq_hi = h2o.get_fjqueue_hi();
      int fjt_hi = h2o.get_fjthrds_hi();
      if(fjq_hi > HeartBeatThread.QUEUEDEPTH)
        row.replace("queueStyleHi","background-color:green;");
      row.replace("fjthrds_hi" , fjt_hi);
      row.replace("fjqueue_hi" , fjq_hi);
      int fjq_lo = h2o.get_fjqueue_lo();
      int fjt_lo = h2o.get_fjthrds_lo();
      if(fjq_lo > HeartBeatThread.QUEUEDEPTH)
        row.replace("queueStyleLo","background-color:green;");
      row.replace("fjthrds_lo" , fjt_lo);
      row.replace("fjqueue_lo" , fjq_lo);
      row.replace("rpcs" ,                h2o.get_rpcs());
      row.replace("tcps_active" ,         h2o.get_tcps_active());

      row.append();
    }
    response.replace("size",cloud._memary.length);
    response.replace("voting",Paxos._commonKnowledge?"":"Voting in progress");
    response.replace("locked",Paxos._cloud_locked?"Cloud locked":"");
    return response.toString();
  }

  static String toMegabytes(long what) {
    return Long.toString(what>>20)+"M";
  }

  static String pos_neg( double d ) {
    return d>=0 ? Double.toString(d) : "n/a";
  }

  private final String html =
    "<div class='alert alert-success'>"
    + "You are connected to cloud <strong>%cloud_name</strong> and node <strong>%node_name</strong>."
    + "</div>\n"
    + "<p>The Local Cloud has %size members\n"
    + "<table class='table table-striped table-bordered table-condensed'>\n"
    + "<thead class=''><tr>\n"
    +     "<th rowspan=\"2\">Local Nodes</th>\n"
    +     "<th rowspan=\"2\">CPUs</th>\n"
    +     "<th rowspan=\"2\">Local Keys</th>\n"
    +     "<th colspan=\"4\" style='text-align:center'>Memory</th>\n"
    +     "<th colspan=\"2\" style='text-align:center'>Disk</th>\n"
    +     "<th colspan=\"4\" style='text-align:center'>CPU Load</th>\n"
    +     "<th colspan=\"3\" style='text-align:center'>Threads / Tasks</th>\n"
    +     "<th rowspan=\"2\">TCPs Active</th>\n"
    + "</tr>\n"
    + "<tr>\n"
    +     "<th>Cached</th>\n"  // memory
    +     "<th>Free</th>\n"
    +     "<th>Total</th>\n"
    +     "<th>Max</th>\n"
    +     "<th>Free</th>\n"    // disk
    +     "<th>Max</th>\n"
    +     "<th>Util</th>\n"    // CPU
    +     "<th>1min</th>\n"
    +     "<th>5min</th>\n"
    +     "<th>15min</th>\n"
    +     "<th>RPCs</th>\n"    // Threads
    +     "<th>HI</th>\n"
    +     "<th>Norm</th>\n"
    + "</tr></thead>\n"
    + "<tbody>\n"
    + "%tableRow{"
    + "  <tr>"
    + "    <td><a href='Remote?Node=%host'>%node</a></td>"
    + "    <td>%num_cpus</td>"
    + "    <td>%num_keys</td>"
    + "    <td>%val_size</td>"
    + "    <td>%free_mem</td>"
    + "    <td>%tot_mem</td>"
    + "    <td>%max_mem</td>"
    + "    <td>%free_disk</td>"
    + "    <td>%max_disk</td>"
    + "    <td>%cpu_util</td>"
    + "    <td>%cpu_load_1</td>"
    + "    <td>%cpu_load_5</td>"
    + "    <td>%cpu_load_15</td>"
    + "    <td>%rpcs</td>"
    + "    <td style='%queueStyleHi'>%fjthrds_hi / %fjqueue_hi</td>"
    + "    <td style='%queueStyleLo'>%fjthrds_lo / %fjqueue_lo</td>"
    + "    <td>%tcps_active</td>"
    + "  </tr>\n"
    + "}"
    + "</tbody>"
    + "</table>\n"
    + "<p>%voting  %locked\n"
    ;
}
