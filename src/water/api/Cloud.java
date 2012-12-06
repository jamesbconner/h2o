
package water.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Properties;
import water.H2O;
import water.H2ONode;
import water.Paxos;
import water.PrettyPrint;

/**
 *
 * @author peta
 */
public class Cloud extends Request {

  public static final String JSON_CLOUD_NAME = "cloud_name";
  public static final String JSON_NODE_NAME = "node_name";
  public static final String JSON_CLOUD_SIZE = "cloud_size";
  public static final String JSON_CONSENSUS = "consensus";
  public static final String JSON_LOCKED = "locked";
  public static final String JSON_NODES = "nodes";
  public static final String JSON_NODES_NAME = "name";
  public static final String JSON_NODES_NUM_CPUS = "num_cpus";
  public static final String JSON_NODES_FREE_MEM = "free_mem";
  public static final String JSON_NODES_TOT_MEM = "tot_mem";
  public static final String JSON_NODES_MAX_MEM = "max_mem";
  public static final String JSON_NODES_NUM_KEYS = "num_keys";
  public static final String JSON_NODES_VAL_SIZE = "val_size";
  public static final String JSON_NODES_FREE_DISK = "free_disk";
  public static final String JSON_NODES_MAX_DISK = "max_disk";
  public static final String JSON_NODES_CPU_UTIL = "cpu_util";
  public static final String JSON_NODES_CPU_LOAD_1 = "cpu_load_1";
  public static final String JSON_NODES_CPU_LOAD_5 = "cpu_load_5";
  public static final String JSON_NODES_CPU_LOAD_15 = "cpu_load_15";
  public static final String JSON_NODES_FJ_THREADS_HI = "fj_threads_hi";
  public static final String JSON_NODES_FJ_QUEUE_HI = "fj_queue_hi";
  public static final String JSON_NODES_FJ_THREADS_LO = "fj_threads_lo";
  public static final String JSON_NODES_FJ_QUEUE_LO = "fj_queue_lo";
  public static final String JSON_NODES_RPCS = "rpcs";
  public static final String JSON_NODES_TCPS_ACTIVE = "tcps_active";

  @Override public void serve(JsonObject response, Properties args) {
    final H2O cloud = H2O.CLOUD;
    final H2ONode self = H2O.SELF;
    response.addProperty(JSON_CLOUD_NAME, H2O.NAME);
    response.addProperty(JSON_NODE_NAME, self.toString());
    response.addProperty(JSON_CLOUD_SIZE, cloud._memary.length);
    response.addProperty(JSON_CONSENSUS, Paxos._commonKnowledge); // Cloud is globally accepted
    response.addProperty(JSON_LOCKED, Paxos._cloud_locked); // Cloud is locked against changes
    JsonArray nodes = new JsonArray();
    for (H2ONode h2o : cloud._memary) {
      JsonObject node = new JsonObject();
      node.addProperty(JSON_NODES_NAME,h2o.toString());
      node.addProperty(JSON_NODES_NUM_CPUS, h2o.get_num_cpus());
      node.addProperty(JSON_NODES_FREE_MEM, PrettyPrint.bytes(h2o.get_free_mem()));
      node.addProperty(JSON_NODES_TOT_MEM, PrettyPrint.bytes(h2o.get_tot_mem()));
      node.addProperty(JSON_NODES_MAX_MEM, PrettyPrint.bytes(h2o.get_max_mem()));
      node.addProperty(JSON_NODES_NUM_KEYS, h2o.get_keys());
      node.addProperty(JSON_NODES_VAL_SIZE, PrettyPrint.bytes(h2o.get_valsz()));
      node.addProperty(JSON_NODES_FREE_DISK, PrettyPrint.bytes(h2o.get_free_disk()));
      node.addProperty(JSON_NODES_MAX_DISK, PrettyPrint.bytes(h2o.get_max_disk()));
      double[] cpu_load = h2o.get_cpu_load();
      node.addProperty(JSON_NODES_CPU_LOAD_1, pos_neg(cpu_load[0]));
      node.addProperty(JSON_NODES_CPU_LOAD_5, pos_neg(cpu_load[1]));
      node.addProperty(JSON_NODES_CPU_LOAD_15, pos_neg(cpu_load[2]));
      node.addProperty(JSON_NODES_FJ_THREADS_HI, h2o.get_fjthrds_hi());
      node.addProperty(JSON_NODES_FJ_QUEUE_HI, h2o.get_fjqueue_hi());
      node.addProperty(JSON_NODES_FJ_THREADS_LO, h2o.get_fjthrds_lo());
      node.addProperty(JSON_NODES_FJ_QUEUE_LO, h2o.get_fjqueue_lo());
      node.addProperty(JSON_NODES_RPCS, h2o.get_rpcs());
      node.addProperty(JSON_NODES_TCPS_ACTIVE, h2o.get_tcps_active());
      nodes.add(node);
    }
    response.add(JSON_NODES,nodes);
  }

  @Override protected void createHTMLBuilders(HTMLBuilder builder) {
    builder.setBuilder(JSON_NODES+"."+JSON_NODES_NAME, builder.new LinkTableCellBuilder() {
      @Override protected String makeHref(JsonElement value) {
        return "Remote.html?node="+DOM.urlEncode(value.getAsString());
      }
    });
  }

  public static String pos_neg(double d) {
    return d >= 0 ? String.valueOf(d) : "n/a";
  }

}
