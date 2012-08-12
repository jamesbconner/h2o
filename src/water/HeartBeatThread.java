package water;
import java.io.File;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

/**
 * Starts a thread publishing multicast HeartBeats to the local subnet: the
 * Leader of this Cloud.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class HeartBeatThread extends Thread {
  
  // Time between heartbeats.  Strictly several iterations less than the
  // timeout.
  static final int SLEEP = 1000;

  // Timeout in msec before we decide to not include a Node in the next round
  // of Paxos Cloud Membership voting.
  static final int TIMEOUT = 600000;

  // Timeout in msec before we decide a Node is suspect, and call for a vote
  // to remove him.  This must be strictly greater than the TIMEOUT.
  static final int SUSPECT = TIMEOUT+500;

  // Receive queue depth count before we decide a Node is suspect, and call for a vote
  // to remove him.
  static public final int QUEUEDEPTH = 100;

  // The Run Method.
  // Started by main() on a single thread, this code publishes Cloud membership
  // to the Cloud once a second (across all members).  If anybody disagrees
  // with the membership Heartbeat, they will start a round of Paxos group
  // discovery.
  public void run() {
    Sigar sigar = new Sigar();      
     
    while( true ) {
      // Once per second, for the entire cloud a Node will multi-cast publish
      // itself, so other unrelated Clouds discover each other and form up.
      try { Thread.sleep(SLEEP); } // Only once-sec per entire Cloud
      catch( InterruptedException e ) { }

      // Update the interesting health self-info for publication also
      final Runtime run = Runtime.getRuntime();
      final H2ONode me  = H2O.SELF;
      final long maxmem = MemoryManager.MAXMEM = run.maxMemory();
      me.set_num_cpus(run.availableProcessors());
      me.set_free_mem(run. freeMemory());
      me.set_max_mem (           maxmem);
      me.set_tot_mem (run.totalMemory());
      me.set_keys    (H2O.STORE.size());
      me.set_valsz   (MemoryManager.USED.get());
      me.set_thread_count(Thread.currentThread().getThreadGroup().activeCount());
      me.set_fjqueue_depth(H2O.FJP.getQueuedSubmissionCount());
      // get the usable and total disk storage for the partition where the
      // persistent KV pairs are stored 
      if (PersistIce.ROOT==null) {
        me.set_free_disk(0); // not applicable
        me.set_max_disk(0); // not applicable
      } else {
        File f = new File(PersistIce.ROOT);
        me.set_free_disk(f.getUsableSpace());
        me.set_max_disk(f.getTotalSpace());
      }
      
      // get cpu utilization from sigar if available     
      try {          
        me.set_cpu_util(1.0 - sigar.getCpuPerc().getIdle());                          
      } catch (SigarException ex) {          
        me.set_cpu_util(-1.0);                  
      }
      
      // get cpu load from sigar if available
      try {          
        double [] cpu_load = sigar.getLoadAverage();                          
        me.set_cpu_load(cpu_load[0],cpu_load[1],cpu_load[2]);
      } catch (SigarException ex) {          
        me.set_cpu_load(-1.0,-1.0,-1.0);
      }
      
      // Announce what Cloud we think we are in.
      // Publish our health as well.
      H2O cloud = H2O.CLOUD;
      UDPHeartbeat.build_and_multicast(cloud);

      // If we have no internet connection, then the multicast goes
      // nowhere and we never receive a heartbeat from ourselves!
      // Fake it now.
      long now = System.currentTimeMillis();
      H2O.SELF._last_heard_from = now;

      // Look for napping Nodes & propose removing from Cloud
      for( H2ONode h2o : cloud._memary ) {
        if( now - h2o._last_heard_from > SUSPECT ) {  // We suspect this Node has taken a dirt nap
          Paxos.print_debug("hart: announce suspect node",cloud._memset,h2o.toString());
          Paxos.do_change_announcement(cloud);
          break;
        }
      }
    }
  }
}
