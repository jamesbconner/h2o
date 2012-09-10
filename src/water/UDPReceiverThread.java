package water;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * The Thread that looks for UDP Cloud requests.
 *
 * This thread just spins on reading UDP packets from the kernel and either
 * dispatching on them directly itself (if the request is known short) or
 * queuing them up for worker threads.  Evilly, I assume a fairly high packet
 * churn rate on packets with a 1600-byte buffer and attempt to recycle packets
 * myself.
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class UDPReceiverThread extends Thread {

  // Synchronized LIFO list of free UDP packets.  Sync'd because multiple
  // worker threads will push free packs on the list.  Stack, because of
  // marginally better cache behavior.
  private static ArrayList<DatagramPacket> FREELIST = new ArrayList<DatagramPacket>();

  // Pull from the stack
  static DatagramPacket get_pack() {
    DatagramPacket p = null;
    synchronized(FREELIST) {
      int sz = FREELIST.size();
      if( sz > 0 ) p = FREELIST.remove(sz-1);
    }
    // Free list is empty, so make a new one
    if( p == null )
      return new DatagramPacket(new byte[1600],1600);
    assert clobbered(p.getData());
    return p;
  }
  // Push to the free stack
  static void free_pack(DatagramPacket pack) {
    assert !on_free_list(pack);
    assert clobber(pack.getData());
    synchronized(FREELIST) { FREELIST.add(pack); }
  }
  static boolean clobber(byte[] b) { Arrays.fill(b,(byte)0xab); return true; }
  static boolean clobbered(byte[] b) {
    for( int i=0; i<64; i++ )
      if( b[i] != (byte)0xab )
        return false;
    for( int i=0; i<64; i++ )
      if( b[b.length-1-i] != (byte)0xab )
        return false;
    return true;
  }
  static boolean on_free_list(DatagramPacket pack) {
    synchronized(FREELIST) { return FREELIST.contains(pack); }
  }

  // The Run Method.
  // ---
  // Started by main() on a single thread, this code manages reading UDP packets 
  public void run() {
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY-1);
    DatagramSocket sock = null, errsock = null;
    boolean saw_error = false;
    final int ACK = UDP.udp.ack.ordinal();
    int unknown_packets_per_sec = 0;
    long unknown_packet_time = 0;

    // Loop forever accepting Cloud Management requests
    while( true ) {
      // Get a free datagram packet
      DatagramPacket pack = get_pack();
      
      try { 
        // ---
        // Cleanup from any prior socket failures.  Rare unless we're really sick.
        if( errsock != null ) { // One time attempt a socket close
          final DatagramSocket tmp2 = errsock; errsock = null;
          tmp2.close();       // Could throw, but errsock cleared for next pass
        }
        if( saw_error ) Thread.sleep(1000); // prevent deny-of-service endless socket-creates
        saw_error = false;

        // ---
        // Common-case setup of a socket
        if( sock == null )
          sock = new DatagramSocket(H2O.UDP_PORT);

        // Receive a packet
        sock.receive(pack);
        TimeLine.record_recv(pack);
        
      } catch( Exception e ) {
        // On any error from anybody, close all sockets & re-open
        System.err.println("UDP Receiver error on port "+H2O.UDP_PORT+" error "+e); 
        saw_error = true;
        errsock  = sock ;  sock  = null; // Signal error recovery on the next loop
      }

      // Look at the packet.
      byte[] pbuf = pack.getData();
      int first_byte = UDP.get_ctrl(pbuf);
      assert first_byte != 0xab; // did not receive a clobbered packet?

      // Get the Cloud we are operating under for this packet
      H2O cloud = H2O.CLOUD;
      // Get the H2ONode (many people use it).
      H2ONode h2o = H2ONode.intern(pack.getAddress(),UDP.get_port(pbuf));
      // Record the last time we heard from any given Node
      h2o._last_heard_from = System.currentTimeMillis();

      // Check cloud membership; stale ex-members are "fail-stop" - we mostly
      // ignore packets from them (except paxos packets).
      boolean is_member = cloud._memset.contains(h2o);
      
      // Snapshots are handled *IN THIS THREAD*, to prevent more UDP packets
      // from being handled during the dump.
      if( is_member && first_byte == UDP.udp.timeline.ordinal() ) {
        UDP.udp.timeline._udp.call(pack,h2o);
        free_pack(pack);
        continue;
      }

      // Suicide packet?  Short-n-sweet...
      if( first_byte == UDP.udp.rebooted.ordinal() && pbuf[UDP.SZ_PORT]==2 )
        System.exit(-1);

      // Paxos stateless packets & ACKs just fire immediately in a worker
      // thread.  Dups are handled by these packet handlers directly.  No
      // current membership check required for Paxos packets
      if( UDP.udp.UDPS[first_byte]._paxos ||
          (is_member && first_byte <= ACK) ) {
        H2O.FJP_HI.execute(new FJPacket(pack,h2o));
        continue;
      }
      
      // Log packets (only from members) are handled separately.
      if( is_member && first_byte == UDP.udp.log.ordinal() ) {
        // TODO: use FJP_HI or FJP_NORM
        UDP.udp.log.pool().execute(new FJPacket(pack,h2o));
        continue;
      }

      // Some non-Paxos packet from a non-member.  Probably should record &
      // complain.
      if( !is_member ) {
        // Filter unknown-packet-reports.  In bad situations of poisoned Paxos
        // voting we can get a LOT of these packets/sec, flooding the console.
        unknown_packets_per_sec++;
        long timediff = h2o._last_heard_from - unknown_packet_time;
        if( timediff > 1000 ) {
          System.err.println("Non-member packets: "+unknown_packets_per_sec+"/sec, last one from "+h2o);
          unknown_packets_per_sec = 0;
          unknown_packet_time = h2o._last_heard_from;
        }
        free_pack(pack);
        continue;
      }

      // Convert Unreliable DP to Reliable DP.
      // If we got this packet already (it is common for the sender to send
      // dups), we do not want to do the work *again* - so we do not want to
      // enqueue it for work.  Also, if we've *replied* to this packet before
      // we just want to send the dup reply back.
      DatagramPacket old = h2o.putIfAbsent(pack);
      if( old != null ) {       // We've seen this packet before?
        byte[] obuf = old.getData();
        if( UDP.get_ctrl(obuf) == ACK ) {
          int dum = H2O.VOLATILE; // Dummy volatile read between 1st byte & rest of packet
          // This is an old re-send of the same thing we've answered to
          // before.  Send back the same old answer ACK.
          h2o.send(old,old.getLength());
        } else {
          // This packet has not been ACK'd yet.  Hence it's still a
          // work-in-progress locally.  We have no answer yet to reply with
          // but we do not want to re-offer the packet for repeated work.
          // Just ignore the packet.
        }
        free_pack(pack);
      } else {                  // Else not a repeat-packet
        // Announce new packet to workers.
        // "rexec" goes to "normal" priority queue.
        // gets/puts go to "high" priority queue.
        UDP.udp.UDPS[first_byte].pool().execute(new FJPacket(pack,h2o));
      }
    }
  }
}
