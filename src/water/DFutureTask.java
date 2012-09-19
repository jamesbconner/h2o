package water;
import java.io.*;
import java.net.DatagramPacket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import water.serialization.RemoteTaskSerializationManager;
import water.serialization.RemoteTaskSerializer;

import jsr166y.ForkJoinPool;

/**
 * A remotely executed FutureTask
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class DFutureTask<V> implements Future<V>, Delayed, ForkJoinPool.ManagedBlocker {

  // The target remote node to pester for a response.
  // NULL'd out if the target disappears or we cancel things.
  H2ONode _target;

  // The result; set once from null to final answer
  volatile V _res;

  // A locally-unique task number; a "cookie" handed to the remote process that
  // they hand back with the response packet.  These *never* repeat, so that we
  // can tell when a reply-packet points to e.g. a dead&gone task.
  final int _tasknum;
  static AtomicInteger Locally_Unique_TaskIDs = new AtomicInteger(1);

  // The major type of thingy we want done
  final UDP.udp _type;

  // Time we started this sucker up.  Controls re-send behavior.
  final long _started;
  long _retry;                  // When we should attempt a retry

  // The set of current, pending tasks.  Basically a map from task# to DFutureTask.
  static public ConcurrentHashMap<Integer,DFutureTask<?>> TASKS = new ConcurrentHashMap<Integer,DFutureTask<?>>();

  // Make a remotely executed FutureTask.  Must name the remote target as well
  // as the remote function.  This function is expected to be subclassed.
  public DFutureTask( H2ONode target, UDP.udp type ) {
    assert type != UDP.udp.ack;
    _target = target;
    _tasknum = Locally_Unique_TaskIDs.getAndIncrement();
    _type = type;
    _started = System.currentTimeMillis();
    _retry = RETRY_MS;
  }

  // 'Pack' a UDP packet, by default.  Override this if you want to use a more
  // clever encoding.  By default Serialize the 'args' array.
  protected int pack( DatagramPacket pack ) {
    throw H2O.unimpl();           // pack that UDP buffer from _args
  }

  // 'Unpack' a UDP reply packet.  Override this if you want to use a more
  // clever result.  By default de-serialize a 'V' from the packet
  protected V unpack(DatagramPacket pack) {
    throw H2O.unimpl();           // unpack that UDP buffer into a V
  }

  // Hit the Timeout.  Mostly just auto-resend packet.  Can be overridden if
  // subclass has a better clue about timeouts.
  protected void resend() {
    synchronized(this) {
      // Keep a global record, for awhile
      TASKS.put(_tasknum,this);
      // We could be racing timeouts-vs-replies.  Blow off timeout if we have an answer.
      if( isDone() ) {
        TASKS.remove(_tasknum);
        return;
      }
      // Default strategy: re-fire the packet and re-start the timeout.  We're
      // not counting failures or 'nuttin.  Just keep hammering the target until
      // we get an answer.
      fire_and_forget();
      // Double retry until we exceed existing age.  This is the time to delay
      // until we try again.  Note that we come here immediately on creation,
      // so the first doubling happens before anybody does any waiting.
      _retry<<=1;
      // Put self on the "TBD" list of tasks awaiting Timeout.
      // So: dont really 'forget' but remember me in a little bit.
      assert !UDPTimeOutThread.PENDING.contains(this);
      UDPTimeOutThread.PENDING.add(this);
    }
  }

  // Got a response packet.  Install it as The Answer packet and wake up
  // anybody waiting on an answer.
  protected void response( DatagramPacket p ) {
    V res = _res;                      // Read result, incase of dup ACKs
    if( res == null ) res = unpack(p); // ignore dup ACKs; just keep same answer
    synchronized(this) {        // Install the answer under lock
      if( _res == null ) _res = res;
      UDPTimeOutThread.PENDING.remove(this);
      TASKS.remove(_tasknum);   // Flag as task-completed, even if the result is null
      notifyAll();              // And notify in any case
    }
    // ACKACK the remote, telling him "we got the answer"
    byte[] buf = p.getData();
    UDP.set_ctrl(buf,UDP.udp.ackack.ordinal());
    UDP.clr_port(buf); // Re-using UDP packet, so side-step the port reset assert
    MultiCast.singlecast(_target,buf,UDP.SZ_TASK);
    UDPReceiverThread.free_pack(p);
  }

  // Similar to FutureTask.get() but does not throw any exceptions.  Returns
  // null for canceled tasks, including those where the target dies.  Target
  // death vs null-result can be determined by calling isCancelled.
  public V get() {
    if( _res != null ) return _res; // Fast-path shortcut
    // Use FJP ManagedBlock for this blocking-wait - so the FJP can spawn
    // another thread if needed.
    try { ForkJoinPool.managedBlock(this); } catch( InterruptedException e ) { }
    assert isDone();
    return _res;
  }
  // Return true if blocking is unnecessary, which is true if the Task isDone.
  public boolean isReleasable() {  return isDone();  }
  // Possibly blocks the current thread.  Returns true if isReleasable would
  // return true.  Used by the FJ Pool management to spawn threads to prevent
  // deadlock is otherwise all threads would block on waits.
  public boolean block() {
    synchronized(this) {
      while( !isDone() ) {
        // Wait until we get an answer.
        try { wait(); } catch( InterruptedException e ) { }
      }
    }
    return true;
  }

  public final V get(long timeout, TimeUnit unit) {
    if( _res != null ) return _res; // Fast-path shortcut
    synchronized(this) {
      while( !isDone() ) {
        // Wait until we get an answer.
        throw H2O.unimpl();
        //try { wait(); } catch( InterruptedException e ) { }
      }
    }
    return _res;
  }

  // Make a UDP packet.  Fire it to the target.  Put it on my worklist
  // of pending DFutureTasks awaiting a return packet.
  private void fire_and_forget() {
    DatagramPacket p = get_pack();
    int len = pack(p);          // Call users' arg-fill code
    // Assert user did not crush the 1st SZ_TASK bytes
    byte[] buf = p.getData();
    assert UDP.get_ctrl(buf) == _type.ordinal();
    assert UDP.get_task(buf) == _tasknum;
    // Targeted UDP packet delivery
    _target.send(p,len);
    // Done with packet
    UDPReceiverThread.free_pack(p);
  }

  // Get a fresh UDP packet.  Pre-fill the 1st SZ_TASK bytes of UDP packet-type
  // and task number.
  protected final DatagramPacket get_pack() {
    DatagramPacket p = UDPReceiverThread.get_pack(); // Get a fresh empty packet
    final byte[] buf = p.getData();
    UDP.set_ctrl(buf,_type.ordinal());
    UDP.set_task(buf,_tasknum);
    return p;
  }

  // Called on the REMOTE Node; finish up & send a single reply packet.
  static void reply(DatagramPacket p, int len, H2ONode h2o) {
    byte[] buf = p.getData();
    UDP.set_port(buf,H2O.UDP_PORT); // Always acks set reply port
    H2O.VOLATILE = 0;           // Dummy volatile write
    // This is a response to a remote task-type.  The response task# is in bytes
    // 3-7.  One-shot set the response to be a standard 'ack'.  Forever-more,
    // this packet is the Answer to the original work request.  Volatile-write
    // between setting the ACK byte setting the rest of the answer packet.
    UDP.set_ctrl(buf,UDP.udp.ack.ordinal());
    // Ship the reply to the original caller.
    if( h2o != null ) h2o.send(p,len);
  }

  // Done if target is dead or canceled, or we have a result.  Result is a fast
  // positive test.  NULL target is a fast negative test.  Else we have to check
  // the set of current tasks.
  public final boolean isDone() {
    return _target==null || _res != null || TASKS.get(_tasknum)==null;
  }
  // Done if target is dead or canceled
  public final boolean isCancelled() {
    return _target==null;
  }

  // Attempt to cancel job
  public final boolean cancel( boolean mayInterruptIfRunning ) {
    boolean did = false;
    synchronized(this) {        // Install the answer under lock
      if( !isCancelled() ) {
        did = true;             // Did cancel (was not canceled already)
        _target = null;         // Flag as canceled
        UDPTimeOutThread.PENDING.remove(this);
        TASKS.remove(_tasknum);
      }
      notifyAll();              // notify in any case
    }
    return did;
  }

  // ---
  // TCP large K/V send from the remote to the target.

  static boolean tcp_send( H2ONode h2o, UDP.udp udp_type, int tasknum, Object... args ) {
    TCPReceiverThread.TCPS_IN_PROGRESS.addAndGet(1);
    Socket sock = null;
    final int old_prior = Thread.currentThread().getPriority();
    try {
      Thread.currentThread().setPriority(Thread.MAX_PRIORITY-1);
      sock = new Socket( h2o._key._inet, h2o._key.tcp_port() );
      DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
      // Write out the initial operation & key
      dos.writeByte(udp_type.ordinal());
      dos.writeShort(H2O.UDP_PORT);
      dos.writeInt(tasknum);

      // Write all passed args.  Probably should an array of LineWire
      // interfaces so I do not need this evil instanceof-tree.
      for( int i=0; i<args.length; i++ ) {
        Object arg = args[i];
        if( arg instanceof Key ) {
          ((Key)arg).write(dos);
        } else if( arg instanceof RemoteTask ) {
          RemoteTask t = (RemoteTask)arg;
          RemoteTaskSerializer<RemoteTask> remoteTaskSerializer = RemoteTaskSerializationManager.get(t.getClass());
          remoteTaskSerializer.write(t, dos);
        } else if( arg instanceof Value ) {
          // For Values, support a pre-loaded byte[]
          if( i < args.length-1 && args[i+1] instanceof byte[] ) {
            ((Value)arg).write(dos,Integer.MAX_VALUE,(byte[])args[i+1]);
            i++;
          } else {
            ((Value)arg).write(dos,Integer.MAX_VALUE);
          }
        } else if( arg instanceof String ) {
          byte[] b = ((String)arg).getBytes();
          dos.writeShort(b.length);
          dos.write(b);
        } else if( arg instanceof Byte ) {
          dos.writeByte((Byte)arg);
        } else {
          throw H2O.unimpl(); // passing a arg.getClass()
        }
      }

      dos.flush(); // Flush before trying to get the ack
      InputStream is = sock.getInputStream();
      int ack = is.read(); // Read 1 byte of ack
      if( ack != 99 ) throw new IOException("missing tcp ack "+ack);
      sock.close();
      TCPReceiverThread.TCPS_IN_PROGRESS.addAndGet(-1);
      Thread.currentThread().setPriority(old_prior);
      return true;
    } catch( IOException e ) {
      TCPReceiverThread.TCPS_IN_PROGRESS.addAndGet(-1);
      Thread.currentThread().setPriority(old_prior);
      try { if( sock != null ) sock.close(); }
      catch( IOException e2 ) { /*no msg for error on closing broken socket */}
      // Be silent for SocketException; we get this if the remote dies and we
      // basically expect them.
      if( !(e instanceof SocketException) ) // We get these if the remote dies mid-write
        System.err.println("tcp socket failed "+e);
      return false;
    }
  }

  // Big sends are slow, especially on loaded networks.  We can timeout and
  // attempt retry before even sending once.  Track the TCP send logic - it
  // only can survive a single send (dups look like new writes), and it's a
  // large transfer - so dups are bad for performance also.
  boolean _tcp_started = false; // Only send TCP once, even if it is slow
  boolean tcp_send_pack( Object ... args ) {
    synchronized(this) {        // One-shot only, allow a TCP send
      if( _tcp_started ) return true;
      _tcp_started = true;
    }
    // NOT under lock, do the TCP send
    return tcp_send(_target,_type,_tasknum,args);
  }

  // ---
  static final long RETRY_MS = 200; // Initial UDP packet retry in msec
  // How long until we should do the "timeout" action?
  public long getDelay( TimeUnit unit ) {
    long delay = (_started+_retry)-System.currentTimeMillis();
    return unit.convert( delay, TimeUnit.MILLISECONDS );
  }
  // Needed for the DelayQueue API
  public final int compareTo( Delayed t ) {
    DFutureTask<?> dt = (DFutureTask<?>)t;
    long nextTime = _started+_retry, dtNextTime = dt._started+dt._retry;
    return nextTime == dtNextTime ? 0 : (nextTime > dtNextTime ? 1 : -1);
  }
}
