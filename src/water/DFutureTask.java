package water;
import java.net.DatagramPacket;
import java.util.Hashtable;
import java.util.concurrent.Delayed;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

  // Distribution means UDP-packet sending, which means packing and unpacking
  // UDP packets.  Record enough info for subclasses to build UDP packets.
  // Purely a convenience arg; because all we can do is serialize/de-serialize
  // into the UDP buffer and that is typically very expensive.
  final Object[] _args;

  // The set of current, pending tasks.  Basically a map from task# to DFutureTask.
  static Hashtable<Integer,DFutureTask> TASKS = new Hashtable<Integer,DFutureTask>();

  // Make a remotely executed FutureTask.  Must name the remote target as well
  // as the remote function.  This function is expected to be subclassed.
  public DFutureTask( H2ONode target, UDP.udp type, Object... args ) {
    assert type != UDP.udp.ack;
    _target = target;
    _tasknum = Locally_Unique_TaskIDs.getAndIncrement();
    _type = type;
    _args = args;
    _started = System.currentTimeMillis();
    _retry = RETRY_MS;
    // Keep a global record, for awhile
    TASKS.put(_tasknum,this);
  }

  // 'Pack' a UDP packet, by default.  Override this if you want to use a more
  // clever encoding.  By default Serialize the 'args' array.
  protected int pack( DatagramPacket pack ) {
    if( _args == null || _args.length == 0 ) return UDP.SZ_TASK;
    throw new Error("unimplemented: pack that UDP buffer from _args");
  }

  // 'Unpack' a UDP reply packet.  Override this if you want to use a more
  // clever result.  By default de-serialize a 'V' from the packet
  protected V unpack(DatagramPacket pack) {
    throw new Error("unimplemented: unpack that UDP buffer into a V");
  }

  // Hit the Timeout.  Mostly just auto-resend packet.  Can be overridden if
  // subclass has a better clue about timeouts.
  protected void resend() {
    // We could be racing timeouts-vs-replies.  Blow off timeout if we have an answer.
    if( _res != null )
      return;
    // We could be repeated versions of the same *identical* Future; this
    // will happen if the Future is slow and is getting enqueued multiple times
    // by the retries.  Kill off dups.
    if( UDPTimeOutThread.PENDING.contains(this) )
      return;
    // Default strategy: re-fire the packet and re-start the timeout.  We're
    // not counting failures or 'nuttin.  Just keep hammering the target until
    // we get an answer.
    fire_and_forget();
    // Put self on the "TBD" list of tasks awaiting Timeout.
    // So: dont really 'forget' but remember me in a little bit.
    UDPTimeOutThread.PENDING.add(this);
  }

  // Got a response packet.  Install it as The Answer packet and wake up
  // anybody waiting on an answer.
  protected void response( DatagramPacket p ) {
    V res = _res;                      // Read result, incase of dup ACKs
    if( res == null ) res = unpack(p); // ignore dup ACKs; just keep same answer
    synchronized(this) {        // Install the answer under lock
      if( _res == null ) _res = res;
      TASKS.remove(_tasknum);   // Flag as task-completed, even if the result is null
      notifyAll();              // And notify in any case
    }
    // ACKACK the remote, telling him "we got the answer"
    byte[] buf = p.getData();
    UDP.set_ctrl(buf,UDP.udp.ackack.ordinal());
    MultiCast.singlecast(_target,buf,UDP.SZ_TASK);
    UDPReceiverThread.free_pack(p);
    UDPTimeOutThread.PENDING.remove(this);
  }

  // Similar to FutureTask.get() but does not throw any exceptions.  Returns
  // null for canceled tasks, including those where the target dies.  Target
  // death vs null-result can be determined by calling isCancelled.
  public V get() {
    if( _res != null ) return _res; // Fast-path shortcut
    // Use FJP ManagedBlock for this blocking-wait - so the FJP can spawn
    // another thread if needed.
    try { H2O.FJP.managedBlock(this); } catch( InterruptedException e ) { }
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
        throw new Error("unimplemented");
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
        TASKS.remove(_tasknum);
      }
      notifyAll();              // notify in any case
    }
    UDPTimeOutThread.PENDING.remove(this);
    return did;
  }

  // ---
  static final long RETRY_MS = 100; // Initial UDP packet retry in msec
  // How long until we should do the "timeout" action?
  public long getDelay( TimeUnit unit ) {
    long now_ms = System.currentTimeMillis();
    long age_ms = now_ms - _started;
    // Double retry until we exceed existing age.
    // This is the time to delay until we try again.
    if( _retry < age_ms ) _retry<<=1;
    return unit.convert( _retry - age_ms, TimeUnit.MILLISECONDS );
  }
  // Needed for the DelayQueue API
  public final int compareTo( Delayed t ) {
    DFutureTask dt = (DFutureTask)t;
    return (int)((dt._started+dt._retry) - (_started+_retry));
  }
}

