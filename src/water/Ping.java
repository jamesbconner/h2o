package water;

import java.io.*;
import java.net.DatagramPacket;
import java.net.Socket;
import java.util.concurrent.*;

import jsr166y.ForkJoinPool;
import jsr166y.ForkJoinPool.ManagedBlocker;


/**
 * Class to test TCP/UDP connection between two nodes.
 *
 * For UDP, we test that we can send and receive UDP packet(identified by current time).
 * TCP check it can establish a connection write/read small data (8 bytes) and get ack back.
 *
 * Each test will be tried MAX_RETRIES times, with the given timeout.
 *
 * Use via two methods: sendPing is non blocking, testConnection is blocking.
 *
 *
 * @author tomasnykodym
 *
 */
public class Ping implements Future<Boolean>, ManagedBlocker, Runnable {

  private static final ConcurrentHashMap<Long, Ping> _pings = new ConcurrentHashMap<Long, Ping>();

  public static final int MAX_RETRIES = 5;
  public static final int TIMEOUT     = 100;

  H2ONode                 _target;
  protected int           _nretries;
  private long            _t1;
  protected boolean       _tstTCP;
  protected boolean       _tstUDP;

  protected int           _udpTimeout = -1;
  protected int           _tcpTimeout = -1;
  protected Thread        _thread;



  private static final PingTimeoutThread             _timeoutThread;
  static {
    _timeoutThread = new PingTimeoutThread();
    _timeoutThread.start();
  }

  private Ping(H2ONode target, boolean tstUDP, boolean tstTCP) {
    _nretries = 0;
    _t1 = System.currentTimeMillis();
    _target = target;
    _tstUDP = tstUDP;
    _tstTCP = tstTCP;
  }

  /**
   * Non blocking send of a ping.
   *
   * @param target
   * @param testUDP
   * @param testTCP
   * @return
   */
  public static Ping sendPing(H2ONode target, boolean testUDP, boolean testTCP){
    Ping p;
    synchronized( _pings ) {
      p = new Ping(target, testUDP, testTCP);
      while( _pings.containsKey(p._t1) )
        ++p._t1;
      _pings.put(p._t1, p);
      _pings.notifyAll();
    }
    return p;
  }
  /**
   * Blocking send of a ping.
   * @param target
   * @param testUDP
   * @param testTCP
   * @return
   */
  public static Ping testConnection(H2ONode target, boolean testUDP, boolean testTCP) {
    Ping p = sendPing(target, testUDP, testTCP);
    try{p.get();}catch(Exception e){throw new Error(e);}
    return p;
  }


  // TCP test thread
  @Override
  public void run() {
    TCPReceiverThread.TCPS_IN_PROGRESS.addAndGet(1);
    Socket sock = null;
    final int old_prior = Thread.currentThread().getPriority();
    try {
      Thread.currentThread().setPriority(Thread.MAX_PRIORITY - 1);
      long t1 = System.currentTimeMillis();
      sock = new Socket(_target._key._inet, _target._key.tcp_port());
      DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
          sock.getOutputStream()));
      // Write out the initial operation & key
      dos.writeByte(UDP.udp.ping.ordinal());
      dos.writeShort(H2O.UDP_PORT);
      dos.writeLong(0);
      dos.flush(); // Flush before trying to get the ack
      InputStream is = sock.getInputStream();
      int ack = is.read(); // Read 1 byte of ack
      if( ack != 99 ) throw new IOException("missing tcp ack " + ack);
      _tcpTimeout = (int) (System.currentTimeMillis() - t1);
      if(isDone()) synchronized(this){
        notifyAll();
      }
    } catch( Exception e ) {
      //e.printStackTrace();
    } finally {
      Thread.currentThread().setPriority(old_prior);
    }
  }



  protected void resend() {
    if( _thread != null ) {
      _thread.interrupt();
      _thread.stop();
      _thread = null;
    }

    if( ++_nretries <= MAX_RETRIES ) {
      if( _tstUDP && _udpTimeout == -1 ) {
        DatagramPacket p = UDPReceiverThread.get_pack();
        byte[] buf = p.getData();
        UDP.clr_port(buf);
        UDP.set_ctrl(buf, UDP.udp.ping.ordinal());
        UDP.set8(buf, UDP.SZ_PORT, _t1);
        _target.send(p, UDP.SZ_PORT + 8);
      }
      if( _tstTCP && _tcpTimeout == -1 ) {
        _thread = new Thread(this);
        _thread.start();
      }
    }
  }

  public static class RemoteHandler extends UDP {
    @Override
    void call(DatagramPacket p, H2ONode h2o) {
      byte [] b = p.getData();
      UDP.set_ctrl(b,UDP.udp.pAck.ordinal());
      int port = UDP.get_port(b);
      UDP.clr_port(b);
      MultiCast.singlecast(p.getAddress(), port, b, p.getLength());
    }

    @Override
    void tcp_read_call(DataInputStream dis, H2ONode h2o) throws IOException {
      dis.readLong();
    }
  }

  public static class UDPpingAck extends UDP {
    @Override
    void call(DatagramPacket p, H2ONode h2o) {
      int port = UDP.get_port(p.getData());
      long t = UDP.get8(p.getData(), UDP.SZ_PORT);
      Ping ping = null;
      ping = _pings.get(t);
      if( ping != null && ping._target != null ) {
        if( ping._target._key._inet.equals(p.getAddress())
            && ping._target._key._port == port ) {
          // _pings.remove(t);
          synchronized( ping ) {
            ping._udpTimeout = (int) (System.currentTimeMillis() - t);
            if( ping.isDone() ) ping.notifyAll();
          }
        }
      }
    }
  }

  @Override
  public Boolean get() throws InterruptedException, ExecutionException {
    if( isDone() ) return connectionOk(); // Fast-path shortcut
    try {
      ForkJoinPool.managedBlock(this);
    } catch( InterruptedException e ) {
    }
    assert isDone();
    return connectionOk();
  }

  @Override
  public Boolean get(long timeout, TimeUnit unit) throws InterruptedException,
      ExecutionException, TimeoutException {
    throw new UnsupportedOperationException();
  }

  // Done if target is dead or canceled
  public final boolean isCancelled() {
    return _target == null;
  }

  // Attempt to cancel job
  public final boolean cancel(boolean mayInterruptIfRunning) {
    boolean did = false;
    synchronized( this ) { // Install the answer under lock
      if( !isDone() ) {
        _target = null;
      }
      notifyAll(); // notify in any case
    }
    return did;
  }

  public boolean testTcp(){return _tstTCP;}
  public boolean testUdp(){return _tstUDP;}

  public boolean tcpOk(){
    return !_tstTCP || _tcpTimeout >= 0;
  }
  public boolean udpOk(){
    return !_tstUDP || _udpTimeout >= 0;
  }
  public boolean connectionOk(){
    assert isDone();
    return tcpOk() && udpOk();
  }
  @Override
  public boolean isDone() {
    if(!_tstTCP && _tcpTimeout != -1) return true;
    if(!_tstUDP && _udpTimeout != -1) return true;
    return _tcpTimeout != -1 && _udpTimeout != -1;
  }

  public boolean isReleasable() {
    return isDone();
  }

  // Possibly blocks the current thread. Returns true if isReleasable would
  // return true. Used by the FJ Pool management to spawn threads to prevent
  // deadlock is otherwise all threads would block on waits.
  public boolean block() {
    synchronized( this ) {
      while( !isDone() ) {
        // Wait until we get an answer.
        try {
          wait();
        } catch( InterruptedException e ) {
        }
      }
    }
    return true;
  }

  // thread to manage timeouts/retries
  private static class PingTimeoutThread extends Thread {
    @Override
    public void run() {
      try {
        while( true ) {
          sleep(TIMEOUT);
          synchronized( _pings ) {
            if( _pings.isEmpty() ) _pings.wait();
            for( long n : _pings.keySet() ) {
              Ping p = _pings.get(n);
              if(p._nretries > MAX_RETRIES)
                synchronized(p){
                  System.err.println("Ping timeouted");
                  if(p._tcpTimeout == -1)p._tcpTimeout = -2;
                  if(p._udpTimeout == -1)p._udpTimeout = -2;
                  p.notifyAll();
                }
              if(p.isDone()) {
                // ping timeouted, cancel it
                _pings.remove(n);
                continue;
              }
              p.resend();
            }
          }
        }
      } catch( Exception e ) {
        e.printStackTrace();
      }
    }
  }
}
