package water;
import java.util.*;

/**
 * Paxos
 *
 * Used to define Cloud membership.  See:
 *   http://en.wikipedia.org/wiki/Paxos_%28computer_science%29
 *
 * This Paxos implementation communicates via combination of multi-cast on the
 * local subnet and point-to-point.  Multi-cast is used to announce the
 * existence of *this* Cloud to any other Cloud in the subnet - which is
 * basically a Client request to all Servers to run a subnet-local round of
 * leadership and membership.  If all Servers who hears this request are
 * already in the same Cloud, then no action is required.

 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public abstract class Paxos {
  // This is also a convenience class for understanding a complex high-usage
  // UDP packet.  We have one internal packet for holding local-Node state and
  // for sending that state to other Nodes, and we have wrapper functions to
  // parse the identical packet received from other Nodes.
  //
  // The UDP packet layout is the current proposal value (wireline membership)
  // plus the proposal number (8 bytes), so we can add 8 more bytes of Promise
  // if more Proposals float by, plus the UDP Packet type.
  static final int udp_off = 0;
  static final int port_off = 1;
  static final int promise_off  = port_off+2; // 8 bytes of Promise to ignore lessthan
  static final int old_proposal_off = promise_off+8; // 8 bytes of prior accepted proposal
  // The remainder of the UDP packet is the *Value* previously accepted
  static final int id_lo_off = old_proposal_off+8;
  static final int id_hi_off = id_lo_off+8;
  static final int member_cnt_off = id_hi_off+8; // 2 bytes of member count
  static final int member_off = member_cnt_off+2; // Start of member IP addresses

  // We have a one-off internal buffer.
  // This buffer serves many duties:
  // - Direct UDP output buffer
  // - Storage of most recent proposal
  // - Storage of most recent promise
  // - Wire-line version of membership list
  static byte[] BUF = new byte[member_off];

  static H2ONode LEADER = H2O.SELF;        // Leader has the lowest IP of any in the set
  static HashSet<H2ONode> PROPOSED_MEMBERS = new HashSet();
  static HashSet<H2ONode> ACCEPTED = new HashSet();
  static { PROPOSED_MEMBERS.add(H2O.SELF);  }

  // If we receive a Proposal, we need to see if it larger than any we have
  // received before.  If so, then we need to Promise to honor it.  Also, if we
  // have Accepted prior Proposals, we need to include that info in any Promise.
  static long PROPOSAL_MAX;

  // Whether or not we have common knowledge
  public static volatile boolean _commonKnowledge = false; 
  // Whether or not we're allowing distributed-writes.  The cloud is not
  // allowed to change shape once we begin writing.
  public static volatile boolean _cloud_locked = false; 

  // ---
  // This is a packet announcing what Cloud this Node thinks is the current
  // Cloud.
  static synchronized void do_heartbeat( H2ONode h2o ) {
    // If this packet is for *this* Cloud, just carry on (the heartbeat has
    // already been recorded.
    H2O cloud = H2O.CLOUD;
    if( h2o.is_cloud_member(cloud) ) {
      // However, do a 1-time printing when we realize all members of the cloud
      // are mutally agreed upon a new cloud shape.  This is not the same as a
      // Paxos vote, which only requires a Quorum.  This happens after everybody
      // has agreed to the cloud AND published that result to this node.
      boolean ck = true;        // Assume true
      for( H2ONode h2o2 : cloud._memary )
        if( !h2o2.is_cloud_member(cloud) )
          ck = false;           // This guy has not heartbeat'd that "he's in"
      if( ck == false && _commonKnowledge == true && _cloud_locked )
        cloud_kill(); // Cloud-wide kill because things changed after key inserted
      if( !_commonKnowledge && ck ) { // Everybody just now agrees on the Cloud
        Paxos.class.notify(); // Also, wake up a worker thread stuck in DKV.put
        System.out.printf("[h20] Paxos Cloud of size %d formed: %s\n",
                          cloud._memset.size(), cloud._memset.toString());
      }
      _commonKnowledge = ck;    // Set or clear "common knowledge"
      return;                   // Do nothing!
    }

    // Mismatched cloud
    print_debug("hart: mismatched cloud announcement",h2o);

    // If this dude is supposed to be *in* our Cloud then maybe it's a slow or
    // delayed heartbeat packet, or maybe he's missed the Accepted announcement.
    // In either case, pound the news into his head.
    if( cloud._memset.contains(h2o) ) {
      if( H2O.isIDFromPrevCloud(h2o) ) {
        // In situations of rapid cloud formation, we could have:
        // A Cloud of {A,B} is voted on.
        // A Cloud of {A,B,C,D} is voted on by A,C,D forming a quorum.  B is slow.
        // New members E,F,G appear to A, so A's proposed list is now {A-G}
        // B, still slow, heartbeats cloud {A,B}
        // At this point: B is in {A,B}, A is in {A,B,C} which includes B,
        // but A is busy working on {A-G}.
        print_debug("hart: is member but did not get the news1",cloud._memset);
        print_debug("hart: is member but did not get the news2",read_members(BUF));
        assert cloud._memset.containsAll(read_members(BUF)); // We expect only to add nodes
        if( PROPOSED_MEMBERS.equals(cloud._memset) ) // But if things are not moving fast...
          UDPPaxosAccepted.build_and_multicast(BUF); // Then try to update the slow guy
        return;
      } else {
        // Trigger new round of Paxos voting: remove this guy from our cloud
        // (since he thinks he does not belong), and try again.
        PROPOSED_MEMBERS.remove(h2o);
      }
    } else {
      // Got a heartbeat from some dude not in the Cloud.  Probably napping
      // Node woke up and hasn't yet smelled the roses (i.e., isn't aware the
      // Cloud shifted and kicked him out).  Could be a late heartbeat from
      // him.  Offer to vote him back in.
      if( !PROPOSED_MEMBERS.add(h2o) )
        print_debug("hart: already part of proposal",PROPOSED_MEMBERS);
    }

    // Trigger a Paxos proposal because there is somebody new, or somebody old
    do_change_announcement(cloud);
  }

  // Remove any laggards.  Laggards should notice they are being removed from
  // the Cloud - and if they do they can complain about it and get
  // re-instated.  If they don't notice... then they need to be removed.
  // Recompute leader.
  static void remove_laggards() {
    long now = System.currentTimeMillis();
    LEADER = null;              // Recompute LEADER while we are at it
    for( Iterator<H2ONode> i = PROPOSED_MEMBERS.iterator(); i.hasNext(); ) {
      H2ONode h2o = i.next();
      // Check if node timed out
      long msec = now - h2o._last_heard_from;
      if( msec > HeartBeatThread.TIMEOUT ) {
        assert h2o != H2O.SELF; // Not timing-out self???
        print_debug("kill: Removing laggard ",h2o);
        i.remove();
      } else {                  // Else find the lowest IP address to be leader
        if( h2o.compareTo(LEADER) < 0 ) LEADER = h2o;
      }
    }
    return;
  }

  // Handle a mis-matched announcement; either a self-heartbeat has noticed a
  // laggard in our current Cloud, or we got a heartbeat from somebody outside
  // the cloud.  The caller must have already synchronized.
  static void do_change_announcement( H2O cloud ) {

    // Remove laggards and recompute leader
    remove_laggards();

    // At this point, we have changed the current Proposal: either added due to
    // a heartbeat from an outsider, or tossed out a laggard or both.

    // Look again at the new proposed Cloud membership.  If it matches the
    // existing Cloud... then we like things the way they are and we want to
    // ignore this change announcement.  This can happen if e.g. somebody is
    // trying to vote-in a laggard; they have announced a Cloud with the
    // laggard and we're skeptical.
    if( cloud._memset.equals(PROPOSED_MEMBERS) ) {
      assert cloud._memary[0] == LEADER;
      print_debug("chng: no change from current cloud, ignoring change request",PROPOSED_MEMBERS);
      return;
    }
    // Reset all memory of old accepted proposals; we're on to a new round of voting
    write_members_set_id();

    // The Leader Node for this proposed Cloud will act as the distinguished
    // Proposer of a new Cloud membership.  Non-Leaders will act as passive
    // Accepters.

    if( H2O.SELF == LEADER ) {
      // If we are proposing a leadership change, we need to do the Basic Paxos
      // algorithm from scratch.  If we are keeping the leadership but, e.g.
      // adding or removing a local Node then we can go for the Multi-Paxos
      // steady-state response.

      // See if we are changing cloud leaders?
      if( cloud._memary[0] == LEADER ) {
        // TODO
        //  throw new Error("Unimplemented: multi-paxos same-leader optimization");
      }

      // We're fighting over Cloud leadership!  We need to throw out a 'Prepare
      // N' so we can propose a new Cloud, where the proposal is bigger than
      // any previously submitted by this Node.  Note that only people who
      // think they should be leaders get to toss out proposals.
      ACCEPTED.clear(); // Reset the Accepted count: we got no takings on this new Proposal
      long proposal_num = PROPOSAL_MAX+1;
      UUID uuid = UUID.randomUUID();
      UDP.set8(BUF,id_lo_off,uuid.getLeastSignificantBits());
      UDP.set8(BUF,id_hi_off,uuid. getMostSignificantBits());
      Paxos.print_debug("send: Prepare "+proposal_num+" for leadership fight ",PROPOSED_MEMBERS);
      UDPPaxosProposal.build_and_multicast(proposal_num);
    } else {
      // Non-Leaders act as passive Accepters.  All Nodes should respond in a
      // timely fashion, including Leaders - if they fail the basic heartbeat
      // timeout, then they may be voted out of the Cloud...  which will start
      // another leadership fight at that time.  Meanwhile, this is effectively a
      // Paxos Client request to the Paxos Leader / Proposer ... not to us.
      // Ignore it.
      print_debug("do  : got cloud change request as an acceptor; ignoring it",PROPOSED_MEMBERS);
    }
  }

  // ---
  // This is a packet announcing a Proposal, which includes an 8-byte Proposal
  // number and the guy who sent it (and thinks he should be leader).
  static synchronized int do_proposal( final long proposal_num, final H2ONode proposer ) {
    print_debug("recv: Proposal num "+proposal_num+" by ",proposer);

    // We got a Proposal from somebody.  We can toss this guy in the world-
    // state we're holding but we won't be pulling in all HIS Cloud
    // members... we wont find out about them until other people start
    // announcing themselves... so adding this dude here is an optimization.
    if( PROPOSED_MEMBERS.add(proposer) ) {
      // Since he's new: would he be leader?
      if( proposer.compareTo(LEADER) < 0 )
        LEADER = proposer;
    }

    // Is the Proposal New or Old?
    if( proposal_num < PROPOSAL_MAX ) { // Old Proposal!  We can ignore it...
      // But we want to NAK this guy
      if( proposer != H2O.SELF )
        UDPPaxosNack.build_and_multicast(PROPOSAL_MAX-1, proposer);
      return print_debug("do_proposal NAK; self:" + H2O.SELF + " target:"+proposer + " proposal " + proposal_num, proposer);
    } else if( proposal_num == PROPOSAL_MAX ) { // Dup max proposal numbers?
      if( proposer == LEADER )                  // Ignore dups from leader
        return print_debug("do_proposal: ignoring duplicate proposal", proposer);
      // Ahh, a dup proposal from non-leader.  Must be an old proposal
      if( proposer != H2O.SELF )
        UDPPaxosNack.build_and_multicast(PROPOSAL_MAX, proposer);
      return print_debug("do_proposal NAK; self:" + H2O.SELF + " target:"+proposer + " proposal " + proposal_num, proposer);
    }

    // A new larger Proposal number appeared; keep track of it
    PROPOSAL_MAX = proposal_num;
    ACCEPTED.clear(); // If I was acting as a Leader, my Proposal just got whacked

    if( LEADER == proposer ) {    // New Proposal from what could be new Leader?
      // Now send a Promise to the Proposer that I will ignore Proposals less
      // than PROPOSAL_MAX (8 bytes) and include any prior ACCEPTED Proposal
      // number (8 bytes) and old the Proposal's Value
      assert old_proposal(BUF) < proposal_num; // we have not already promised what is about to be proposed
      set_promise(BUF,proposal_num);
      return UDPPaxosPromise.singlecast(BUF, proposer);
    }
    // Else proposal from some guy who I do not think should be leader in the
    // New World Order.  If I am not Leader in the New World Order, let Leader
    // duke it out with this guy.
    if( H2O.SELF != LEADER )
      return print_debug("do  : Proposal from non-leader to non-leader; ignore; leader should counter-propose",PROPOSED_MEMBERS);

    // I want to be leader, and this guy is messing with my proposal.  Try
    // again to make him realize I should be leader.
    do_change_announcement(H2O.CLOUD);
    return 0;
  }

  // Received a Nack on a proposal
  static synchronized void do_nack( byte[] buf, final H2ONode h2o ) {
    long proposal_num = promise(buf);
    print_debug("recv: Nack num "+proposal_num+" by ",h2o);

    if( PROPOSED_MEMBERS.add(h2o) ) {
      // Since he's new: would he be leader?
      if( h2o.compareTo(LEADER) < 0 )
        LEADER = h2o;
    }

    // Nacking the named proposal
    if( proposal_num >= PROPOSAL_MAX ) {
      PROPOSAL_MAX = proposal_num;       // At least bump proposal to here
      do_change_announcement(H2O.CLOUD); // Re-vote from the start
    }
  }

  // Recieved a Promise from an Acceptor to ignore Proposals below a certain
  // number.  Must be synchronized.  Buf is freed upon return.
  static synchronized int do_promise( byte[] buf, H2ONode h2o ) {
    print_debug("recv: Promise",PROPOSED_MEMBERS,buf);
    long promised_num = promise(buf);

    if( PROPOSAL_MAX==0 )
      return print_debug("do  : nothing, received Promise "+promised_num+" but no proposal in progress",PROPOSED_MEMBERS);

    // Check for late-arriving promise, after a better Leader has announced
    // himself to me... and I do not want to be leader no more.
    if( LEADER != H2O.SELF )
      return print_debug("do  : nothing: recieved promise ("+promised_num+"), but I gave up being leader" ,PROPOSED_MEMBERS);

    // Hey!  Got a Promise from somebody, while I am a leader!
    // I always get my OWN proposals, which raise PROPOSAL_MAX, so this is for
    // some old one.  I only need to track the current "largest proposal".
    if( promised_num < PROPOSAL_MAX )
      return print_debug("do  : nothing: promise ("+promised_num+") is too old to care about ("+PROPOSAL_MAX+")",h2o);

    // Extract any prior accepted proposals
    long prior_proposal = old_proposal(buf);
    // Extract the prior accepted Value also
    HashSet<H2ONode> prior_value = read_members(buf);

    // Does this promise match the membership I like?  If so, we'll accept the
    // promise.  If not, we'll blow it off.. and hope for promises for what I
    // like.  In normal Paxos, we have to report back any Value PREVIOUSLY
    // promised, even for new proposals.  But in this case, the wrong promise
    // basically triggers a New Round of Paxos voting... so this becomes a
    // stale promise for the old round
    if( prior_proposal > 0 && !PROPOSED_MEMBERS.equals(prior_value) )
      return print_debug("do  : nothing, because this is a promise for the wrong thing",prior_value);

    ACCEPTED.add(h2o);

    // See if we hit the Quorum needed
    final int quorum = (PROPOSED_MEMBERS.size()>>1)+1;
    if( ACCEPTED.size() < quorum )
      return print_debug("do  : No Quorum yet "+ACCEPTED+"/"+quorum,PROPOSED_MEMBERS);
    if( ACCEPTED.size() > quorum )
      return print_debug("do  : Nothing; Quorum exceeded and already sent AcceptRequest "+ACCEPTED+"/"+quorum,PROPOSED_MEMBERS);

    // We hit Quorum.  We can now ask the Acceptors to accept this proposal.
    // Build & multicast an Accept! packet.  It is our own proposal with the 8
    // bytes of Accept number set, and includes the members as the agreed Value
    set_old_proposal(BUF,PROPOSAL_MAX);
    write_members_set_id();
    UDPPaxosAccept.build_and_multicast(BUF);
    return print_debug("send: AcceptRequest because hit Quorum ",PROPOSED_MEMBERS,BUF);
  }

  // Recieved an Accept Request from some Proposer after he hit a Quorum.  The
  // packet has 8 bytes of proposal number, and a membership list.  Buf is
  // freed when done.
  static synchronized int do_accept( byte[] buf, H2ONode h2o ) {
    print_debug("recv: AcceptRequest ",null,buf);
    long proposal_num = old_proposal(buf);
    if( PROPOSAL_MAX==0 )
      return print_debug("do  : nothing, received Accept! "+proposal_num+" but no proposal in progress",PROPOSED_MEMBERS,buf);

    if( proposal_num < PROPOSAL_MAX )
      // We got an out-of-date AcceptRequest which we can ignore.  The Leader
      // should have already started a new proposal round
      return print_debug("do  : ignoring out of date AcceptRequest ",null,buf);

    PROPOSAL_MAX = proposal_num;

    // At this point, all Acceptors should tell all Learners via Accepted
    // messages about the new agreement.  However, the Leader is also an
    // Acceptor so we'll let him do one broadcast to all Learners.
    if( LEADER != H2O.SELF )
      return print_debug("do  : Nothing; Accept but I am not Leader, no need to send Accepted",PROPOSED_MEMBERS);

    UDPPaxosAccepted.build_and_multicast(buf);
    return print_debug("send: Accepted from leader only",PROPOSED_MEMBERS,buf);
  }

  // Recieved an Accepted packet from the Leader after he hit Quorum.
  // Setup a new Cloud.  Buf is freed when done.
  static synchronized int do_accepted( byte[] buf, H2ONode h2o ) {
    // Record most recent ping time from sender
    long proposal_num = promise(buf);
    HashSet<H2ONode> members = read_members(buf);
    print_debug("recv: Accepted ",members,buf);
    if( !members.contains(H2O.SELF) ) { // Not in this set?
      // This accepted set excludes me, so we need to start another round of
      // voting.  Pick up the largest proposal to-date, and start voting again.
      if( proposal_num > PROPOSAL_MAX ) PROPOSAL_MAX = proposal_num;
      return print_debug("do  : Leader missed me; I am still not in the new Cloud, so refuse the Accept and let my Heartbeat publish me again",members,buf);
      //do_change_announcement(H2O.CLOUD);
      //return 0;
    }

    if( proposal_num == PROPOSAL_MAX && uuid(buf).equals(H2O.CLOUD._id) )
      return print_debug("do  : Nothing: Accepted with same cloud membership list",members,buf);

    // We just got a proposal to change the cloud
    if( _commonKnowledge ) {    // We thought we knew what was going on?
      if( _cloud_locked )       // Oops - cloud locked
        cloud_kill(); // Cloud-wide kill because things changed after key inserted
      _commonKnowledge = false; // No longer sure about things
      System.out.println("[h2o] Paxos Cloud voting in progress");
    }

    H2O.CLOUD.set_next_Cloud(uuid(buf),members);
    PROPOSAL_MAX=0; // Reset voting; next proposal will be for a whole new cloud
    set_promise(BUF,0);
    set_old_proposal(BUF,0);
    return print_debug("do  : Accepted so set new cloud membership list",members,buf);
  }

  // Before we start doing distributed writes... block until the cloud
  // stablizes.  After we start doing distrubuted writes, it is an error to
  // change cloud shape - the distributed writes will be in the wrong place.
  static void lock_cloud() {
    if( _cloud_locked ) return; // Fast-path cutout
    synchronized(Paxos.class) {
      while( !_commonKnowledge ) 
        try { Paxos.class.wait(); } catch( InterruptedException ie ) { }
    }
    _cloud_locked = true;
  }

  // Cloud changed shape after locking (after keys distributed)... all key
  // lookups would be foo-bar'd, so just kill everything instead.
  static void cloud_kill() {
    UDPRebooted.global_kill(3);
    System.err.println("[h2o] Cloud changing after Keys distributed - fatal error.");
    System.err.println("[h2o] Received kill "+3+" from "+H2O.SELF);
    System.exit(-1);
  }

  // Extract a UUID from the proposed value
  static UUID uuid( byte[] buf ) {
    return new UUID( UDP.get8(buf,id_lo_off),
                     UDP.get8(buf,id_hi_off));
  }

  static long promise( byte[] buf ) {  return UDP.get8(buf,promise_off);  }
  static void set_promise( byte[] buf,long proposal_num ) {
    UDP.set8(buf,promise_off,proposal_num);
  }

  static long old_proposal( byte[] buf ) {  return UDP.get8(buf,old_proposal_off);  }
  static void set_old_proposal( byte[] buf, long proposal_num ) {
    UDP.set8(buf,old_proposal_off,proposal_num);
  }


  // Read wire-line protocol membership list from the buf
  static HashSet<H2ONode> read_members( byte[] buf ) {
    int len = UDP.get2(buf,member_cnt_off);
    int off = member_off;
    HashSet<H2ONode> members = new HashSet<H2ONode>();
    for( int i=0; i<len; i++ ) {
      H2ONode h2o = H2ONode.read(buf,off);
      off += H2ONode.wire_len();
      members.add(h2o);
    }
    return members;
  }
  // Write wire-line protocol membership list from the buf
  static void write_members_set_id(  ) {
    int newsiz = PROPOSED_MEMBERS.size()*H2ONode.wire_len()+member_off;
    // Correct the size of the resulting output buf
    if( BUF.length != newsiz )
      BUF = Arrays.copyOf(BUF,newsiz);

    int off = member_cnt_off;
    off += UDP.set2(BUF,off,PROPOSED_MEMBERS.size());
    // Write out the members as bytes of IP address and port #s
    for( H2ONode h2o : PROPOSED_MEMBERS )
      off = h2o._key.write(BUF,off);
  }

  static int print_debug( String msg, HashSet<H2ONode> members, byte[] buf ) {
    //print(msg,members," promise:"+promise(buf)+" old:"+old_proposal(buf));
    return 0;                   // handy flow-coding return
  }
  static int print_debug( String msg, H2ONode h2o ) {
    //HashSet tmp = new HashSet<H2ONode>();
    //tmp.add(h2o);
    //print(msg, tmp, "");
    return 0;                   // handy flow-coding return
  }
  static int print_debug( String msg, HashSet<H2ONode> members ) {
    //print(msg,members,"");
    return 0;                   // handy flow-coding return
  }
  static int print_debug( String msg, HashSet<H2ONode> members, String msg2 ) {
    //print(msg,members,msg2);
    return 0;                   // handy flow-coding return
  }
  static int print( String msg, HashSet<H2ONode> members, String msg2 ) {
    //System.out.println(msg+members+msg2);
    return 0;                   // handy flow-coding return
  }
}
