package didatrade.util;

import java.util.ArrayList;
import java.util.Hashtable;

import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;

import didatrade.configs.ConfigurationScheduler;

public class PhaseOneResponseProcessor extends GenericResponseProcessor<DidaTradePaxos.PhaseOneReply> {
  private ConfigurationScheduler scheduler;
  private boolean accepted;
  private int value;
  private int valballot;
  private int maxballot;
  private int acceptedCount;
  private int low_ballot;
  private int high_ballot;
  private int quorum;
  private int nAcceptors;
  private int nResponses;

  public PhaseOneResponseProcessor(ConfigurationScheduler s, int l, int h, int quorum, int nAcceptors) {
    // System.out.println("Phase 1 processor constructor with low_ballot =" + l + "
    // high_ballot = " + h);
    this.accepted = false;
    this.value = -1;
    this.valballot = -1;
    this.maxballot = -1;
    this.low_ballot = l;
    this.high_ballot = h;
    this.scheduler = s;
    this.acceptedCount = 0;
    this.quorum = quorum;
    this.nAcceptors = nAcceptors;
    this.nResponses = 0;
  }

  public boolean getAccepted() {
    return this.accepted;
  }

  public int getValue() {
    return this.value;
  }

  public int getValballot() {
    return this.valballot;
  }

  public int getMaxballot() {
    return this.maxballot;
  }

  public synchronized boolean onNext(ArrayList<DidaTradePaxos.PhaseOneReply> all_responses,
      DidaTradePaxos.PhaseOneReply last_response) {

    System.out.println("PHASE1 RESPONSE: " + last_response);

    this.nResponses++;

    if (last_response.getAccepted()) {
      this.acceptedCount++;

      if (last_response.getValballot() > this.valballot) {
        this.valballot = last_response.getValballot();
        this.value = last_response.getValue();
      }
    } else {
      if (last_response.getMaxballot() > this.maxballot) {
        this.maxballot = last_response.getMaxballot();
      }
      // Possible otimization: if we know an acceptor already saw an higher ballot,
      // there is no need to wait for the rest because even if we succed it will get
      // replaced by the other and we dont need to wait.
      this.accepted = false;
      return true;
    }
    int remaining = this.nAcceptors - this.nResponses;
    boolean success = this.acceptedCount >= this.quorum;
    boolean fail = (this.acceptedCount + remaining) < this.quorum;

    this.accepted = success;
    return success || fail;
  }
}
