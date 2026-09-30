package didatrade.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import didatrade.DidaTradePaxos;
import didatrade.configs.ConfigurationScheduler;

public class PhaseOneResponseProcessor extends GenericResponseProcessor<DidaTradePaxos.PhaseOneReply> {

    private ConfigurationScheduler scheduler;
    private boolean accepted;
    private int maxballot;
    private int acceptedCount;
    private int low_ballot;
    private int high_ballot;
    private int quorum;
    private int nAcceptors;
    private int nResponses;
    private Map<Integer, int[]> adopted;

    public PhaseOneResponseProcessor(ConfigurationScheduler s, int l, int h, int quorum, int nAcceptors) {
        // System.out.println("Phase 1 processor constructor with low_ballot =" + l + "
        // high_ballot = " + h);
        this.accepted = false;
        this.maxballot = -1;
        this.low_ballot = l;
        this.high_ballot = h;
        this.scheduler = s;
        this.acceptedCount = 0;
        this.quorum = quorum;
        this.nAcceptors = nAcceptors;
        this.nResponses = 0;
        this.adopted = new HashMap<>();

    }

    public boolean getAccepted() {
        return this.accepted;
    }

    public int getMaxballot() {
        return this.maxballot;
    }

    public Map<Integer, Integer> getAdoptedByInstance() {
        Map<Integer, Integer> out = new HashMap<>();
        for (Map.Entry<Integer, int[]> e : this.adopted.entrySet()) {
            out.put(e.getKey(), e.getValue()[0]);
        }
        return out;
    }

    public synchronized boolean onNext(ArrayList<DidaTradePaxos.PhaseOneReply> all_responses,
            DidaTradePaxos.PhaseOneReply last_response) {

        this.nResponses++;

        if (last_response.getAccepted()) {
            this.acceptedCount++;
            for (DidaTradePaxos.AcceptedEntry ae : last_response.getAdoptedList()) {
                int inst = ae.getInstance();
                int[] cur = this.adopted.get(inst);
                if (cur == null || ae.getValballot() > cur[1]) {
                    this.adopted.put(inst, new int[]{ae.getValue(), ae.getValballot()});
                }
            }

        } else {
            if (last_response.getMaxballot() > this.maxballot) {
                this.maxballot = last_response.getMaxballot();
            }
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
