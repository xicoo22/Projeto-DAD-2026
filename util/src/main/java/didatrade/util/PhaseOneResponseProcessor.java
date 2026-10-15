package didatrade.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import didatrade.DidaTradePaxos;

public class PhaseOneResponseProcessor extends GenericResponseProcessor<DidaTradePaxos.PhaseOneReply> {

    private boolean accepted;
    private int maxballot;
    private int acceptedCount;
    private int quorum;
    private int nAcceptors;
    private int nResponses;
    private Map<Integer, int[]> adopted;
    private List<List<Integer>> groups;
    private int[] groupQuorum, groupAccepted, groupResponses;

    public PhaseOneResponseProcessor(int quorum, int nAcceptors) {
        // System.out.println("Phase 1 processor constructor with low_ballot =" + l + "
        // high_ballot = " + h);
        this.accepted = false;
        this.maxballot = -1;
        this.acceptedCount = 0;
        this.quorum = quorum;
        this.nAcceptors = nAcceptors;
        this.nResponses = 0;
        this.adopted = new HashMap<>();

    }

    // This constructor is used when we have groups of acceptors and we want to
    // track the responses per group
    public PhaseOneResponseProcessor(List<List<Integer>> groups, List<Integer> quorums) {
        this(0, 0);
        this.groups = groups;
        int g = groups.size();
        this.groupQuorum = new int[g];
        this.groupAccepted = new int[g];
        this.groupResponses = new int[g];
        for (int i = 0; i < g; i++) {
            this.groupQuorum[i] = quorums.get(i);
        }
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
                    this.adopted.put(inst, new int[] { ae.getValue(), ae.getValballot() });
                }
            }

        } else {
            // Update the maxballot if the last response has a higher maxballot to know that
            // we need to increase our ballot for the next round
            if (last_response.getMaxballot() > this.maxballot) {
                this.maxballot = last_response.getMaxballot();
            }
            // Possible otimization: if we know an acceptor already saw an higher ballot,
            // there is no need to wait for the rest because even if we succed it will get
            // replaced by the other and we dont need to wait.
            this.accepted = false;
            return true;
        }

        // If we have groups of acceptors, we need to track the responses per group and
        // check if we have enough accepted responses to reach quorum for each group
        if (this.groups != null) {
            int serverId = last_response.getServerid();
            for (int g = 0; g < groups.size(); g++) {
                if (groups.get(g).contains(serverId)) {
                    groupResponses[g]++;
                    if (last_response.getAccepted())
                        groupAccepted[g]++;
                }
            }
            boolean success = true, fail = false;
            for (int g = 0; g < groups.size(); g++) {
                if (groupAccepted[g] < groupQuorum[g])
                    success = false;
                if (groupAccepted[g] + (groups.get(g).size() - groupResponses[g]) < groupQuorum[g])
                    fail = true;
            }
            this.accepted = success;
            return success || fail;
        }

        int remaining = this.nAcceptors - this.nResponses;

        // If we have enough accepted responses to reach quorum, we can stop waiting for
        // more responses
        boolean success = this.acceptedCount >= this.quorum;
        // If it is impossible to reach quorum even if all remaining responses are
        // accepted, we can also stop waiting for more responses
        boolean fail = (this.acceptedCount + remaining) < this.quorum;

        this.accepted = success;
        return success || fail;
    }
}
