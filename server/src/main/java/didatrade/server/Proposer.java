package didatrade.server;

import java.util.ArrayList;
import java.util.List;

import didatrade.DidaTradePaxos;
import didatrade.util.CollectorStreamObserver;
import didatrade.util.GenericResponseCollector;
import didatrade.util.PhaseOneResponseProcessor;
import didatrade.util.PhaseTwoResponseProcessor;

public class Proposer implements Runnable {

    DidaTradeServerState state;
    private boolean has_work;

    public Proposer(DidaTradeServerState s) {
        this.state = s;
        this.has_work = false;
    }

    public synchronized void wakeup() {
        this.has_work = true;
        notify();
    }

    public void run() {
        while (true) {
            while (true) {
                synchronized (this) {
                    RequestRecord req = state.req_history.getFirstNotProposed();
                    int ballot = state.getCurrentBallot();
                    if (ballot >= 0 && req != null
                            && state.scheduler.leader(ballot) == state.my_id) {
                        break;
                    }

                    has_work = false;
                    while (!has_work) {
                        try {
                            wait();
                        } catch (InterruptedException e) {
                        }
                    }
                }
            }

            int n = state.getNextInstanceToPropose();
            proposeInstance(n);
            state.incNextInstanceToPropose();
        }
    }

    private void proposeInstance(int entry_number) {
        PaxosInstance next_entry = state.paxos_log.testAndSetEntry(entry_number);

        if (next_entry.decided) {
            return;
        }
        RequestRecord request_record = state.req_history.getFirstNotProposed();
        if (request_record == null) {
            state.decNextInstanceToPropose();
            return;
        }
        int ballot = state.getCurrentBallot();

        // Double check that we are still the leader, so we don't have a server
        // proposing with a ballot that is not supposed to be his.
        if (state.scheduler.leader(ballot) != state.my_id) {
            state.decNextInstanceToPropose();
            return;
        }

        List<Integer> acceptors = state.scheduler.acceptors(ballot);
        int quorum = state.scheduler.quorum(ballot);
        int n_acceptors = acceptors.size();

        request_record.setProposed(true);
        int reqid = request_record.getId();
        int phase_two_value = reqid;

        // === Phase 1 ===
        if (!state.getPhase1Done()) {
            System.out.println("[PROPOSER] instance=" + entry_number + " reqid=" + reqid
                    + " ballot=" + ballot + " start");
            DidaTradePaxos.PhaseOneRequest p1_request = DidaTradePaxos.PhaseOneRequest.newBuilder()
                    .setInstance(entry_number)
                    .setRequestballot(ballot)
                    .build();

            PhaseOneResponseProcessor p1_processor = new PhaseOneResponseProcessor(quorum, n_acceptors);
            ArrayList<DidaTradePaxos.PhaseOneReply> p1_responses = new ArrayList<>();
            GenericResponseCollector<DidaTradePaxos.PhaseOneReply> p1_collector = new GenericResponseCollector<>(
                    p1_responses, n_acceptors,
                    p1_processor);

            for (int i = 0; i < n_acceptors; i++) {
                state.async_stubs[acceptors.get(i)].phaseone(p1_request,
                        new CollectorStreamObserver<>(p1_collector));
            }
            p1_collector.waitUntilDone();

            if (!p1_processor.getAccepted()) {
                // Phase 1 failed, we need to bump our ballot and retry later
                int maxballot = p1_processor.getMaxballot();
                System.out.println("[PROPOSER] instance=" + entry_number + " phase1 ABORTED"
                        + " (higher ballot=" + maxballot + ")");
                if (maxballot > state.getCurrentBallot()) {
                    state.setCurrentBallot(maxballot);
                }
                request_record.setProposed(false);
                state.decNextInstanceToPropose();
                return;

            } else {
                state.setPhase1Done(true);
                state.setAdoptedMap(p1_processor.getAdoptedByInstance());
                System.out.println("[PROPOSER] instance=" + entry_number + " phase1 ok"
                        + " (adopted map size=" + p1_processor.getAdoptedByInstance().size() + ")");
            }
        }
        // === Phase 2 ===

        // Consult adopted map from prefix Phase 1
        Integer adoptedVal = state.getAdoptedFor(entry_number);
        if (adoptedVal != null) {
            phase_two_value = adoptedVal;
            request_record.setProposed(false);
            state.clearAdopted(entry_number);
            System.out.println("[PROPOSER] instance=" + entry_number
                    + " using adopted value=" + adoptedVal);
        }

        final int fpv = phase_two_value;

        DidaTradePaxos.PhaseTwoRequest p2_request = DidaTradePaxos.PhaseTwoRequest.newBuilder()
                .setInstance(entry_number)
                .setRequestballot(ballot)
                .setValue(fpv)
                .build();

        final PhaseTwoResponseProcessor p2_tracker = new PhaseTwoResponseProcessor(quorum, n_acceptors);
        for (int i = 0; i < n_acceptors; i++) {
            state.async_stubs[acceptors.get(i)].phasetwo(p2_request,
                    new io.grpc.stub.StreamObserver<DidaTradePaxos.PhaseTwoReply>() {
                        public void onNext(DidaTradePaxos.PhaseTwoReply r) {
                            p2_tracker.onResponse(r.getAccepted());
                            if (!r.getAccepted() && r.getMaxballot() > state.getCurrentBallot()) {
                                System.out.println("[PROPOSER] instance=" + entry_number
                                        + " phase2 rejected, bumping ballot to " + r.getMaxballot());
                                state.setCurrentBallot(r.getMaxballot());
                            }
                            if (p2_tracker.isImpossible()) {
                                System.out.println("[PROPOSER] instance=" + entry_number
                                        + " reqid=" + fpv + " phase2 failed, releasing for retry");
                                request_record.setProposed(false);
                            }

                        }

                        public void onError(Throwable t) {
                        }

                        public void onCompleted() {
                        }
                    });
        }

        System.out.println("[PROPOSER] instance=" + entry_number + " reqid=" + fpv + " phase2 SENT (async)");

    }

}
