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

    public Proposer(DidaTradeServerState s) {
        this.state = s;
    }

    public synchronized void wakeup() {
        notify();
    }

    public void run() {
        while (true) {
            while (true) {
                synchronized (this) {
                    int ballot = state.getCurrentBallot();
                    if (ballot >= 0 && state.scheduler.leader(ballot) == state.my_id && hasWorkToDo(ballot)) {
                        break;
                    }

                    try {
                        wait();
                    } catch (InterruptedException e) {
                    }

                }
            }

            int n = state.getAndIncNextInstanceToPropose();
            proposeInstance(n);
        }
    }

    private void proposeInstance(int entry_number) {
        PaxosInstance next_entry = state.paxos_log.testAndSetEntry(entry_number);

        if (next_entry.decided) {
            return;
        }
        int ballot = state.getCurrentBallot();

        // Double check that we are still the leader, so we don't have a server
        // proposing with a ballot that is not supposed to be his.
        if (state.scheduler.leader(ballot) != state.my_id) {
            state.decInstanceIfUnused(entry_number);
            return;
        }

        List<Integer> acceptors = state.scheduler.acceptors(ballot);
        int quorum = state.scheduler.quorum(ballot);
        int n_acceptors = acceptors.size();

        // Phase 1: Runs even without a pending client request
        if (!state.getPhase1Done()) {
            System.out.println("[PROPOSER] instance=" + entry_number + " ballot=" + ballot + "phase1 start");
            DidaTradePaxos.PhaseOneRequest p1_request = DidaTradePaxos.PhaseOneRequest.newBuilder()
                    .setInstance(entry_number)
                    .setRequestballot(ballot)
                    .build();

            PhaseOneResponseProcessor p1_processor = new PhaseOneResponseProcessor(quorum, n_acceptors);
            ArrayList<DidaTradePaxos.PhaseOneReply> p1_responses = new ArrayList<>();
            GenericResponseCollector<DidaTradePaxos.PhaseOneReply> p1_collector = new GenericResponseCollector<>(
                    p1_responses, n_acceptors, p1_processor);

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
                state.decInstanceIfUnused(entry_number);
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                }
                return;

            } else {
                state.setPhase1Done(true);
                state.setAdoptedMap(p1_processor.getAdoptedByInstance());
                // No record here. this is ANY or an adopted value, not a real client request
                Integer adoptedVal = state.getAdoptedFor(entry_number);
                if (adoptedVal != null) {
                    state.clearAdopted(entry_number);
                    System.out.println("[PROPOSER] instance=" + entry_number + " using adopted value=" + adoptedVal);
                    sendPhaseTwo(entry_number, ballot, adoptedVal, acceptors, quorum, n_acceptors, null);
                    return;
                }

                if (state.scheduler.fastpaxos(ballot)) {
                    System.out.println("[PROPOSER] instance=" + entry_number + " fast paxos ballot — proposing ANY");
                    sendPhaseTwo(entry_number, ballot, PaxosInstance.ANY_VALUE, acceptors, quorum, n_acceptors, null);
                    return;
                }
            }
        }
        // === Phase 2 ===

        // Consult adopted map from prefix Phase 1
        Integer adoptedVal = state.getAdoptedFor(entry_number);
        if (adoptedVal != null) {
            state.clearAdopted(entry_number);
            // Same as above: no real request behind an adopted value.
            this.sendPhaseTwo(entry_number, ballot, adoptedVal, acceptors, quorum, n_acceptors, null);

            System.out.println("[PROPOSER] instance=" + entry_number
                    + " using adopted value=" + adoptedVal);
            return;
        }

        // Finally, if we have a pending client request, propose it. Otherwise, release
        // the reserved instance,we have nothing to do.
        RequestRecord request_record = state.req_history.getFirstNotProposed();
        if (request_record == null) {
            state.decInstanceIfUnused(entry_number);
            return;
        }
        request_record.setProposed(true);
        int reqid = request_record.getId();
        sendPhaseTwo(entry_number, ballot, reqid, acceptors, quorum, n_acceptors, request_record);
    }

    private void sendPhaseTwo(int entry_number, int ballot, int value, List<Integer> acceptors,
            int quorum, int n_acceptors, RequestRecord request_record) {

        DidaTradePaxos.PhaseTwoRequest p2_request = DidaTradePaxos.PhaseTwoRequest.newBuilder()
                .setInstance(entry_number)
                .setRequestballot(ballot)
                .setValue(value)
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
                            // Without this check, a rejected Phase 2 left the request stuck with
                            // proposed=true forever: resetProposedFlags no longer fires here (it
                            // only runs from the console's newballot, to avoid wiping unrelated
                            // in-flight requests), so nothing else would ever free it again.
                            if (p2_tracker.isImpossible() && request_record != null) {
                                System.out.println("[PROPOSER] instance=" + entry_number
                                        + " reqid=" + value + " phase2 failed, releasing for retry");
                                request_record.setProposed(false);
                            }

                        }

                        public void onError(Throwable t) {
                        }

                        public void onCompleted() {
                        }
                    });
        }

        System.out.println("[PROPOSER] instance=" + entry_number + " reqid=" + value + " phase2 SENT (async)");

    }

    // If Phase 1 is not done, we have to do it anyway, even if there are no pending
    // requests
    private boolean hasWorkToDo(int ballot) {
        return !state.getPhase1Done() || state.req_history.getFirstNotProposed() != null;
    }

}
