package didatrade.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
            if (!sendPhaseOne(entry_number, ballot, acceptors, quorum, n_acceptors)) {
                return;
            }
        }

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

    // Returns true if the caller should go on with phase 2 in this same call
    private boolean sendPhaseOne(int entry_number, int ballot, List<Integer> acceptors, int quorum,
            int n_acceptors) {

        System.out.println("[PROPOSER] instance = " + entry_number + " ballot = " + ballot + "phase1 start");

        // A new leader can't just ask its own acceptors. Anything decided in an
        // older configuration lives there, so we ask every configuration since the
        // last completed ballot. Each one is a group and needs its own quorum.
        int lastCompletedBallot = Math.max(state.getCompletedBallot(), 0);
        List<List<Integer>> groups = new ArrayList<>();
        List<Integer> quorums = new ArrayList<>();
        List<Integer> p1_targets = new ArrayList<>();
        // For each ballot from lastCompletedBallot to current ballot, get the acceptors
        // and quorum, and add them to the groups and quorums lists. Also, add the
        // acceptors to the p1_targets list if they are not already there.
        for (int b = lastCompletedBallot; b <= ballot; b++) {
            List<Integer> acc = state.scheduler.acceptors(b);
            if (!groups.contains(acc)) {
                groups.add(acc);
                quorums.add(state.scheduler.quorum(b));
                for (int a : acc)
                    if (!p1_targets.contains(a))
                        p1_targets.add(a);
            }
        }
        boolean transfer = groups.size() > 1;
        int start = transfer ? 0 : entry_number;
        System.out.println("[PROPOSER] phase1 groups=" + groups + " quorums=" + quorums
                + " transfer=" + transfer + " from instance " + start);

        DidaTradePaxos.PhaseOneRequest p1_request = DidaTradePaxos.PhaseOneRequest.newBuilder()
                .setInstance(start)
                .setRequestballot(ballot)
                .build();

        // send phase 1 to everyone in the groups, the processor counts the
        // answers per group
        PhaseOneResponseProcessor p1_processor = new PhaseOneResponseProcessor(groups, quorums);
        ArrayList<DidaTradePaxos.PhaseOneReply> p1_responses = new ArrayList<>();
        GenericResponseCollector<DidaTradePaxos.PhaseOneReply> p1_collector = new GenericResponseCollector<>(
                p1_responses, p1_targets.size(), p1_processor);

        for (int t : p1_targets) {
            state.async_stubs[t].phaseone(p1_request, new CollectorStreamObserver<>(p1_collector));
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
            return false;

        } else {
            Map<Integer, Integer> adopted = p1_processor.getAdoptedByInstance();
            if (!adopted.isEmpty()) {

                int max = Collections.max(adopted.keySet());
                if (transfer) {
                    System.out.println("[PROPOSER] ballot=" + ballot + " state transfer of instances 0.." + max
                            + " to " + acceptors);
                    if (!transferState(ballot, max, adopted, acceptors, quorum)) {
                        System.out.println("[PROPOSER] ballot=" + ballot + " state transfer FAILED, will retry");
                        state.decInstanceIfUnused(entry_number);
                        try {
                            Thread.sleep(200);
                        } catch (InterruptedException e) {
                        }
                        return false;
                    }
                    System.out.println("[PROPOSER] ballot=" + ballot + " state transfer COMPLETE");

                    // the next instance can’t leave holes behind the copied log
                    state.setNextInstanceToPropose(max + 1);

                    // a request that is already in the copied log isn’t decided twice
                    for (int v : adopted.values()) {
                        RequestRecord r = state.req_history.getIfPending(v);
                        if (r != null) {
                            r.setProposed(true);
                        }
                    }

                    // the console’s reply doesn’t depend on another decision, and the next ballot’s
                    // Phase 1 starts from here
                    state.setPhase1Done(true);
                    state.setCompletedBallot(ballot);

                    // fast ballots open their first round at the right instance
                    if (state.scheduler.fastpaxos(ballot)) {
                        int next = state.getAndIncNextInstanceToPropose();
                        System.out.println("[PROPOSER] ballot=" + ballot
                                + " fast paxos ballot: proposing ANY at instance=" + next);
                        sendPhaseTwo(next, ballot, PaxosInstance.ANY_VALUE, acceptors, quorum, n_acceptors, null);
                    }
                    return false;
                } else {
                    fillHoles(ballot, entry_number, max, adopted, acceptors, quorum);
                    if (!adopted.containsKey(entry_number)) {
                        // this instance was itself a hole and now holds a NOOP, nothing else to propose
                        // in it
                        state.setPhase1Done(true);
                        state.setAdoptedMap(adopted);
                        return false;
                    }
                }
            }

            state.setPhase1Done(true);
            state.setAdoptedMap(adopted);
            // No record here. this is ANY or an adopted value, not a real client request
            Integer adoptedVal = state.getAdoptedFor(entry_number);
            if (adoptedVal != null) {
                state.clearAdopted(entry_number);
                System.out.println("[PROPOSER] instance=" + entry_number + " using adopted value=" + adoptedVal);
                sendPhaseTwo(entry_number, ballot, adoptedVal, acceptors, quorum, n_acceptors, null);
                return false;
            }

            if (state.scheduler.fastpaxos(ballot)) {
                System.out.println("[PROPOSER] instance=" + entry_number + " fast paxos ballot: proposing ANY");
                sendPhaseTwo(entry_number, ballot, PaxosInstance.ANY_VALUE, acceptors, quorum, n_acceptors, null);
                return false;
            }
        }

        return true;
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

    private void sendPhaseTwoTracked(int instance, int ballot, int value, List<Integer> acceptors, int quorum,
            AtomicInteger acks, CountDownLatch latch) {
        DidaTradePaxos.PhaseTwoRequest request = DidaTradePaxos.PhaseTwoRequest.newBuilder()
                .setInstance(instance).setRequestballot(ballot).setValue(value).build();
        PhaseTwoResponseProcessor tracker = new PhaseTwoResponseProcessor(quorum, acceptors.size());

        for (int a : acceptors) {
            state.async_stubs[a].phasetwo(request,
                    new io.grpc.stub.StreamObserver<DidaTradePaxos.PhaseTwoReply>() {
                        public void onNext(DidaTradePaxos.PhaseTwoReply r) {
                            tracker.onResponse(r.getAccepted());
                            if (r.getAccepted() && acks.incrementAndGet() >= quorum) {
                                latch.countDown(); // a quorum accepted
                            } else if (tracker.isImpossible()) {
                                latch.countDown(); // a quorum can no longer be reached
                            }
                        }

                        public void onError(Throwable t) {
                            tracker.onResponse(false);
                            if (tracker.isImpossible()) {
                                latch.countDown();
                            }
                        }

                        public void onCompleted() {
                        }
                    });
        }
    }

    private boolean transferState(int ballot, int max, Map<Integer, Integer> adopted, List<Integer> acceptors,
            int quorum) {
        List<AtomicInteger> acks = new ArrayList<>();
        List<CountDownLatch> latches = new ArrayList<>();

        for (int inst = 0; inst <= max; inst++) {
            Integer found = adopted.get(inst);
            int value = (found != null) ? found : PaxosInstance.NOOP_VALUE; // hole -> no-op
            AtomicInteger count = new AtomicInteger();
            CountDownLatch latch = new CountDownLatch(1);
            acks.add(count);
            latches.add(latch);
            sendPhaseTwoTracked(inst, ballot, value, acceptors, quorum, count, latch);
        }

        boolean ok = true;
        for (int i = 0; i < latches.size(); i++) {
            try {
                if (!latches.get(i).await(10, TimeUnit.SECONDS)) {
                    ok = false;
                }
            } catch (InterruptedException e) {
                ok = false;
            }
            if (acks.get(i).get() < quorum) {
                ok = false;
            }
        }
        return ok;
    }

    private void fillHoles(int ballot, int from, int max, Map<Integer, Integer> adopted,
            List<Integer> acceptors, int quorum) {
        for (int instance = from; instance <= max; instance++) {
            if (adopted.containsKey(instance)) {
                continue; // someone accepted a value here, the normal path re-proposes it
            }
            if (state.paxos_log.testAndSetEntry(instance).decided) {
                continue; // already decided on this server
            }
            sendPhaseTwo(instance, ballot, PaxosInstance.NOOP_VALUE, acceptors, quorum, acceptors.size(), null);
        }
    }

    // If Phase 1 is not done, we have to do it anyway, even if there are no pending
    // requests
    private boolean hasWorkToDo(int ballot) {
        return !state.getPhase1Done() || state.req_history.getFirstNotProposed() != null;
    }

}
