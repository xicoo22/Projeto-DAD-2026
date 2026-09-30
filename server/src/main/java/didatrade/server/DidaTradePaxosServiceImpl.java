package didatrade.server;

import java.util.*;

import didatrade.DidaTradeMain;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;

import didatrade.util.GenericResponseCollector;
import didatrade.util.CollectorStreamObserver;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.Context;

public class DidaTradePaxosServiceImpl extends DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase {

    DidaTradeServerState server_state;

    public DidaTradePaxosServiceImpl(DidaTradeServerState state) {
        this.server_state = state;
    }

    @Override
    public void phaseone(DidaTradePaxos.PhaseOneRequest request,
            StreamObserver<DidaTradePaxos.PhaseOneReply> responseObserver) {
        this.server_state.checkDebugState();

        int start_instance = request.getInstance();
        int ballot = request.getRequestballot();
        boolean accepted = false;

        if (ballot >= this.server_state.getCurrentBallot()) {
            accepted = true;
            this.server_state.setCurrentBallot(ballot);
        }

        int maxballot = this.server_state.getCurrentBallot();

        DidaTradePaxos.PhaseOneReply.Builder response_builder = DidaTradePaxos.PhaseOneReply.newBuilder();
        response_builder.setInstance(start_instance);
        response_builder.setServerid(this.server_state.my_id);
        response_builder.setRequestballot(ballot);
        response_builder.setAccepted(accepted);
        response_builder.setMaxballot(maxballot);

        if (accepted) {
            int len = this.server_state.paxos_log.length();
            for (int i = start_instance; i < len; i++) {
                PaxosInstance e = this.server_state.paxos_log.getEntry(i);
                if (e != null && e.write_ballot > -1) {
                    response_builder.addAdopted(
                            DidaTradePaxos.AcceptedEntry.newBuilder()
                                    .setInstance(i)
                                    .setValue(e.command_id)
                                    .setValballot(e.write_ballot)
                                    .build());
                }
            }
        }
        System.out.println("[ACCEPTOR] p1 recv start=" + start_instance + " ballot=" + ballot
                + " -> accept=" + accepted + " maxballot=" + maxballot
                + " adopted=" + response_builder.getAdoptedCount());

        responseObserver.onNext(response_builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void phasetwo(DidaTradePaxos.PhaseTwoRequest request,
            StreamObserver<DidaTradePaxos.PhaseTwoReply> responseObserver) {
        this.server_state.checkDebugState();

        int instance = request.getInstance();
        int ballot = request.getRequestballot();
        int value = request.getValue();
        PaxosInstance entry = this.server_state.paxos_log.testAndSetEntry(instance);
        boolean accepted = false;
        int maxballot = ballot;

        if (ballot >= this.server_state.getCurrentBallot()) {
            accepted = true;
            entry.command_id = value;
            entry.write_ballot = ballot;
            this.server_state.setCurrentBallot(ballot);
        } else {
            maxballot = this.server_state.getCurrentBallot();
        }

        System.out.println("[ACCEPTOR] p2 recv instance=" + instance + " ballot=" + ballot
                + " value=" + value + " -> accept=" + accepted + " maxballot=" + maxballot);

        DidaTradePaxos.PhaseTwoReply.Builder response_builder = DidaTradePaxos.PhaseTwoReply.newBuilder();
        response_builder.setAccepted(accepted);
        response_builder.setInstance(instance);
        response_builder.setServerid(this.server_state.my_id);
        response_builder.setRequestballot(ballot);
        response_builder.setMaxballot(maxballot);

        DidaTradePaxos.PhaseTwoReply response = response_builder.build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();

        // Notify learners
        if (accepted == true) {

            Context ctx = Context.current().fork();
            ctx.run(() -> {
                List<Integer> learners = this.server_state.scheduler.learners(ballot);
                int n_targets = learners.size();

                DidaTradePaxos.LearnRequest.Builder learn_request_builder = DidaTradePaxos.LearnRequest.newBuilder();
                learn_request_builder.setInstance(instance);
                learn_request_builder.setValue(value);
                learn_request_builder.setBallot(ballot);

                DidaTradePaxos.LearnRequest learn_request = learn_request_builder.build();

                ArrayList<DidaTradePaxos.LearnReply> learn_responses = new ArrayList<DidaTradePaxos.LearnReply>();
                GenericResponseCollector<DidaTradePaxos.LearnReply> learn_collector = new GenericResponseCollector<DidaTradePaxos.LearnReply>(
                        learn_responses, n_targets);
                for (int i = 0; i < n_targets; i++) {
                    CollectorStreamObserver<DidaTradePaxos.LearnReply> learn_observer = new CollectorStreamObserver<DidaTradePaxos.LearnReply>(
                            learn_collector);
                    this.server_state.async_stubs[learners.get(i)].learn(learn_request, learn_observer);
                }
            });
        }

    }

    @Override
    public void learn(DidaTradePaxos.LearnRequest request, StreamObserver<DidaTradePaxos.LearnReply> responseObserver) {
        this.server_state.checkDebugState();

        int instance = request.getInstance();
        int ballot = request.getBallot();
        int value = request.getValue();

        synchronized (this) {
            PaxosInstance entry = this.server_state.paxos_log.testAndSetEntry(instance);

            this.server_state.setCurrentBallot(ballot);

            if (ballot == entry.accept_ballot) {
                entry.n_accepts++;
                if (entry.n_accepts >= this.server_state.scheduler.quorum(ballot) && !entry.decided) {
                    System.out.println("[LEARNER] instance=" + instance + " decided value=" + value
                            + " ballot=" + ballot);
                    this.server_state.updateCompletedBallot(ballot);
                    entry.decided = true;
                    this.server_state.executor.wakeup();
                }
            } else if (ballot > entry.accept_ballot) {
                System.out.println("[LEARNER] instance=" + instance + " reset (higher ballot=" + ballot + ")");
                entry.command_id = value;
                entry.accept_ballot = ballot;
                entry.n_accepts = 1;
            }
        }

        DidaTradePaxos.LearnReply.Builder response_builder = DidaTradePaxos.LearnReply.newBuilder();
        response_builder.setInstance(instance);
        response_builder.setBallot(ballot);

        DidaTradePaxos.LearnReply response = response_builder.build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}
