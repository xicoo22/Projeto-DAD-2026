package didatrade.server;

import didatrade.DidaTradeMaster;
import didatrade.DidaTradeMasterServiceGrpc;

import io.grpc.stub.StreamObserver;

public class DidaTradeMasterServiceImpl extends DidaTradeMasterServiceGrpc.DidaTradeMasterServiceImplBase {
	DidaTradeServerState server_state;

	public DidaTradeMasterServiceImpl(DidaTradeServerState state) {
		this.server_state = state;
	}

	@Override
	public void newballot(DidaTradeMaster.NewBallotRequest request,
			StreamObserver<DidaTradeMaster.NewBallotReply> responseObserver) {
		System.out.println(request);

		int request_id = request.getReqid();
		int new_ballot = request.getNewballot();
		int completed_ballot = request.getCompletedballot();
		;

		// for debug purposes
		System.out.println("Current ballot = " + this.server_state.getCurrentBallot() + " new ballot = " + new_ballot
				+ " completed ballot = " + completed_ballot);

		this.server_state.setCompletedBallot(completed_ballot);

		if (new_ballot > this.server_state.getCurrentBallot()) {
			this.server_state.setCurrentBallot(new_ballot);

			this.server_state.main_loop.wakeup();

			completed_ballot = this.server_state.waitForCompletedBallot(new_ballot);
		} else {
			completed_ballot = this.server_state.getCompletedBallot();
		}

		DidaTradeMaster.NewBallotReply.Builder response_builder = DidaTradeMaster.NewBallotReply.newBuilder();
		response_builder.setReqid(request_id);
		response_builder.setCompletedballot(completed_ballot);

		DidaTradeMaster.NewBallotReply response = response_builder.build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	@Override
	public void activate(DidaTradeMaster.ActivateRequest request,
			StreamObserver<DidaTradeMaster.ActivateReply> responseObserver) {
		System.out.println(request);

		int request_id = request.getReqid();
		int completed_ballot = request.getCompletedballot();
		;

		// for debug purposes
		System.out.println(
				"Current ballot = " + this.server_state.getCurrentBallot() + " activated ballot = " + completed_ballot);

		// do stuff

		DidaTradeMaster.ActivateReply.Builder response_builder = DidaTradeMaster.ActivateReply.newBuilder();
		response_builder.setReqid(request_id);
		response_builder.setAck(true);

		DidaTradeMaster.ActivateReply response = response_builder.build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	@Override
	public void setdebug(DidaTradeMaster.SetDebugRequest request,
			StreamObserver<DidaTradeMaster.SetDebugReply> responseObserver) {
		// for debug purposes
		System.out.println(request);

		boolean response_value = true;
		DebugMode mode = DebugMode.fromInt(request.getMode());

		switch (mode) {
			case CRASH:
				System.out.println("Exiting server due to debug mode CRASH");
				System.exit(1);
				break;
			case FREEZE:
				this.server_state.setDebugMode(mode);
				break;
			case UNFREEZE:
				this.server_state.setDebugMode(DebugMode.NONE);
				break;
			case SLOW_ON:
				this.server_state.setDebugMode(mode);
				break;
			case SLOW_OFF:
				this.server_state.setDebugMode(DebugMode.NONE);
				break;
			default:
				System.err.println("Ignoring invalid debug mode");
				break;
		}

		System.out.println("Debug mode is now = " + this.server_state.getDebugMode());

		int request_id = request.getReqid();

		DidaTradeMaster.SetDebugReply.Builder response_builder = DidaTradeMaster.SetDebugReply.newBuilder();
		response_builder.setReqid(request_id);
		response_builder.setAck(response_value);

		DidaTradeMaster.SetDebugReply response = response_builder.build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}
}
