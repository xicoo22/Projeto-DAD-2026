package didatrade.util;

public class PhaseTwoResponseProcessor {
	private int accepted;
	private int responded;
	private final int quorum;
	private final int n_acceptors;

	public PhaseTwoResponseProcessor(int quorum, int n_acceptors) {
		this.accepted = 0;
		this.responded = 0;
		this.quorum = quorum;
		this.n_acceptors = n_acceptors;
	}

	public synchronized void onResponse(boolean was_accepted) {
		this.responded++;
		if (was_accepted) {
			this.accepted++;
		}
	}

	public synchronized boolean isImpossible() {
		int remaining = this.n_acceptors - this.responded;
		return (this.accepted + remaining) < this.quorum;
	}
}