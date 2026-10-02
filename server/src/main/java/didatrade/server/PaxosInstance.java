package didatrade.server;

public class PaxosInstance {
        public static final int ANY_VALUE = Integer.MIN_VALUE;

        int instance_nb;
        int command_id;
        int write_ballot;
        int accept_ballot;
        int n_accepts;
        boolean decided;

        public PaxosInstance() {
                this.instance_nb = 0;
                this.command_id = 0;
                this.write_ballot = -1;
                this.accept_ballot = -1;
                this.n_accepts = 0;
                this.decided = false;
        }

        public PaxosInstance(int id) {
                this.instance_nb = id;
                this.command_id = 0;
                this.write_ballot = -1;
                this.accept_ballot = -1;
                this.n_accepts = 0;
                this.decided = false;
        }

}
