package didatrade.server;

import java.util.Hashtable;

public class PaxosLog {
    private Hashtable<Integer, PaxosInstance> log;
    private int max_seen;

    public PaxosLog() {
        this.log = new Hashtable<Integer, PaxosInstance>();
        this.max_seen = -1;
    }

    public synchronized int length() {
        return this.max_seen + 1;
    }

    public synchronized PaxosInstance getEntry(int position) {
        return this.log.get(position);
    }

    public synchronized PaxosInstance testAndSetEntry(int position) {
        PaxosInstance entry = this.log.get(position);
        if (entry == null) {
            entry = new PaxosInstance(position);
            this.log.put(position, entry);
            if (position > this.max_seen) {
                this.max_seen = position;
            }
        }
        return entry;
    }

}
