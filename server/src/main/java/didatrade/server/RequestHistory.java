package didatrade.server;

import java.util.Enumeration;
import java.util.LinkedHashMap;

public class RequestHistory {

    private LinkedHashMap<Integer, RequestRecord> pending;
    private LinkedHashMap<Integer, RequestRecord> processed;

    public RequestHistory() {
        this.pending = new LinkedHashMap<Integer, RequestRecord>();
        this.processed = new LinkedHashMap<Integer, RequestRecord>();
    }

    public synchronized RequestRecord getIfPending(int requestid) {
        Integer id = new Integer(requestid);
        return this.pending.get(id);
    }

    public synchronized RequestRecord getFirstPending() {
        var it = this.pending.values().iterator();
        return it.hasNext() ? it.next() : null;
    }

    public synchronized RequestRecord getFirstNotProposed() {
        for (RequestRecord r : this.pending.values()) {
            if (!r.isProposed()) {
                return r;
            }
        }
        return null;
    }

    public synchronized RequestRecord getIfProcessed(int requestid) {
        Integer id = new Integer(requestid);
        return this.processed.get(id);
    }

    public synchronized RequestRecord getIfExists(int requestid) {
        RequestRecord record;
        Integer id = new Integer(requestid);

        record = this.pending.get(id);
        if (record == null) {
            record = this.processed.get(id);
        }
        return record;
    }

    public synchronized void addToPending(int requestid, RequestRecord record) {
        Integer id = new Integer(requestid);

        this.pending.put(id, record);
    }

    public synchronized RequestRecord moveToProcessed(int requestid) {
        Integer id = new Integer(requestid);
        RequestRecord record = this.pending.remove(id);
        this.processed.put(id, record);
        return record;
    }

    public synchronized void resetProposedFlags() {
        for (RequestRecord r : this.pending.values()) {
            r.setProposed(false);
        }
    }

}
