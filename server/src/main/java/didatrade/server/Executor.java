package didatrade.server;

public class Executor implements Runnable {
    DidaTradeServerState state;

    public Executor(DidaTradeServerState s) {
        this.state = s;
    }

    public synchronized void wakeup() {
        notify();
    }

    public void run() {
        while (true) {
            int n = state.getNextInstanceToExecute();
            executeEntry(n);
            state.incNextInstanceToExecute();
        }
    }

    private void executeEntry(int entry_number) {
        PaxosInstance entry = state.paxos_log.testAndSetEntry(entry_number);

        // Espera até a linha ficar decidida
        synchronized (this) {
            while (!entry.decided) {
                try {
                    wait();
                } catch (InterruptedException e) {
                }
            }
        }
        // If the entry is a fast-paxos "ANY" marker, skip it. It doesn't correspond to
        // a real client request.
        if (entry.command_id == PaxosInstance.ANY_VALUE) {
            System.out.println("[EXECUTOR] instance=" + entry_number + " skipping ANY (fast-paxos open marker)");
            return;
        }

        // Vai buscar o pedido original correspondente a este command_id
        RequestRecord request_record = state.req_history.getIfPending(entry.command_id);
        boolean warned = false;
        while (request_record == null) {
            if (!warned) {
                System.out.println("[EXECUTOR] instance=" + entry_number
                        + " waiting for record reqid=" + entry.command_id);
                warned = true;
            }
            synchronized (this) {
                // Wait a bit for the record to be registered in this server. Nothing notifies
                // us when it is, so poll instead of blocking forever.
                try {
                    wait(100);
                } catch (InterruptedException e) {
                }
            }
            request_record = state.req_history.getIfPending(entry.command_id);
        }

        // Executa o pedido no mercadinho
        DidaTradeCommand command = request_record.getRequest();
        boolean result = false;
        int balance = 0;
        DidaTradeAction action = command.getAction();

        switch (action) {
            case POPULATE:
                result = state.trade_manager.populate(command.getQuantity());
                break;
            case ADDUSER:
                result = state.trade_manager.add_user(command.getUserId(),
                        command.getQuantity(), command.getStock());
                break;
            case SELL:
                result = state.trade_manager.sell(command.getUserId(), command.getQuantity());
                break;
            case BUY:
                result = state.trade_manager.acquire(command.getUserId(), command.getQuantity());
                break;
            case BALANCE:
                balance = state.trade_manager.balance(command.getUserId());
                command.setQuantity(balance);
                result = (balance != -1);
                break;
            case DUMP:
                state.trade_manager.dump();
                result = true;
                break;
            default:
                result = false;
                System.err.println("[EXECUTOR] unknown action");
                break;
        }

        System.out.println("[EXECUTOR] instance=" + entry_number + " apply " + action
                + " reqid=" + entry.command_id + " -> result=" + result);
        request_record.setResponse(result);
        state.req_history.moveToProcessed(request_record.getId());
    }
}
