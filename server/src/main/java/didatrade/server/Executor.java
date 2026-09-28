package didatrade.server;

  public class Executor implements Runnable {
      DidaTradeServerState state;
      private boolean has_work;

      public Executor(DidaTradeServerState s) {
          this.state = s;
          this.has_work = false;
      }

      public synchronized void wakeup() {
          this.has_work = true;
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
                  has_work = false;
                  try { wait(); } catch (InterruptedException e) {}
              }
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
                  try { wait(100); } catch (InterruptedException e) {}
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
