package didatrade.server;

public enum DebugMode {
  NONE, // Initial state, no debug mode
  CRASH, // The server crashes (i.e., exits)
  FREEZE, // The server “freezes”, i.e., blocks all requests from clients and from other
          // servers (except from the console) until it is “un-freezes”
  UNFREEZE, // The server “un-freezes”, i.e., processes all pending blocked requests and
            // removes the blocking condition. Should go back to NONE state after this.
  SLOW_ON, // The server applies a random delay
  SLOW_OFF; // The server stops applying a random delay and goes back to NONE state.

  public static DebugMode fromInt(int mode) {
    DebugMode[] values = DebugMode.values();
    if (mode <= 0 || mode >= values.length) {
      System.err.println("Invalid debug mode: " + mode);
      return NONE;
    }
    return values[mode];
  }

}
