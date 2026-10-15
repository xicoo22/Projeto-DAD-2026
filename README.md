# DAD2627

DAD 26-27 Project

This repository holds the base code required to implement the project. Students are free to improve on the following
code.
Students should create a copy of this repository by clicking on `Use This Template` on the project GitHub page.

# ⚠️ **WARNING:** ⚠️

Students must **NOT** develop their solution on a Fork of the repository.
Forks are public and your code **WILL** be visible to other students.

It falls onto the students to **NOT DEVELOP ON A FORK THE PROJECT**

# Requirements

The project requires the following packages (you are free to use other versions of java):

- Java 22
- Maven 3.8.4
- Protoc 3.12

## Environment

The project includes a template `setup_env.sh` which students may use as basis to download all necessary packages for
the project execution.
The current script was designed for Linux/Ubuntu distributions on an Intel x86 environments.
Students may extend this script for their target distribution/CPU architecture.

If so, it will keep all required packages in `INSTALL_DIR`, which can be activated by running
`source INSTALL_DIR/env.sh`.

# Compiling

To compile the project, students must run the command
`mvn clean install` in the root directory

## ⚠️ ️**WARNING - Compilation Environment** ⚠️

The project requires different `pom.xml` for the *contract* module depending on the CPU architecture/OS distribution.
The project currently has two pre-pepared poms:

- One for **ARM/M4 Mac-OS** based systems, named `arm-pom.xml`;
- One for **Intel/Linux** based systems, named `intel-pom.xml`;

Before the first compilation, copy your required contract pom and rename it to `pom.xml`.

# Deployment

Current implementation assumes that all modules run on the same physical machine and requires 5 active servers.

The project is composed of three main components:

- Servers
- App (Clients)
- Console

## Servers

The servers run the base implementation. They are executed running the following command in the *server* directory:

`mvn exec:java -Dexec.args="{port} {id} {scheduler}"`

Where you must fill in the following arguments:

- **{port}**: Base port of all servers. **All servers should use the same port**. The Server binded port will be  *
  *{port} + {id}**.
- **{id}**: Sequential id of the server. Current implementation requires servers to be ID'ed starting from *0* to *N-1*
  servers.
- **{scheduler}**: The scheduler used for reconfiguration (use 'A' to start)

## Client

A client that executes transactions. It is executed by running the following command in the *app* directory:

`mvn exec:java -Dexec.args="{id} {host} {port} {scheduler}"`

Where you must fill in the following arguments:

- **{id}**: Sequential id of the client. Current implementation requires clients to be ID'ed starting from *1*.
- **{host}**: The host for all servers. (just use "localhost")
- **{port}**: Base port of all servers.
- **{scheduler}**: The scheduler used (use 'A' to start)

The client module opens a terminal from where students may issue commands. The following commands are available:

- `help` - Shows the full command list;
- `exit` - Gracefully finishes the client.

## Console

The console client servers as a front-end to issue configuration settings to servers. It is executed by running the
following command in the *consoleclient* directory:

`mvn exec:java -Dexec.args="{host} {port} {scheduler}"`

here you must fill in the following arguments:

- **{host}**: The host for all servers. (just use "localhost")
- **{port}**: Base port of all servers.
- **{scheduler}**: The scheduler used (use 'A' to start)

The console client opens a terminal from where students may issue configuration changes to servers. The following
commands are available:

- `help` - Shows the full command list;
- `ballot ballot_number server` - Instructs a server to start ballot with number 'ballot_number';
- `debug mode replica_id` - Activates debug on a given replica;
- `exit` - Gracefully finishes the console.

## Quick launch with `dida.sh` (tmux)

Instead of opening one terminal per process, the script `dida.sh` (in the root directory) builds the project and
starts all servers, the clients and the console inside a single tmux session. It runs exactly the commands described
above, one per pane.

Before using it:

- install `tmux`;
- make sure `contract/pom.xml` exists (see the compilation warning above);
- if you use `setup_env.sh`, run `source INSTALL_DIR/env.sh` in the terminal where you start the script;
- make the script executable once: `chmod +x dida.sh`.

### Usage

```
./dida.sh [options] <A|B|C>
./dida.sh stop
```

| Option              | Meaning                                                         |
|---------------------|-----------------------------------------------------------------|
| `<A\|B\|C>`         | schedule to run (3 servers for A and B, 7 servers for C)        |
| `-c N`, `--clients` | number of clients to start (default 1, ids 1 to N)              |
| `-k 0\|1`, `--console` | start the console (default 1)                                |
| `-p PORT`, `--port` | base port of server 0 (default 58000)                           |
| `-n`, `--no-build`  | skip the Maven build (use it when nothing changed since the last run) |
| `-l`, `--log`       | also save the output of every pane in `logs/` (one file per process) |
| `-d`, `--detach`    | create the session but do not attach to it                      |
| `stop`              | kill the session and every process in it                        |

Examples:

```
./dida.sh A                 # 3 servers, 1 client, 1 console
./dida.sh C -c 2            # 7 servers, 2 clients, 1 console
./dida.sh B -k 0 -n -l      # no console, no build, logs saved in logs/
./dida.sh stop              # stop everything
```

### What you get

The script creates one tmux session called `dida` with two windows:

- **servers**: one pane per server (S0, S1, ...), tiled. This is only for watching the logs.
- **clients** (opens first): the console in the large left pane and the clients stacked on the right. This is where
  you type commands.

Servers 1 to N-1 are started first and server 0 (the leader of ballot 0) is started last, once all the others accept
connections. The console and the clients start after that.

Useful tmux keys (all start with `Ctrl-b`, then release and press the key):

| Keys         | Action                                                 |
|--------------|--------------------------------------------------------|
| `n` / `p`    | next / previous window (servers / clients)             |
| `z`          | zoom the current pane to full screen (press again to restore) |
| `d`          | detach and leave everything running (`tmux attach -t dida` to come back) |
| mouse click  | select a pane (the mouse is enabled)                   |

Panes stay open after a process exits, so you can still read its output. Running the script again replaces the
previous session. Add `logs/` to `.gitignore` if you use `-l`.

## Protobuffs and Utils

To support these modules, the project has additional directories:

- *contract*, holding the required `.proto` files;
- *util*, holding the general classes to collect RPC responses
- *core*, holding the general classes that manage meetings
- *configs*, holding the general classes that manage configurations
  
