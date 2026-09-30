# Step 2 — Multi-Paxos (implementação)

Este documento descreve tudo o que foi feito para implementar Multi-Paxos.
Escrito para os colegas de grupo perceberem cada alteração e o porquê.
Explicações em criança com analogia do **mercadinho**.

**Estado**: implementado e validado experimentalmente.

---

## 1. Enunciado (§3.2)

> *"Multi-Paxos is a variant of Paxos that allows a new instance to be started
> before the previous instance is terminated. More precisely, when using
> Multi-Paxos, a leader may propose some value v′ for instance n immediately
> after proposing a value v for instance n − 1, without waiting for v to be
> first accepted by a majority of acceptors."*

Duas propriedades:
1. **Pipelining** — o líder propõe `n+1` sem esperar que `n` decida.
2. **Skip Phase 1** — o líder faz Phase 1 uma vez por ballot; depois só Phase 2s.

---

## 2. Analogia do mercadinho (contexto)

Pensa numa loja com **3 empregados**. Um deles é o **professor** (líder) que
decide a ordem pela qual as encomendas são executadas. Os outros são
empregados/carteiros normais que obedecem à ordem que o professor combinar.

- **Phase 1 (Prepare)** = professor toca à porta dos outros e diz *"sou eu que
  mando nesta ronda"*. Recebe autorização.
- **Phase 2 (Accept)** = professor envia cada encomenda para os outros
  registarem *"ok, aceito a encomenda X no slot N"*.
- **Learn** = carteiro leva a confirmação de aceite a todos, para contar votos.
- **Decided** = quando 2 de 3 (quorum) confirmam a mesma encomenda no mesmo
  slot → a encomenda pode ser executada.
- **Executor** = empregado que aplica a encomenda no mercadinho (mexe em stocks
  e wallets).

Antes: **um professor**, uma linha só, faz Phase 1 + Phase 2 para cada
encomenda uma de cada vez.

Depois (Multi-Paxos): professor **combina uma vez** com os outros que é o
chefe (Phase 1 só uma vez), e **dispara múltiplas encomendas em paralelo**
sem esperar pelas confirmações antes de despachar a próxima.

---

## 3. Ficheiros alterados

| Ficheiro | Passo | Descrição |
|---|---|---|
| `server/RequestHistory.java` | 1 | Ordem determinística + flag `proposed` |
| `server/RequestRecord.java` | 1 | Novo campo `proposed` |
| `server/DidaTradeServerState.java` | 2 | Novos campos e cursores; reset em ballot change |
| `server/Proposer.java` | 3, 4, 5 | Nova classe: propõe (Phase 1 + Phase 2 async) |
| `server/Executor.java` | 3 | Nova classe: aplica por ordem |
| `server/DidaTradePaxosServiceImpl.java` | 3 | `learn` acorda o Executor |
| `server/DidaTradeMainServiceImpl.java` | 3 | Cliente-RPC acorda o Proposer |
| `server/DidaTradeMasterServiceImpl.java` | 3 | Consola-RPC acorda o Proposer |
| `server/MainLoop.java` | 7 | **APAGADO** (substituído por Proposer + Executor) |

---

## 4. Passo 1 — `RequestHistory` ordenado

### O que existia

`RequestHistory` guardava os pedidos pendentes numa `Hashtable`. O método
`getFirstPending()` iterava com `keys()` — ordem **não determinística**.

### Problema

Com pipelining, múltiplos pedidos ficam em `pending` ao mesmo tempo. Se a
ordem em que os apanhamos é aleatória, cada réplica pode propô-los por ordens
diferentes → estado divergente.

### Fix

1. `Hashtable` → `LinkedHashMap` (ordem de inserção).
2. Novo método `getFirstNotProposed()` que devolve o pedido **mais antigo**
   ainda não proposto.
3. Flag `proposed` no `RequestRecord`.

### Analogia

Antes: os recibos estavam num **saco fechado** — enfiar a mão e tirar um ao
calhas. Agora: fila **FIFO** com etiqueta *"já proposto"* que o professor
carimba quando pega no recibo. Se o carimbo cair (aborto), o recibo volta a
ficar disponível.

### Código chave

`RequestRecord.java`:
```java
private boolean proposed;
public synchronized boolean isProposed() { return this.proposed; }
public synchronized void setProposed(boolean p) { this.proposed = p; }
```

`RequestHistory.java`:
```java
private LinkedHashMap<Integer, RequestRecord> pending;

public synchronized RequestRecord getFirstNotProposed() {
    for (RequestRecord r : this.pending.values()) {
        if (!r.isProposed()) return r;
    }
    return null;
}

public synchronized void resetProposedFlags() {
    for (RequestRecord r : this.pending.values()) {
        r.setProposed(false);
    }
}
```

`resetProposedFlags` é chamado quando muda o líder (nova ballot) — todos os
recibos que o professor antigo tinha no bolso (mas não decidiu) voltam ao
balcão.

---

## 5. Passo 2 — Estado partilhado

### Novos campos em `DidaTradeServerState`

```java
private boolean phase1_done_for_ballot;      // Phase 1 já feita nesta ballot?
private int next_instance_to_propose;        // Cursor do Proposer
private int next_instance_to_execute;        // Cursor do Executor

Proposer proposer;                            // Thread do Proposer
Executor executor;                            // Thread do Executor
Thread proposer_worker;
Thread executor_worker;
```

### Mudança no `setCurrentBallot`

```java
public synchronized void setCurrentBallot(int ballot) {
    if (ballot > this.current_ballot) {
        this.current_ballot = ballot;
        this.phase1_done_for_ballot = false;   // ← RESET
        this.req_history.resetProposedFlags(); // ← RESET
    }
}
```

**Interpretação**: sempre que a ballot sobe (novo líder, ou reject detectado),
o próximo professor tem de recomeçar a Phase 1 do zero, e os recibos que o
professor anterior estava a processar voltam ao balcão para o novo os agarrar.

### Analogia

- `phase1_done_for_ballot` = *"o professor atual já tocou à porta e recebeu
  autoridade?"*. Se sim, não precisa de o fazer outra vez.
- `next_instance_to_propose` = número da **próxima linha do caderno** onde o
  professor vai escrever.
- `next_instance_to_execute` = número da **próxima linha** que o empregado
  vai executar.

---

## 6. Passo 3 — Split `MainLoop` → `Proposer` + `Executor`

### O que existia

Uma única thread `MainLoop` fazia tudo:
1. Corria Paxos até decidir.
2. Executava o comando no `TradeManager`.
3. Passava à próxima instância.

Impossível pipelinar — cada instância bloqueava a seguinte.

### O que existe agora

Duas threads independentes:

**`Proposer`** — só propõe:
- Espera que haja pedido em `pending` e que seja líder.
- Dispara Phase 1 (só na primeira instância da ballot) + Phase 2 (async).
- Incrementa `next_instance_to_propose` e volta ao topo — **não espera** que
  a instância decida.

**`Executor`** — só aplica:
- Espera que a instância `next_instance_to_execute` fique `decided`.
- Vai buscar o pedido original em `req_history` (pelo `command_id`).
- Aplica no `TradeManager` (SELL / BUY / POPULATE / ...).
- Chama `request_record.setResponse(result)` → desbloqueia a thread do gRPC
  do cliente.
- Incrementa cursor e repete.

### Analogia

Antes: um empregado fazia tudo sozinho (professor + executor).

Agora: **professor** (Proposer) só escreve encomendas no caderno com ordem
combinada com os outros; **empregado** (Executor) só olha para o caderno e
executa as linhas por ordem.

Os dois trabalham em paralelo. Enquanto o empregado ainda executa a linha 5,
o professor já pode estar a escrever a linha 8.

### Wake-up correto (bug crítico corrigido)

Aviso importante que quase esquecemos: no `DidaTradePaxosServiceImpl.learn`
(quando conta quorum e marca `decided`), tem que chamar
`state.executor.wakeup()`, não `main_loop.wakeup()`.

Similarmente, em todos os RPCs de cliente (`sell`, `buy`, `populate`, ...) e
consola (`newballot`), chamamos `state.proposer.wakeup()`.

**Bug que já apanhámos**: se o professor (líder) marca `decided` localmente
mas não acorda o empregado, o próprio servidor líder fica preso sem executar
comandos. Fix: `state.executor.wakeup()` no ramo onde o Proposer conclui a
decisão.

---

## 7. Passo 4 — Skip Phase 1

### Regra

Phase 1 (Prepare) só precisa de ser feita **uma vez por ballot**. Depois disso,
o mesmo líder pode fazer Phase 2 diretamente para todas as instâncias
seguintes.

### Analogia

Professor toca à porta e diz *"sou eu que mando"* uma vez, apanha as
autorizações e enfia-as no bolso. Enquanto for ele o professor, salta este
passo. Novo professor entra? Toca à porta de novo.

### Código chave (`Proposer.java`)

```java
if (!state.getPhase1Done()) {
    // ... corre Phase 1 síncrona ...
    if (!p1_processor.getAccepted()) {
        // reject: sobe ballot, devolve recibo, sai
        request_record.setProposed(false);
        return;
    } else {
        state.setPhase1Done(true);   // ← marca como feita
        if (p1_processor.getValballot() > -1) {
            phase_two_value = p1_processor.getValue();
            request_record.setProposed(false);   // adopted, não é o meu recibo
        }
    }
}
// Phase 2 sempre corre, independentemente
```

**Detalhe subtil**: se a Phase 1 for aceite **mas** algum acceptor tinha um
valor prévio (Paxos regra sagrada: manter valores já aceites), somos
obrigados a propor esse valor antigo. O recibo novo que agarramos volta
ao balcão (`setProposed(false)`). Vai ser reproposto numa instância seguinte.

### Reset da flag

Quando `setCurrentBallot` sobe a ballot → `phase1_done_for_ballot = false`.
O novo líder (ou o mesmo líder após pre-empt) volta a correr Phase 1.

---

## 8. Passo 5 — Pipelining (Phase 2 fire-and-forget)

### Antes (síncrono)

```java
// Phase 2: envia + espera + verifica
for (i = 0; i < n; i++) state.async_stubs[i].phasetwo(req, collector_obs);
p2_collector.waitUntilDone();          // ← BLOQUEIA
if (p2_processor.getAccepted()) {
    entry.decided = true;
    executor.wakeup();
}
```

**Problema**: o Proposer fica parado à espera. Enquanto espera, novas
instâncias não podem começar.

### Agora (assíncrono / fire-and-forget)

```java
DidaTradePaxos.PhaseTwoRequest p2_request = /* ... */;

for (int i = 0; i < n_acceptors; i++) {
    state.async_stubs[acceptors.get(i)].phasetwo(p2_request,
        new StreamObserver<DidaTradePaxos.PhaseTwoReply>() {
            public void onNext(PhaseTwoReply r) {
                if (!r.getAccepted() && r.getMaxballot() > state.getCurrentBallot()) {
                    state.setCurrentBallot(r.getMaxballot());   // pre-empt detectado
                }
            }
            public void onError(Throwable t) {}
            public void onCompleted() {}
        });
}
// SEM waitUntilDone. SEM entry.decided = true. SEM executor.wakeup().
```

### Como decide agora?

O `learn` handler do `DidaTradePaxosServiceImpl` (que já existia) é quem
marca `entry.decided = true` e chama `state.executor.wakeup()`. O fluxo:

1. Proposer dispara `phasetwo` a todos os acceptors.
2. Cada acceptor, ao aceitar, dispara `learn` a todos os learners (fork thread
   no `phasetwo`).
3. Cada learner conta os `n_accepts`. Quando atinge quorum → `decided=true`,
   `executor.wakeup()`.
4. O próprio líder é learner (na ScheduleA todos são AL). Recebe learns e
   marca `decided` no seu próprio log.

O Proposer não tem trabalho nenhum depois de disparar.

### Callback de pre-empt

Se algum acceptor rejeita Phase 2 (porque viu ballot maior), o `StreamObserver`
inline detecta e chama `setCurrentBallot(maxballot)`. Isto automaticamente:
- Marca `phase1_done_for_ballot = false`.
- Faz `resetProposedFlags()`.
- Faz com que o Proposer, no próximo ciclo do outer loop, veja `leader(nova) !=
  my_id` e vá dormir.

**Analogia**: o professor manda cartas mas não fica à espera das respostas.
Se uma resposta diz *"há outro professor mais recente"*, o carteiro volta a
correr, o professor lê a carta, percebe que já não manda, e vai dormir. Os
recibos que ele tinha no bolso voltam ao balcão.

### Fluxo pipelinado

```
Cliente A: sell reqid=101 ─┐
Cliente B: sell reqid=201 ─┼─→ Server 0 (líder):
Cliente A: sell reqid=102 ─┘     addToPending para cada
                                  proposer.wakeup()

Proposer thread:
   pending=[101, 201, 102]
   ↓ (cada iteração)
   [instance=0 SENT reqid=101]  ← não espera
   [instance=1 SENT reqid=201]  ← não espera
   [instance=2 SENT reqid=102]  ← não espera

Acceptors recebem P2 em paralelo, cada um responde e dispara learn:

Learners contam:
   [instance=0 decided]   ← quando chegar a quorum
   [instance=1 decided]
   [instance=2 decided]

Executor thread:
   next=0 → espera instance 0 decided → aplica → next=1
   next=1 → espera instance 1 decided → aplica → next=2
   ...
```

**Chave**: várias instâncias em voo ao mesmo tempo.

---

## 9. Passo 6 — Testes de ballot change

Não requer código novo — apenas validação.

Cenários testados:
- Consola muda ballot 0 → 1 → novo líder faz Phase 1 uma vez e retoma.
- Crash do líder → consola muda para ballot com outro líder → sobreviventes retomam.
- Freeze de 1 acceptor → sistema aguenta (quorum 2 de 3).

---

## 10. Passo 7 — Limpeza

- `MainLoop.java` apagado (substituído por Proposer + Executor).
- Zero referências a `main_loop` no código.

---

## 11. Casos limite tratados

| Caso | Como se resolve |
|---|---|
| Novo líder, cursor em 0, instâncias 0..N já decididas | `if (next_entry.decided) return;` no topo de `proposeInstance`, cursor avança sem gastar pedidos |
| Phase 1 adopta valor prévio | `request_record.setProposed(false)`, recibo volta ao balcão |
| Phase 2 pre-empted | `StreamObserver` inline atualiza `current_ballot`, dispara reset via setter |
| Learn chega fora de ordem | Executor espera pelo `next_to_execute` — se o menor não decidiu ainda, dorme |
| Líder morre | Console força ballot nova para outro server vivo |
| Um acceptor congelado | Quorum de 2 de 3 aguenta; congelado apanha via learns quando descongela |

---

## 12. Ficheiros por linha (referência rápida)

- `RequestHistory.java:64` — `resetProposedFlags()`
- `RequestRecord.java` — flag `proposed` + getter/setter
- `DidaTradeServerState.java:97` — `resetProposedFlags()` no `setCurrentBallot`
- `Proposer.java:26-50` — thread principal com outer loop
- `Proposer.java:52-151` — `proposeInstance` (Phase 1 sync + Phase 2 async)
- `Proposer.java:130-146` — `StreamObserver` inline com callback de pre-empt
- `Executor.java` — thread que aplica por ordem
- `DidaTradePaxosServiceImpl.java:141` — `learn` acorda o Executor
- `DidaTradeMainServiceImpl.java` — cada RPC chama `proposer.wakeup()`
- `DidaTradeMasterServiceImpl.java` — `newballot` chama `proposer.wakeup()`

---

## 13. Próximos passos (Step 3+)

Não são deste doc. Vê o enunciado. Ideias soltas:
- Ballot arithmetic + Vertical Paxos II (§4.3).
- Fast Paxos (§4.2).
- State transfer real (§4.6) — hoje `updateCompletedBallot` só funciona sem transferência.
