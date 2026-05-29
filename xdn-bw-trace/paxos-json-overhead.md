# JSON-encoded Paxos packets cost ~2× the wire bytes vs byteification

GigaPaxos has two on-the-wire formats for `RequestPacket` / `AcceptPacket`
/ `BatchedAcceptReply` / `BatchedCommit`: a packed byteified format and a
JSON-string fallback. The byteified path is gated by `IntegerMap.allInt()`,
which silently flips to `false` whenever a node name isn't a numeric
string. With node IDs like `AR0..AR4` (the convention in
`gigapaxos.xdn.local.properties` and several CloudLab configs), the JSON
fallback runs for every Paxos message — roughly doubling inter-replica
bandwidth.

This was discovered investigating why leader→follower bandwidth was
~8× higher than client↔replica bandwidth at small payloads.

## Empirical evidence

All runs: 100% PUT, 60 s @ rate=50 per replica, 3-replica placement
(linearizable Paxos), identical config except for the active-replica
naming. Three body shapes:

- **1× (47 B)**: default `{"title":"trace_bw","author":"benchmark"}`
- **10× pathological (473 B)**: `{"title":"X×440","author":"benchmark"}`
  — long run of identical bytes, deflate collapses it to almost nothing
- **10× entropy (473 B)**: same shape but `title` is random-looking,
  url-safe-base64 padding — incompressible

### Headline: 1× baseline (47 B body)

| Flow                              | Named (`AR0..AR4`) | Numeric (`0..4`) | Δ      |
|-----------------------------------|-------------------:|-----------------:|-------:|
| Leader → follower (per write)     |             1063 B |        **487 B** | −54%   |
| Follower → leader (per write)     |              411 B |            196 B | −52%   |
| Client → leader (HTTP, per write) |              268 B |            268 B |  0%    |
| Leader → client (HTTP, per write) |              181 B |            181 B |  0%    |
| Leader-out total per write        |             2127 B |        **974 B** | **−54%** |
| Ratio leader-out : client-in      |               8.0× |             3.6× |        |

Client/HTTP framing is unchanged (the AR HTTP frontend doesn't depend on
node-id encoding). The reduction is entirely on the Paxos NIO links.

### Six-run cross-cut: leader → follower per-write bytes

| Body         | Named (JSON)  | Numeric (byteified) | Savings |
|--------------|--------------:|--------------------:|--------:|
| 1× (47 B)    | 1063 B        | 487 B               | **−54%** |
| 10× pathological (`X×440`) | 1056 B | 474 B          | **−55%** |
| 10× entropy (random)       | 1833 B | 836 B          | **−54%** |

The savings ratio is essentially flat across body sizes — the
byteification optimization touches only the Paxos protocol envelope, not
the body propagation path, and the same per-byte JSON-encoding tax
applies to both metadata bytes (heavily, since they're rich in
non-printables) and the QV body bytes (lightly, since deflated payloads
are mostly printable when ISO-8859-1 viewed).

CSVs preserved under `xdn-bw-trace/results/`:
- `bookcatalog-writes-1x.csv` (named, JSON path)
- `bookcatalog-writes-1x-numeric.csv` (numeric, byteified path)
- plus `-10x`, `-10x-entropy`, `-10x-numeric`, `-10x-entropy-numeric`

## Single AcceptPacket on the wire (named-IDs / JSON path)

Captured live with `tcpdump` on `lo`, decoded with scapy:

```
total payload:                                     763 B
  NIO frame header (length prefix):                  8 B   (1%)
  JSON-encoded AcceptPacket:                       755 B
    JSON envelope (Paxos protocol metadata):       471 B  (62%)
      field names: B PT QV E type QID ET SNDR
                   GC_S S QF V ID CA               ~50 B
      values (ballot, slot, sender, paxosID,
              clientAddress, ...)                  ~250 B
      JSON syntax (braces/commas/colons/quotes/
                   \u00XX escapes)                 ~170 B
    QV (the application payload, JSON-escaped):    284 B  (37%)
      XdnHttpRequestBatch wrapper (svc name,
        uncompressed-len, compressed-len)           23 B
      deflated XdnHttpRequest proto
        (HTTP method + URI + headers + body)      ~255 B
```

## Single AcceptPacket on the wire (numeric-IDs / byteified path)

Captured live on the same workload, same config except for the node IDs.
Sums to exactly **392 bytes** — every field accounted for from the
GigaPaxos struct definitions:

| Range   | Layer                          | Bytes | Contents (live values) |
|---------|--------------------------------|------:|------------------------|
| 0–8     | NIO frame                      |   8   | preamble (`0x2b1eb469`) + payload-length (384) |
| 8–32    | `PaxosPacket` header           |  24   | type=90 · sub_type=3 (ACCEPT) · version=0 · paxosID-len=11 · paxosID=`"bookcatalog"` |
| 32–87   | `RequestPacket.SIZEOF_REQUEST_FIXED` | 55 | requestID(8) · stop(1) · clientAddr(6) · listenAddr(6) · entryReplica(4) · entryTime(8) · shouldReturnReqVal(1) · forwardCount(4) · broadcasted(1) · digest-len(4) · reqVal-len(4) · respVal-len(4) · batchSize(4) |
| 87      | digest bytes                   |   0   | empty (`DIGEST_REQUESTS=false`) |
| 87–370  | requestValue (XdnHttpRequestBatch) | 283 | svcname-len(4) · `"bookcatalog"`(11) · uncompressed-len(4) · compressed-len(4) · deflated XdnHttpRequest proto |
| 370     | responseValue bytes            |   0   | empty for accepts |
| 370     | batched-slots bytes            |   0   | singleton batch — already inlined in reqval |
| 370–374 | `ProposalPacket.SIZEOF_PROPOSAL` | 4   | slot=2404 |
| 374–388 | `PValuePacket.SIZEOF_PVALUE`   |  14   | ballot.num(4) · ballot.coord(4) · recovery(1) · medianCheckpointedSlot(4) · noCoalesce(1) |
| 388–392 | `AcceptPacket.SIZEOF_ACCEPT`   |   4   | sender=0 (the numeric node id, on the wire as a raw int) |
| **Total** |                              | **392** | |

The 109 B of Paxos protocol fixed overhead (8 NIO + 24 PaxosPacket + 55
RequestPacket + 4 Proposal + 14 PValue + 4 Accept) is what the JSON path
inflates to 471 B (4.3×) — see the JSON breakdown above.

## Per-write breakdown (numeric, 1× baseline → 487 B/write outbound)

From the live capture: ~1.06 leader→follower packets per write,
averaging 441 B/pkt. The single decoded AcceptPacket above is one
sample.

| Component                       | Bytes/write | Notes |
|---------------------------------|------------:|-------|
| 1 × AcceptPacket (avg)          | ~415        | 1.0 pkt/write × ~415 B avg |
| ↳ Fixed Paxos protocol (NIO + headers) | 109  | 8 + 24 + 55 + 4 + 14 + 4 |
| ↳ deflated XdnHttpRequestBatch  | ~283        | scales with body size |
| residual: small commit/decision follow-ups, heartbeats, accept-replies merged into bidirectional flow | ~70 | empirical gap to 487 |
| **Measured total**              | **487 B/write** |   |

## Side-by-side per AcceptPacket: numeric vs named

| Component                      | Numeric (byteified) | Named (JSON) | Inflation |
|--------------------------------|--------------------:|-------------:|----------:|
| NIO frame header               |         8 B         |        8 B   |     1.0×  |
| **Paxos protocol metadata**    |       **109 B**     |    **471 B** |   **4.3×** |
| ↳ as struct fields (numeric)   | type/sub_type/version/paxosID + RequestPacket(55) + Proposal(4) + PValue(14) + Accept(4) | | |
| ↳ as JSON keys+values (named)  | | `"B":"0:AR1","PT":3,"E":65056,"type":90,"QID":...,"ET":...,"SNDR":65056,"GC_S":...,"S":...,"QF":true,"V":0,"ID":"bookcatalog","CA":"/127.0.0.1:2001"` | |
| Application payload (QV / requestValue) | 283 B      |     284 B    |     1.0×  |
| ↳ deflated XdnHttpRequest proto | 283 B verbatim     | 278 B (after `\u00XX` unescape) + ~6 B JSON-escape | |
| **Total per AcceptPacket**     |       **392 B**     |    **763 B** |   **1.95×** |

The ~2× wire savings is concentrated entirely in the Paxos protocol
metadata. The application payload itself is virtually unchanged — JSON
only adds the modest cost of escaping non-printable bytes inside the
`"QV"` string.

## The gating logic

`paxospackets/RequestPacket.java:815-832`:

```java
protected byte[] toBytes(boolean instrument) {
    ...
    // check if we can use byteification at all; if not, use toString()
    if (!((BYTEIFICATION && IntegerMap.allInt()) || instrument)) {
        ...
        return this.byteifiedSelf = this.toString().getBytes(CHARSET);
    }
    // else byteify
    ...
}
```

The same gate appears in `AcceptPacket.toBytes()`, `BatchedAcceptReply`,
and `BatchedCommit`. `BYTEIFICATION` defaults to `true`
(`PaxosConfig.java:644`). The flip happens in `IntegerMap.put()`
(`paxosutil/IntegerMap.java:61-66`):

```java
public int put(NodeIDType node) {
    int id = getID(node);
    if (!node.toString().equals(Integer.valueOf(id).toString()))
        allInteger = false;
    ...
}
```

`getID` hashes the node ID to an int (e.g. `"AR1"` → `65056`), then this
check compares the stringified hash (`"65056"`) to the original name
(`"AR1"`). They don't match, so `allInteger` flips to `false` once and
never recovers. From that point every Paxos packet falls through to
`toString().getBytes(CHARSET)` — which is the JSON encoding from
`toJSONObject()`/`toJSONSmart()`.

The code comment at `RequestPacket.java:797-813` is explicit about the
cost:

> The serialization overhead really matters most for `RequestPacket` and
> `AcceptPacket`. ... byteification makes a non-trivial difference
> (~2× over json-smart and >4× over org.json).

Our measurement (`1063 / 487 = 2.18×`) lands in the predicted range.

## The fix

Use numeric strings for active-replica and reconfigurator IDs. The
key after `active.` / `reconfigurator.` is the only thing that needs
to change:

```properties
# Engages byteification:
active.0=127.0.0.1:2000
active.1=127.0.0.1:2001
...
reconfigurator.5=127.0.0.1:3000

# Disables byteification (currently the default in our local configs):
active.AR0=127.0.0.1:2000
active.AR1=127.0.0.1:2001
...
reconfigurator.RC0=127.0.0.1:3000
```

Reference config: `conf/gigapaxos.xdn.local.numeric.properties`
(numeric variant of `conf/gigapaxos.xdn.local.properties`,
identical otherwise).

## Caveats

- **The wire-format compatibility is asymmetric.** A cluster running
  with mixed naming (some nodes numeric, some named) emits packets in
  whichever format the *sending* JVM's `IntegerMap.allInt()` returns
  at send time. Mixed deployments should be tested before relying on
  this — don't assume nodes negotiate the format.
- **`xdn-cli` and any tooling that pretty-prints replica info** is
  agnostic to whether IDs are numeric (e.g. `xdn service info` happily
  shows `0`, `1`, `3` as the placement). Anything that hard-codes
  `AR<N>` strings would need updating; a quick grep finds none in the
  data plane, only the bandwidth tracer's *probe-attach* helper
  (`build_pid_to_ar` in `trace_bw.py`), which already accepts any
  token after `ReconfigurableNode`.
- **The optimization is purely on the Paxos NIO channel.** Client-facing
  HTTP, state diffs, and Docker manifests are untouched — this is not
  a workload-shape change, just a serialization-path change.
- **Savings ratio is constant across body sizes.** Across 1× (47 B),
  10× pathological, and 10× entropy bodies the leader→follower
  per-write reduction was 54%, 55%, 54%. Both the metadata envelope
  and the body bytes inside `QV` get the same JSON-escape tax in the
  named path, so the optimization scales with payload — there's no
  body shape at which it stops paying off.

## Reproducer

```bash
# Stop existing cluster + clean state
sudo ./bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.xdn.local.properties forceclear all
sg docker -c "docker ps -aq --filter 'name=^c0\.e0\.' | xargs -r docker rm -f"
sudo rm -rf /tmp/gigapaxos /tmp/xdn

# Start with numeric IDs
sg docker -c "./bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.xdn.local.numeric.properties start all"

# Deploy + trace
export XDN_CONTROL_PLANE=localhost
sg docker -c "xdn launch bookcatalog --image=fadhilkurnia/xdn-bookcatalog --state=/app/data/ --deterministic=true"
sudo python3 xdn-bw-trace/trace_bw.py \
    --config conf/gigapaxos.xdn.local.numeric.properties \
    --service bookcatalog \
    --duration 60 --interval 5 --rate 50 --read-ratio 0.0 \
    --output xdn-bw-trace/results/bookcatalog-writes-1x-numeric.csv

# Compare matrices
python3 xdn-bw-trace/plot_bw_graph.py xdn-bw-trace/results/bookcatalog-writes-1x.csv
python3 xdn-bw-trace/plot_bw_graph.py xdn-bw-trace/results/bookcatalog-writes-1x-numeric.csv
```
