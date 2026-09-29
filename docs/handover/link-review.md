# Link module review (src/io/link.c, link.h, tests/unit/link.c)

Worktree /home/user/vsr/build/wt/rev-link, branch `wt/rev-link`, commits on
top of 8fe324d (not pushed, not merged, not rebased). Every fix has its
breaking test first (section "Review: identity, completion and teardown
corners" at the end of tests/unit/link.c: `test_review_own_node`,
`test_review_failed_send_closing`, `test_review_link_wanted_retiring`,
`test_review_teardown_cancel`, `test_review_handshakes_due`), each test
observed failing before its fix. store.c and stream.c untouched.

Checks: clang (ASan+UBSan) `make check` 65/65, gcc (`--disable-sanitize`)
65/65; tests/unit/link also run with seeds 7, 12345, 0xBEEF. Lint:
`make format-check` clean; clang-tidy (`-p build/asan`) and cppcheck (the
Makefile's flags) clean on src/io/link.c and tests/unit/link.c.

## CONFIRMED (5)

1. **Inbound peer claiming the engine's own node id accepted** (area 2).
   `link_hello` (TRUSTED inbound), `vsr_io_links_handshake_done` (EXTERNAL
   inbound) and `vsr_io_links_adopt` only checked that the claimed node is
   in the node table; a node may list itself (`node_set(own, NULL)`) and its
   own replicas' authorizations name it, so the claim passed, the link was
   established, elected the own node's carrier, and its MESSAGEs with
   `from` = an own replica passed the decision-67 check (`lookup(cluster,
   from) == link->node`), letting a reflected handshake or a misbehaving
   peer inject frames as this engine. Fix 82b0de5: refused like an unknown
   node (`-EPROTO`, `-EACCES`, `EINVAL`). Decision 86.

2. **Failed send result without MORE on an already-closing link never
   completes the messages it covered** (area 3). `link_send_complete`'s
   error path freed the entry, recomputed the floor and called `link_close`,
   which returns at once for a CLOSING link (a demoted link with a frame in
   flight, `vsr_io_links_close`, a node change). No `node_queue_ready` ran,
   so the retiring messages whose only remaining send this was stayed
   queued (with `retiring` naming a link entry that then freed and could be
   reused) until an unrelated send of the node completed: the core's SEND op
   could hang. Reachable with a zero-copy send refused at translation
   (decision 62) or a plain send failing after the teardown's SHUTDOWN.
   Fix 3ad80bf: the entry is released through `link_send_release` (floor +
   queue review) like at a NOTIF.

3. **DIAL deadline of a caller-dialed node completed retiring messages
   while a lost link's zero-copy sends may still read them** (area 4).
   `vsr_io_links_deadline` used `node_queue_drop(all = true)` for an
   unanswered LINK_WANTED, completing messages retiring on a CLOSING link
   before its NOTIFs, which breaks the pin rule of vsr.h for the NOTIF's
   duration (decision 83 confines that hazard to node_clear and shutdown,
   and says the unanswered case retries the *waiting* messages). Fix
   0e2d884: `all = false` (the unstarted tail only); the retiring ones
   complete at the NOTIFs as everywhere else.

4. **Teardown never cancels a pending CONNECT or EXTERNAL preamble RECV**
   (area 4). `link_prepare_teardown` waited for `connect_slot` to complete
   before emitting anything. A CONNECT to a black-holed address lasts the
   kernel's SYN retries (~2 min) and the acceptor's raw 8-byte RECV never
   completes while a silent peer stays connected, so the handshake timeout
   closed the link without freeing it: the entry and its descriptor stayed
   CLOSING for as long as the peer wished (a few silent connections exhaust
   the link table of an EXTERNAL acceptor), and `vsr_io_close` waited on a
   dead dial. Fix 9988b6c: a CANCEL of that record precedes the raw CLOSE on
   the shutdown slot (SOCKET and the TCP_NODELAY record are still waited
   for, as before). Decision 87; docs/io-implementation.md updated.

5. **`handshakes_due` leaks when an EXTERNAL link closes before its
   HANDSHAKE op found ring room** (area 4). `link_want_handshake` counts
   the link, `link_forward_handshake` uncounts it only when forwarded;
   `link_close` reset the stage without uncounting, so after one such close
   (timeout, node change, shutdown) every later poll scanned the whole link
   table for nothing. Fix ef183c8: `link_close` decrements the count when
   it leaves that stage.

Docs: 60e8d12 logs decisions 86, 87 (placeholders) in docs/io-design.md
section 10 and updates docs/io-implementation.md ("Links": identity
refusals, the EXTERNAL completion, the teardown records; 7.4 prepare
list). 59a1f99 is clang-format only.

## OPEN, not fixed (1)

6. **Accepted-descriptor flood beyond the 16-entry orphan table leaks
   descriptors** (area 4; the authors' known open question). With the link
   table full (or the engine closing), each accepted connection goes to
   `orphans[]`; a multishot ACCEPT can post up to a CQ batch of accepts
   between two prepares, and the 17th and later leak for the process's
   lifetime, so a connect flood against a full table eventually exhausts
   descriptors. The module is sans-IO (a direct close(2) would close the
   simulation's fake descriptors), so the fix is structural: stop re-arming
   the multishot ACCEPT while no link entry is free (the kernel backlog
   bounds the rest) and re-arm when a link frees, or size `orphans` to the
   engine's completion batch. The harness cannot model it either (its
   listener backlog is 8 and SOCKETS is 24), so no test was written. Left
   for the coordinator to schedule; a `decision` would be needed.

## NOT A BUG (10), examined and rejected

- `link_carve` continuing after `link_frame` when the link was freed
  synchronously by `link_close` ("gone at once" needs `fd < 0`): a link
  that receives frames always has its slot fd, so the CLOSING check holds.
- `LINKS_ASSERT(false)` before `-EBADMSG` when the encoder returns EINVAL:
  a core that broke the pin rule of vsr.h; a debug trap is deliberate.
- `node_queue_review` closing a demoted link whose in-flight send is a
  whole frame (`vec_count > 0` during flight): conservative but within
  decision 83; the bytes reach the peer and the core's RETRY duplicates,
  which VSR tolerates.
- The HELLO nonce is never compared: reserved for KEYED; TRUSTED needs no
  tie-break (decision 41's carrier rule is computed by both ends).
- Copy-only-the-frame, header-first reassembly, the HELD bound (-ENOBUFS),
  CRC coverage (header CRC in `get_frame`, body CRC in `link_carve`), the
  frame length bound (`frame_limit - 24`, multiple of 8, `whole <=
  slab_bytes` asserted in `link_reassemble`), preamble byte-at-a-time
  across runs, `link_frame_missing` returning 0 for a header carving will
  reject: all verified; the copy source/offset arithmetic is right.
- Decision 67 placement: `link_message` checks `lookup(cluster, from) ==
  link->node` per frame on the link the bytes arrived on, so re-authorize,
  revoke, node_clear (links closed first) and carrier switches are covered;
  a MESSAGE before the handshake or on a STREAM link closes `-EPROTO`; a
  TRUSTED/EXTERNAL mismatch closes at the HELLO; a wire purpose outside
  PEER/STREAM is refused by the codec.
- Zero-copy result-then-NOTIF: entry states, the refused-without-MORE slot
  free (decision 62), the floor and `notified_offset` as minima over live
  entries, and the short-send resume (`header_sent = header_begin`, `begin
  = stream_offset - build_bytes`) are consistent, ring wrap skip included.
- `node_queue_remove`'s carrier `encoding` fix-up: the encoded message is
  never the removed one (it has `end != 0` and `end > notified_offset`
  while encoding; node_clear/shutdown close the links first).
- The acceptor's HELLO answer cannot be overtaken by queued messages: the
  only way `link_write_hello` fails on a fresh link is a missing send slab,
  which stops `link_build_messages` too, and both retry in the same order.
- A CARVE_BLOCKED frame on a link the stream module closed inside
  `vsr_io_streams_frame` sets `retry` on a CLOSING link: the next poll's
  drain finds nothing and clears it; counters stay consistent.

## Mutation testing (all caught, link.c restored after each)

- M1 own-node check removed from `link_hello` -> test_review_own_node.
- M2 error path back to `SEND_FREE` + floor -> test_review_failed_send_closing.
- M3 `node_queue_drop(all = true)` at the DIAL deadline -> test_review_link_wanted_retiring.
- M4 `cancel_dial` forced false in the teardown -> test_review_teardown_cancel.
- M5 `handshakes_due--` removed from `link_close` -> test_review_handshakes_due.
- M6 (existing suite) `link_live_sends(link) == 0` made unreachable in
  `node_queue_ready` -> test_review_failed_send_closing.
