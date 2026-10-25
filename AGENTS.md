# AGENTS.md

- Before you finish, `gofmt -l .` must print nothing and `go vet ./...`,
  `go test -race ./...` and the js/wasm tests
  (`GOOS=js GOARCH=wasm go test ./...` with `$(go env GOROOT)/lib/wasm` on
  `PATH`) must pass.
- Standard library only, also for tests. The root package imports neither
  net nor net/http, and `upnet` not net/http; register every new package in
  `deps_test.go`.
- Picks are lock-free and allocation-free: membership and health changes
  publish a new immutable view atomically.
- Callbacks (subscribers, `OnResult`, `OnChange`, `OnRemove`, balancer hooks,
  probes, target functions of `Watch` and `Attach`) never run under a lock,
  and every method is safe to call from them. `Close` never blocks; callers
  wait on `Done`.
- After `Remove`, `RemoveFromGroup`, `RemoveGroup` or `SyncGroup` returns, no
  callback for a removed target starts. Reports carry the target's
  `Generation`. A checker that stops driving a pool resets the health it set.
- A lease is released once and reports one outcome; in-flight counts never
  leak. Retries never return a backend the request already tried.
- The default health check only connects over TCP and closes again: it
  sends no bytes. Never invent probes for protocols without a generic check,
  such as UDP; such targets stay unknown.
- Time comes from the injectable `Clock`; tests use the fake clock instead
  of sleeping, and must not assume a fine-grained clock (Windows).
- Tests must not leak goroutines, timers or connections.
- The README, docs and commits name no other project and quote no benchmark
  numbers.
- Commit subjects are short, English and imperative.
- Never commit host names, IP addresses or credentials.
