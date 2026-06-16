/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

//go:build !windows

package questdb

// QuestDB server fixture for the QWP fuzz tests. Locates a QuestDB
// distribution, launches it on freshly discovered ports, waits until
// the HTTP service answers /ping, and exposes Stop/Start/Bounce plus
// an /exec SQL helper so the fuzz tests can drive a real server end
// to end.
//
// Server resolution order (first hit wins):
//
//  1. QDB_FUZZ_ADDR=host:httpPort — talk to an already-running server.
//     The fixture does not own its lifecycle, so Bounce is unavailable
//     (bounce-dependent tests skip themselves).
//  2. QDB_JAR=/path/to/questdb-*.jar — launch this jar.
//  3. QDB_REPO=/path/to/questdb — glob core/target for the built
//     questdb-*-SNAPSHOT.jar.
//  4. A sibling ../questdb (or ../../questdb) checkout, same glob.
//
// When none of these resolve (and no JDK is found) the fuzz tests skip,
// so the normal `go test ./...` run on a box without QuestDB stays green.
// The dedicated CI job builds QuestDB from source and sets QDB_REPO.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const (
	fuzzServerStartTimeout = 180 * time.Second
	fuzzServerStopTimeout  = 30 * time.Second
	fuzzServerPingPeriod   = 200 * time.Millisecond
)

// qwpFuzzServer is a launched (or externally provided) QuestDB instance
// shared by every fuzz test in a `go test` run.
type qwpFuzzServer struct {
	// owns is false in QDB_FUZZ_ADDR mode: we connect but never manage
	// the process, so Bounce returns an error.
	owns bool

	javaPath string
	jarPath  string

	baseDir string // temp root, removed on stop()
	dataDir string // QuestDB -d directory
	confDir string // dataDir/conf
	logPath string

	host       string
	httpPort   int
	lineTCPort int
	pgPort     int

	// envOverrides is appended to the JVM child's environment so a
	// per-instance fixture can flip server config keys at boot (e.g.
	// QDB_HTTP_RECV_BUFFER_SIZE for the small-buffer fuzz test).
	// nil/empty on the shared singleton; populated by bootSidecarServer.
	envOverrides map[string]string

	mu      sync.Mutex
	cmd     *exec.Cmd
	waitCh  chan struct{}
	waitErr error
	logFile *os.File
}

var (
	fuzzServerOnce   sync.Once
	fuzzServerShared *qwpFuzzServer
	fuzzServerSkip   string
	fuzzServerErr    error
)

// TestMain guarantees the shared fuzz server is torn down once the whole
// package test run finishes. Without this the launched JVM would leak
// past `go test` since Go has no other per-package teardown hook.
func TestMain(m *testing.M) {
	code := m.Run()
	if fuzzServerShared != nil {
		fuzzServerShared.stop()
	}
	os.Exit(code)
}

// fuzzStrict reports whether an unavailable server must FAIL the test
// instead of skipping it. The dedicated qwp-fuzz CI workflow sets
// QDB_FUZZ_STRICT=1 so a misconfigured build (no jar produced, server
// won't boot, wrong path) is a loud red failure instead of a silent
// green skip that never actually fuzzes anything. The regular
// `go test ./...` run leaves it unset and skips cleanly. It is a
// dedicated opt-in env var, NOT derived from CI=true, because the
// ordinary build.yml job also runs in CI but has no jar and must skip.
func fuzzStrict() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("QDB_FUZZ_STRICT")))
	return v != "" && v != "0" && v != "false" && v != "no"
}

// fuzzServer returns the shared QuestDB instance, booting it on first
// use. A resolved-but-unstartable server always fails the test. An
// unresolvable server skips — unless QDB_FUZZ_STRICT is set, in which
// case it fails (see fuzzStrict).
func fuzzServer(t *testing.T) *qwpFuzzServer {
	t.Helper()
	fuzzServerOnce.Do(func() {
		fuzzServerShared, fuzzServerSkip, fuzzServerErr = launchFuzzServer()
	})
	if fuzzServerErr != nil {
		t.Fatalf("fuzz server failed to start: %v", fuzzServerErr)
	}
	if fuzzServerSkip != "" {
		if fuzzStrict() {
			t.Fatalf("QDB_FUZZ_STRICT is set but the fuzz server is unavailable "+
				"(this must run in CI, not skip): %s", fuzzServerSkip)
		}
		t.Skip(fuzzServerSkip)
	}
	return fuzzServerShared
}

// launchFuzzServer resolves and starts a server. The string return is a
// non-empty skip reason when the environment simply isn't set up for
// fuzzing (no error, just nothing to run against).
func launchFuzzServer() (*qwpFuzzServer, string, error) {
	if addr := strings.TrimSpace(os.Getenv("QDB_FUZZ_ADDR")); addr != "" {
		host, portStr, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, "", fmt.Errorf("QDB_FUZZ_ADDR %q: %w", addr, err)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, "", fmt.Errorf("QDB_FUZZ_ADDR %q: bad port: %w", addr, err)
		}
		s := &qwpFuzzServer{owns: false, host: host, httpPort: port}
		if err := s.waitHTTPReady(fuzzServerStartTimeout); err != nil {
			return nil, "", fmt.Errorf("QDB_FUZZ_ADDR %q not reachable: %w", addr, err)
		}
		return s, "", nil
	}

	javaPath, err := findJava()
	if err != nil {
		return nil, "no JDK found (set JAVA_HOME or PATH); set QDB_FUZZ_ADDR to use a running server", nil
	}
	jarPath, err := findQuestDBJar()
	if err != nil {
		return nil, "no QuestDB jar found (set QDB_JAR, QDB_REPO, or build a sibling ../questdb); or set QDB_FUZZ_ADDR", nil
	}

	baseDir, err := os.MkdirTemp("", "qwpfuzz-")
	if err != nil {
		return nil, "", fmt.Errorf("mkdtemp: %w", err)
	}
	s := &qwpFuzzServer{
		owns:     true,
		javaPath: javaPath,
		jarPath:  jarPath,
		baseDir:  baseDir,
		dataDir:  filepath.Join(baseDir, "data"),
		host:     "127.0.0.1",
	}
	s.confDir = filepath.Join(s.dataDir, "conf")
	s.logPath = filepath.Join(s.dataDir, "log", "log.txt")
	for _, d := range []string{s.confDir, filepath.Dir(s.logPath)} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			os.RemoveAll(baseDir)
			return nil, "", fmt.Errorf("mkdir %s: %w", d, err)
		}
	}
	// Best-effort: QuestDB serves /exec and QWP without mime.types, but
	// copying it (as fixture.py does) silences a startup warning and
	// matches the proven layout.
	copyMimeTypes(jarPath, s.confDir)

	if err := s.discoverPorts(); err != nil {
		os.RemoveAll(baseDir)
		return nil, "", err
	}
	if err := s.start(); err != nil {
		log := s.tailLog(4000)
		s.stop()
		return nil, "", fmt.Errorf("%w\n--- QuestDB log tail ---\n%s", err, log)
	}
	return s, "", nil
}

// bootSidecarServer launches a private QuestDB instance for ONE test,
// independent of the shared singleton, with the given env overrides
// applied to the JVM child. Used by fuzz tests that need a server-side
// config knob the singleton doesn't expose (e.g. small recv buffer,
// forced wire fragmentation). The instance is torn down via t.Cleanup.
//
// Requires fixture-launched mode (Java + jar resolvable). Skips in
// QDB_FUZZ_ADDR mode (external server we can't restart with custom
// env). Honours QDB_FUZZ_STRICT exactly like the shared fixture: a
// resolved-but-unstartable server always fails; an unresolved one
// fails under STRICT and skips otherwise.
func bootSidecarServer(t *testing.T, envOverrides map[string]string) *qwpFuzzServer {
	t.Helper()
	if strings.TrimSpace(os.Getenv("QDB_FUZZ_ADDR")) != "" {
		t.Skip("sidecar server requires fixture-launched mode (QDB_FUZZ_ADDR is set — external server we can't restart with custom env)")
	}
	javaPath, err := findJava()
	if err != nil {
		if fuzzStrict() {
			t.Fatalf("QDB_FUZZ_STRICT is set but no JDK is available for sidecar boot: %v", err)
		}
		t.Skip("no JDK found for sidecar boot")
	}
	jarPath, err := findQuestDBJar()
	if err != nil {
		if fuzzStrict() {
			t.Fatalf("QDB_FUZZ_STRICT is set but no QuestDB jar is available for sidecar boot: %v", err)
		}
		t.Skip("no QuestDB jar found for sidecar boot")
	}
	baseDir, err := os.MkdirTemp("", "qwpfuzz-sidecar-")
	if err != nil {
		t.Fatalf("sidecar mkdtemp: %v", err)
	}
	s := &qwpFuzzServer{
		owns:         true,
		javaPath:     javaPath,
		jarPath:      jarPath,
		baseDir:      baseDir,
		dataDir:      filepath.Join(baseDir, "data"),
		host:         "127.0.0.1",
		envOverrides: envOverrides,
	}
	s.confDir = filepath.Join(s.dataDir, "conf")
	s.logPath = filepath.Join(s.dataDir, "log", "log.txt")
	for _, d := range []string{s.confDir, filepath.Dir(s.logPath)} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			os.RemoveAll(baseDir)
			t.Fatalf("sidecar mkdir %s: %v", d, err)
		}
	}
	copyMimeTypes(jarPath, s.confDir)
	if err := s.discoverPorts(); err != nil {
		os.RemoveAll(baseDir)
		t.Fatalf("sidecar ports: %v", err)
	}
	if err := s.start(); err != nil {
		log := s.tailLog(4000)
		s.stop()
		t.Fatalf("sidecar start: %v\n--- QuestDB log tail ---\n%s", err, log)
	}
	t.Cleanup(s.stop)
	return s
}

// findJava mirrors fixture.py:_find_java — prefer $JAVA_HOME/bin/java,
// fall back to PATH.
func findJava() (string, error) {
	if jh := strings.TrimSpace(os.Getenv("JAVA_HOME")); jh != "" {
		cand := filepath.Join(jh, "bin", "java")
		if fi, err := os.Stat(cand); err == nil && !fi.IsDir() {
			return cand, nil
		}
	}
	return exec.LookPath("java")
}

// findQuestDBJar mirrors fixture.py:install_questdb_from_repo's jar
// discovery (core/target/**/questdb*-SNAPSHOT.jar), plus direct QDB_JAR.
func findQuestDBJar() (string, error) {
	if jar := strings.TrimSpace(os.Getenv("QDB_JAR")); jar != "" {
		if fi, err := os.Stat(jar); err == nil && !fi.IsDir() {
			return jar, nil
		}
		return "", fmt.Errorf("QDB_JAR=%q does not exist", jar)
	}

	var repos []string
	if r := strings.TrimSpace(os.Getenv("QDB_REPO")); r != "" {
		repos = append(repos, r)
	}
	// Sibling checkouts relative to the test working directory (the
	// package dir, e.g. .../go-questdb-client).
	repos = append(repos, filepath.Join("..", "questdb"), filepath.Join("..", "..", "questdb"))

	for _, repo := range repos {
		if jar := pickNewestServerJar(filepath.Join(repo, "core", "target")); jar != "" {
			abs, _ := filepath.Abs(jar)
			return abs, nil
		}
	}
	return "", errors.New("questdb-*-SNAPSHOT.jar not found")
}

// isServerJar matches the QuestDB server jar and rejects the sibling
// -tests / -sources / -javadoc jars (the glob "questdb*-SNAPSHOT.jar"
// already excludes "-SNAPSHOT-tests.jar" by suffix, but be explicit).
func isServerJar(name string) bool {
	if !strings.HasPrefix(name, "questdb") || !strings.HasSuffix(name, "-SNAPSHOT.jar") {
		return false
	}
	for _, bad := range []string{"-tests", "-sources", "-javadoc"} {
		if strings.Contains(name, bad) {
			return false
		}
	}
	return true
}

// pickNewestServerJar returns the most recently modified server jar
// under dir (glob + a deeper walk for nested layouts), or "". Newest
// wins so a stale jar from an older build/version never shadows a fresh
// one — CI clones fresh so there is only one, but local dev trees
// accumulate multiple SNAPSHOT versions.
func pickNewestServerJar(dir string) string {
	seen := map[string]struct{}{}
	var best string
	var bestMod time.Time
	consider := func(path string) {
		if _, dup := seen[path]; dup {
			return
		}
		seen[path] = struct{}{}
		if !isServerJar(filepath.Base(path)) {
			return
		}
		fi, err := os.Stat(path)
		if err != nil || fi.IsDir() {
			return
		}
		if best == "" || fi.ModTime().After(bestMod) {
			best, bestMod = path, fi.ModTime()
		}
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "questdb*-SNAPSHOT.jar"))
	for _, m := range matches {
		consider(m)
	}
	_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			consider(path)
		}
		return nil
	})
	return best
}

func copyMimeTypes(jarPath, destConfDir string) {
	src := filepath.Join(filepath.Dir(jarPath), "classes", "io", "questdb", "site", "conf", "mime.types")
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()
	out, err := os.Create(filepath.Join(destConfDir, "mime.types"))
	if err != nil {
		return
	}
	defer out.Close()
	_, _ = io.Copy(out, in)
}

// discoverPorts grabs three free TCP ports for http / line.tcp / pg. Same
// bind-then-close hack as fixture.py:discover_avail_ports — racy but fine
// for tests, and reused verbatim across a Bounce so the server rebinds
// the same ports.
func (s *qwpFuzzServer) discoverPorts() error {
	ports := make([]int, 0, 3)
	listeners := make([]net.Listener, 0, 3)
	for i := 0; i < 3; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			for _, x := range listeners {
				x.Close()
			}
			return fmt.Errorf("discover ports: %w", err)
		}
		listeners = append(listeners, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	for _, l := range listeners {
		l.Close()
	}
	s.httpPort, s.lineTCPort, s.pgPort = ports[0], ports[1], ports[2]
	return nil
}

// serverConf mirrors fixture.py's generated server.conf for the non-auth,
// non-UDP fuzz path. QWP-over-WebSocket rides the HTTP port with no extra
// config.
func (s *qwpFuzzServer) serverConf() string {
	return fmt.Sprintf(`http.bind.to=0.0.0.0:%d
line.tcp.net.bind.to=0.0.0.0:%d
pg.net.bind.to=0.0.0.0:%d
http.min.enabled=false
line.udp.enabled=false
qwp.udp.enabled=false
line.tcp.maintenance.job.interval=100
line.tcp.min.idle.ms.before.writer.release=300
telemetry.enabled=false
cairo.commit.lag=100
cairo.writer.data.append.page.size=64k
cairo.writer.data.index.value.append.page.size=64k
line.tcp.commit.interval.fraction=0.1
`, s.httpPort, s.lineTCPort, s.pgPort)
}

// start writes the config and launches the JVM, blocking until /ping
// answers 204 or the process dies / times out. Idempotent: if a JVM is
// already managed by this fixture, returns nil immediately (so a
// defensive t.Cleanup(start) is safe regardless of test state).
func (s *qwpFuzzServer) start() error {
	if !s.owns {
		return nil
	}
	s.mu.Lock()
	already := s.cmd != nil
	s.mu.Unlock()
	if already {
		return nil
	}
	if err := os.WriteFile(filepath.Join(s.confDir, "server.conf"), []byte(s.serverConf()), 0o644); err != nil {
		return fmt.Errorf("write server.conf: %w", err)
	}
	f, err := os.OpenFile(s.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open log: %w", err)
	}

	// Verbatim from fixture.py:launch_args. "-Dnoebug" is QuestDB's own
	// (deliberately misspelled) debug-off switch — do not "fix" it.
	cmd := exec.Command(s.javaPath,
		"-DQuestDB-Runtime-0",
		"-ea",
		"-Dnoebug",
		"-XX:+UnlockExperimentalVMOptions",
		"-XX:+AlwaysPreTouch",
		"-p", s.jarPath,
		"-m", "io.questdb/io.questdb.ServerMain",
		"-d", s.dataDir,
	)
	cmd.Dir = s.dataDir
	cmd.Stdout = f
	cmd.Stderr = f
	if len(s.envOverrides) > 0 {
		// Strip any pre-existing values for the override keys before
		// appending ours. POSIX leaves the behaviour of duplicate names
		// in execve's envp unspecified, and getenv() in some libc
		// implementations returns the FIRST entry — so an inherited
		// QDB_<KEY>=... would silently win over our override. Dedup is
		// load-bearing for correctness, not stylistic.
		cmd.Env = make([]string, 0, len(os.Environ())+len(s.envOverrides))
		for _, kv := range os.Environ() {
			eq := strings.IndexByte(kv, '=')
			if eq < 0 {
				cmd.Env = append(cmd.Env, kv)
				continue
			}
			if _, override := s.envOverrides[kv[:eq]]; override {
				continue
			}
			cmd.Env = append(cmd.Env, kv)
		}
		for k, v := range s.envOverrides {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
	}

	s.mu.Lock()
	if err := cmd.Start(); err != nil {
		s.mu.Unlock()
		f.Close()
		return fmt.Errorf("start java: %w", err)
	}
	s.cmd = cmd
	s.logFile = f
	s.waitCh = make(chan struct{})
	waitCh := s.waitCh
	s.mu.Unlock()

	go func() {
		err := cmd.Wait()
		s.mu.Lock()
		s.waitErr = err
		s.mu.Unlock()
		close(waitCh)
	}()

	// Make the server launch visible in CI logs (the point of the
	// qwp-fuzz job is that it actually starts a server — a silent skip
	// would be a false green).
	fmt.Fprintf(os.Stderr, "[qwp-fuzz] launched QuestDB pid=%d jar=%s http=127.0.0.1:%d; waiting for /ping\n",
		cmd.Process.Pid, s.jarPath, s.httpPort)

	deadline := time.Now().Add(fuzzServerStartTimeout)
	for {
		select {
		case <-waitCh:
			return fmt.Errorf("QuestDB exited during startup: %v", s.waitErr)
		default:
		}
		if s.pingOK() {
			fmt.Fprintf(os.Stderr, "[qwp-fuzz] QuestDB ready on 127.0.0.1:%d\n", s.httpPort)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s waiting for QuestDB /ping", fuzzServerStartTimeout)
		}
		time.Sleep(fuzzServerPingPeriod)
	}
}

// waitHTTPReady is the external-mode (QDB_FUZZ_ADDR) readiness probe.
func (s *qwpFuzzServer) waitHTTPReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if s.pingOK() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s waiting for /ping", timeout)
		}
		time.Sleep(fuzzServerPingPeriod)
	}
}

func (s *qwpFuzzServer) pingOK() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("http://%s:%d/ping", s.host, s.httpPort), nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusNoContent
}

// pause stops the JVM (SIGTERM with kill fallback) without touching the
// data directory or discovered ports — a subsequent start() boots a
// fresh JVM that adopts the same dataDir and rebinds the same ports.
// Idempotent and a no-op in QDB_FUZZ_ADDR mode. Underlies bounce() (the
// bouncer primitive for the ingress-oracle bounce-torture and
// restart-replay ports) and stop() (which additionally rm's the data
// dir on teardown), and is the primitive the ingress-oracle
// async-connect port calls to arrange a "server not listening yet"
// state.
func (s *qwpFuzzServer) pause() {
	if !s.owns {
		return
	}
	s.mu.Lock()
	cmd, waitCh, logFile := s.cmd, s.waitCh, s.logFile
	s.cmd, s.waitCh, s.logFile = nil, nil, nil
	s.mu.Unlock()

	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-waitCh:
		case <-time.After(fuzzServerStopTimeout):
			_ = cmd.Process.Kill()
			<-waitCh
		}
	}
	if logFile != nil {
		logFile.Close()
	}
}

// kill abruptly terminates the JVM with SIGKILL — no graceful shutdown
// hooks run, no in-flight WS ACKs leave the worker pool, and the OS
// tears down listening + accepted sockets via RST. The abrupt
// counterpart to pause()'s SIGTERM. Required by the restart-replay
// tests in qwp_ingress_server_restart_fuzz_test.go that need to leave
// the client's SF disk holding genuinely-unacked frames across Close:
// SIGTERM lets the JVM's shutdown hooks ack everything before exit,
// which empirically full-drains those tests' 500-5000-row batches
// every time and skips the "frames stay on disk through close" code
// path. Idempotent and a no-op in QDB_FUZZ_ADDR mode.
func (s *qwpFuzzServer) kill() {
	if !s.owns {
		return
	}
	s.mu.Lock()
	cmd, waitCh, logFile := s.cmd, s.waitCh, s.logFile
	s.cmd, s.waitCh, s.logFile = nil, nil, nil
	s.mu.Unlock()

	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
		<-waitCh
	}
	if logFile != nil {
		logFile.Close()
	}
}

// stop terminates the JVM (via pause()) and removes the temp data dir.
// Called once at TestMain teardown; not re-entry-safe with start().
func (s *qwpFuzzServer) stop() {
	s.pause()
	if s.owns && s.baseDir != "" {
		os.RemoveAll(s.baseDir)
	}
}

// bounce restarts the server on the same ports and data dir, exercising
// the client's reconnect/replay path: SIGTERM, ~500ms down, then a fresh
// JVM rebinds the identical ports and dataDir (no data loss — only stop()
// removes baseDir). Consumed by the ingress-oracle bounce-torture test.
// Returns an error in QDB_FUZZ_ADDR mode (the fixture does not own that
// process, so bounce-dependent tests skip themselves).
func (s *qwpFuzzServer) bounce() error {
	if !s.owns {
		return errors.New("cannot bounce a server in QDB_FUZZ_ADDR mode")
	}
	s.pause()
	// Brief settle so the OS can release the listening sockets before
	// the new JVM rebinds the same ports (fixture.py BounceThread does
	// the same with a short randomized sleep).
	time.Sleep(500 * time.Millisecond)
	return s.start()
}

func (s *qwpFuzzServer) tailLog(n int) string {
	if s.logPath == "" {
		return "(no log)"
	}
	b, err := os.ReadFile(s.logPath)
	if err != nil {
		return fmt.Sprintf("(log unreadable: %v)", err)
	}
	if len(b) > n {
		b = b[len(b)-n:]
	}
	return string(b)
}

// connConf is the QWP connection string for senders / query clients.
func (s *qwpFuzzServer) connConf() string {
	return fmt.Sprintf("ws::addr=%s:%d;", s.host, s.httpPort)
}

// wsAddr is the host:port for QWP senders that assemble their own
// connection string (sf_dir / reconnect / auto_flush tuning) instead of
// using connConf — used by the ingress-oracle bounce-torture test.
func (s *qwpFuzzServer) wsAddr() string {
	return fmt.Sprintf("%s:%d", s.host, s.httpPort)
}

// execSQL runs SQL via the HTTP /exec endpoint (used for DDL/DML setup
// and oracle read-back), returning the parsed result or the server's
// error message.
func (s *qwpFuzzServer) execSQL(sql string) (qwpTableResult, error) {
	u, err := url.Parse(fmt.Sprintf("http://%s:%d/exec", s.host, s.httpPort))
	if err != nil {
		return qwpTableResult{}, err
	}
	q := url.Values{}
	q.Set("query", sql)
	u.RawQuery = q.Encode()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return qwpTableResult{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return qwpTableResult{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return qwpTableResult{}, err
	}
	var withErr struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &withErr) == nil && withErr.Error != "" {
		return qwpTableResult{}, fmt.Errorf("server error: %s (sql=%q)", withErr.Error, sql)
	}
	var result qwpTableResult
	if err := json.Unmarshal(body, &result); err != nil {
		return qwpTableResult{}, fmt.Errorf("parse /exec response: %w (body=%s)", err, string(body))
	}
	return result, nil
}

func (s *qwpFuzzServer) mustExec(t *testing.T, sql string) qwpTableResult {
	t.Helper()
	r, err := s.execSQL(sql)
	if err != nil {
		t.Fatalf("execSQL: %v", err)
	}
	return r
}

// dropAllTables clears the database (the _fuzz_loop.py model: one
// long-lived server, drop-all between tests). Consumed by the sender
// fuzz port, which auto-creates tables on first write and relies on
// a clean slate per test.
func (s *qwpFuzzServer) dropAllTables(t *testing.T) {
	t.Helper()
	res, err := s.execSQL("SHOW TABLES")
	if err != nil {
		t.Fatalf("SHOW TABLES: %v", err)
	}
	for _, row := range res.Dataset {
		if len(row) == 0 {
			continue
		}
		name, ok := row[0].(string)
		if !ok {
			continue
		}
		if _, err := s.execSQL("DROP TABLE IF EXISTS '" + name + "'"); err != nil {
			t.Logf("warning: drop table %q: %v", name, err)
		}
	}
}

// awaitRows polls until `table` has at least `want` rows or the deadline
// passes. Replaces the Java tests' in-process engine.awaitTable / WAL
// drain, which a network client cannot do. The last execSQL error (if
// any) is surfaced in the timeout message so "server unreachable the
// whole window" is distinguishable from "WAL never caught up".
func (s *qwpFuzzServer) awaitRows(t *testing.T, table string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	q := fmt.Sprintf("SELECT count() FROM '%s'", table)
	var lastN int64
	var lastErr error
	for {
		res, err := s.execSQL(q)
		if err != nil {
			lastErr = err
		} else if len(res.Dataset) == 1 && len(res.Dataset[0]) == 1 {
			if n, ok := toInt64(res.Dataset[0][0]); ok {
				lastN = n
				if n >= int64(want) {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout: table %q reached %d / %d rows within %s (last execSQL err: %v)",
				table, lastN, want, timeout, lastErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// toInt64 coerces a JSON-decoded numeric (float64 / json.Number / string)
// to int64.
func toInt64(v interface{}) (int64, bool) {
	switch x := v.(type) {
	case float64:
		return int64(x), true
	case json.Number:
		n, err := x.Int64()
		return n, err == nil
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}
