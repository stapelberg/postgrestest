// Copyright 2020 Ross Light
// Copyright 2024 Michael Stapelberg
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

// Package postgrestest provides a test harness that starts an ephemeral
// PostgreSQL server. PostgreSQL must be installed locally for this package to
// work.
package postgrestest

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

const superuserName = "postgres"

// A Server represents a running PostgreSQL server.
type Server struct {
	dir     string
	driver  string
	baseURL *url.URL
	conn    *sql.DB

	exited  <-chan struct{}
	waitErr error
}

// A Config configures a PostgreSQL test server.
type Config struct {
	driver string
	dir    string
}

// A Option changes something in Config.
type Option func(any)

// WithSQLDriver sets the SQL driver that postgrestest should use to connect to
// the database. The default is "postgres" (implemented by github.com/lib/pq),
// another tested choice is "pgx" (implemented by github.com/jackc/pgx).
//
// The general recommendation is to use the same driver as you already use for
// the rest of your non-test code, to keep dependencies minimal.
func WithSQLDriver(driver string) Option {
	return func(c any) {
		switch c := c.(type) {
		case *Config:
			c.driver = driver
		case *DBCreator:
			c.driver = driver
		}
	}
}

// WithDir specifies a directory in which postgrestest should set up
// PostgreSQL. By default, a temporary directory is used.
func WithDir(dir string) Option {
	return func(c any) {
		switch c := c.(type) {
		case *Config:
			c.dir = dir
		}
	}
}

// Start starts a PostgreSQL server with an empty database and waits for it to
// accept connections.
//
// Start looks for the programs "pg_ctl" and "initdb" in PATH. If these are not
// found, then Start searches for them in /usr/lib/postgresql/*/bin, preferring
// the highest version found.
func Start(ctx context.Context, opts ...Option) (_ *Server, err error) {
	var cfg Config
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.driver == "" {
		cfg.driver = "postgres"
	}
	if cfg.dir == "" {
		var err error
		// Prepare data directory.
		cfg.dir, err = ioutil.TempDir("", "postgrestest")
		if err != nil {
			return nil, fmt.Errorf("start postgres: %w", err)
		}
		defer func() {
			if err != nil {
				os.RemoveAll(cfg.dir)
			}
		}()
	} else {
		// The user specified a directory with known path.
		// Prevent other processes from using the directory.
		if err := lock(cfg.dir); err != nil {
			return nil, err
		}
	}
	dataDir := filepath.Join(cfg.dir, "data")
	err = runCommand("initdb",
		"--no-sync",
		"--username="+superuserName,
		"-D", dataDir)
	if err != nil {
		return nil, fmt.Errorf("start postgres: %w", err)
	}
	const configFormat = "" +
		"listen_addresses = ''\n" +
		"unix_socket_directories = '%s'\n" +
		"fsync = off\n" +
		"synchronous_commit = off\n" +
		"full_page_writes = off\n"
	err = ioutil.WriteFile(
		filepath.Join(dataDir, "postgresql.conf"),
		[]byte(fmt.Sprintf(configFormat, filepath.ToSlash(cfg.dir))),
		0666)
	if err != nil {
		return nil, fmt.Errorf("start postgres: %w", err)
	}

	// Start server process.
	// On Unix systems, pg_ctl runs as a daemon.
	// On Windows systems, pg_ctl runs in the foreground (not well-documented) and
	// drops privileges as needed.
	logFile := filepath.Join(cfg.dir, "log.txt")
	proc, err := command("pg_ctl", "start", "--no-wait", "--pgdata="+dataDir, "--log="+logFile)
	if err != nil {
		return nil, fmt.Errorf("start postgres: %w", err)
	}
	if err := proc.Start(); err != nil {
		return nil, fmt.Errorf("start postgres: %w", err)
	}
	exited := make(chan struct{})
	srv := &Server{
		dir:    cfg.dir,
		driver: cfg.driver,
		baseURL: &url.URL{
			Scheme: "postgres",
			Host:   "localhost",
			User:   url.UserPassword(superuserName, ""),
			Path:   "/",
			RawQuery: (&url.Values{
				"host":    []string{cfg.dir},
				"sslmode": []string{"disable"},
			}).Encode(),
		},
		exited: exited,
	}
	go func() {
		defer close(exited)
		srv.waitErr = proc.Wait()
	}()

	// Wait for server to come up healthy.
	srv.conn, err = sql.Open(cfg.driver, srv.DefaultDatabase())
	if err != nil {
		// Failure to open means the DSN is invalid. Connections aren't created
		// until we ping.
		srv.stop()
		return nil, fmt.Errorf("start postgres: %w", err)
	}
	defer func() {
		if err != nil {
			srv.conn.Close()
		}
	}()
	srv.conn.SetMaxOpenConns(1)
	for {
		select {
		case <-ctx.Done():
			srv.stop()
			logOutput, _ := ioutil.ReadFile(logFile)
			if len(logOutput) == 0 {
				return nil, fmt.Errorf("start postgres: %w", ctx.Err())
			}
			return nil, fmt.Errorf("start postgres: %w\n%s", ctx.Err(), logOutput)
		default:
			if err := srv.conn.PingContext(ctx); err == nil {
				return srv, nil
			}
		}
	}
}

// DefaultDatabase returns the data source name of the default "postgres" database.
func (srv *Server) DefaultDatabase() string {
	return srv.dsn("postgres")
}

func dsnString(u *url.URL) string {
	dsn := u.String()
	// We need to set a non-empty Host, otherwise the / separating hostname and
	// path will be missing from the String() representation. Hence, we replace
	// the first 'localhost' Host with the empty string textually:
	dsn = strings.Replace(dsn, "localhost", "", 1)
	return dsn
}

func (srv *Server) dsn(dbName string) string {
	u := *srv.baseURL
	u.Path = dbName
	return dsnString(&u)
}

// NewDatabase opens a connection to a freshly created database on the server.
func (srv *Server) NewDatabase(ctx context.Context) (*sql.DB, error) {
	dsn, err := srv.CreateDatabase(ctx)
	if err != nil {
		return nil, err
	}
	return sql.Open(srv.driver, dsn)
}

// CreateDatabase creates a new database on the server and returns its
// data source name.
func (srv *Server) CreateDatabase(ctx context.Context) (string, error) {
	dbc := &DBCreator{
		baseDSN: &dsn{
			u: srv.baseURL,
		},
		db: srv.conn,
	}
	return dbc.CreateDatabase(ctx)
}

// Cleanup shuts down the server and deletes any on-disk files the server used.
func (srv *Server) Cleanup() {
	if srv.conn != nil {
		srv.conn.Close()
	}
	srv.stop()
	os.RemoveAll(srv.dir)
}

func shutdownPostgres(dir string) error {
	// Use Immediate Shutdown mode. We don't care about data corruption.
	// https://www.postgresql.org/docs/current/server-shutdown.html
	//
	// TODO(someday): What happens if this fails?
	return runCommand("pg_ctl", "stop",
		"--pgdata="+filepath.Join(dir, "data"),
		"--mode=immediate",
		"--wait")
}

func (srv *Server) stop() {
	shutdownPostgres(srv.dir)
	<-srv.exited
}

// command creates an *exec.Cmd for the given PostgreSQL program. If it it
// cannot find the program on the PATH, then it searches some well-known
// PostgreSQL installation paths.
func command(name string, args ...string) (*exec.Cmd, error) {
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	p, lookErr := exec.LookPath(name)
	if lookErr == nil {
		return exec.Command(p, args...), nil
	}
	// Find PostgreSQL installation path. If this doesn't work, return the
	// original LookPath error, since the runner of the test should add the binary
	// to their PATH if it can't be found.
	postgresBin.init.Do(findPostgresBin)
	if postgresBin.dir == "" {
		return nil, lookErr
	}
	p = filepath.Join(postgresBin.dir, name)
	if _, err := os.Stat(p); err != nil {
		return nil, lookErr
	}
	return exec.Command(p, args...), nil
}

func findPostgresBin() {
	dir := "/usr/lib/postgresql"
	if runtime.GOOS == "windows" {
		dir = `C:\Program Files\PostgreSQL`
	}
	listing, err := ioutil.ReadDir(dir)
	if err != nil {
		return
	}
	maxVersion := -1
	for _, ent := range listing {
		v, err := strconv.ParseInt(ent.Name(), 10, 0)
		if err != nil || v <= 0 {
			continue
		}
		if int(v) > maxVersion {
			maxVersion = int(v)
		}
	}
	if maxVersion < 0 {
		return
	}
	postgresBin.dir = filepath.Join(dir, strconv.Itoa(maxVersion), "bin")
}

var postgresBin struct {
	init sync.Once
	dir  string
}

func runCommand(name string, args ...string) error {
	c, err := command(name, args...)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	out, err := c.CombinedOutput()
	if errors.As(err, new(*exec.ExitError)) {
		return fmt.Errorf("%s: %s", name, out)
	}
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

func randomString(n int) (string, error) {
	enc := base64.RawURLEncoding
	bits := make([]byte, enc.DecodedLen(n))
	if _, err := rand.Read(bits); err != nil {
		return "", fmt.Errorf("generate random string: %w", err)
	}
	return enc.EncodeToString(bits), nil
}

type dsn struct {
	u *url.URL
}

func dsnFromString(pgurl string) (*dsn, error) {
	u, err := url.Parse(pgurl)
	if err != nil {
		return nil, err
	}
	return &dsn{u}, nil
}

func (d *dsn) DB() string {
	return d.u.Query().Get("host")
}

func (d *dsn) WithPath(path string) string {
	u := *d.u
	u.Path = path
	return dsnString(&u)
}

// DBCreator allows creating ephemeral databases (think CREATE DATABASE) within
// the ephemeral PostgreSQL instance. This functionality is decoupled from the
// [Server] so that it can work with a PostgreSQL instance that was created out
// of process.
type DBCreator struct {
	// config
	driver string

	baseDSN *dsn
	db      *sql.DB
}

// NewDBCreator returns a database creator for the PostgreSQL instance
// identified by the specified DSN.
func NewDBCreator(pgurl string, opts ...Option) (*DBCreator, error) {
	baseDSN, err := dsnFromString(pgurl)
	if err != nil {
		return nil, err
	}
	if baseDSN.u.Host == "" {
		baseDSN.u.Host = "localhost"
	}
	dbc := &DBCreator{
		driver:  "postgres",
		baseDSN: baseDSN,
	}
	for _, opt := range opts {
		opt(dbc)
	}
	db, err := sql.Open(dbc.driver, pgurl)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	dbc.db = db
	return dbc, nil
}

// CreateDatabase creates a new database on the server and returns its
// data source name.
func (c *DBCreator) CreateDatabase(ctx context.Context) (dsn string, _ error) {
	dbName, err := randomString(16)
	if err != nil {
		return "", fmt.Errorf("new database: %w", err)
	}
	if _, err := c.db.ExecContext(ctx, "CREATE DATABASE \""+dbName+"\";"); err != nil {
		return "", fmt.Errorf("new database: %w", err)
	}
	return c.baseDSN.WithPath(dbName), nil
}
