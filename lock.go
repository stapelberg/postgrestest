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

package postgrestest

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

func (cfg *Config) lockfn() string {
	return filepath.Join(cfg.dir, "lock")
}

func (cfg *Config) tryLock() error {
	lockfn := cfg.lockfn()
	lockf, err := os.OpenFile(lockfn, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			// The parent directory does not exist.
			// Create the directory and retry.
			if err := os.MkdirAll(cfg.dir, 0755); err != nil {
				return err
			}
			return cfg.tryLock()
		}
		if os.IsExist(err) {
			return fmt.Errorf("Lock file %s already exists -- did another process race us?", lockfn)
		}
		return err
	}
	defer lockf.Close()
	if _, err := fmt.Fprintf(lockf, "%d\n", os.Getpid()); err != nil {
		return err
	}
	return lockf.Close()
}

func (cfg *Config) removeStaleAndTryLock() error {
	lockfn := cfg.lockfn()
	log.Printf("removing stale lockfile %s", lockfn)
	if err := os.Remove(lockfn); err != nil {
		if os.IsNotExist(err) {
			// another process raced our deletion. fallthrough
		} else {
			return err
		}
	}

	// If we had to remove a stale lock, maybe there is a stale Postgres
	// instance still running, too?
	if err := shutdownPostgres(cfg, cfg.dir); err != nil {
		log.Printf("stale postgres cleanup failed: %v", err)
	}

	// Remove log.txt and data dir, otherwise starting a new instance will fail
	if err := os.Remove(filepath.Join(cfg.dir, "log.txt")); err != nil {
		log.Printf("stale postgres cleanup failed: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(cfg.dir, "data")); err != nil {
		log.Printf("stale postgres cleanup failed: %v", err)
	}

	return cfg.tryLock()
}

func (cfg *Config) lock() error {
	b, err := os.ReadFile(cfg.lockfn())
	if err != nil {
		if os.IsNotExist(err) {
			// Lock available
			return cfg.tryLock()
		}
		return err
	}
	// Figure out if the existing lock is stale or live.
	lockPid, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return err
	}
	proc, err := os.FindProcess(int(lockPid))
	if err != nil {
		// stale lock file (non-unix systems)
		return cfg.removeStaleAndTryLock()
	}
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		// stale lock file (unix systems)
		return cfg.removeStaleAndTryLock()
	}
	return fmt.Errorf("already locked by pid %d", lockPid)
}
