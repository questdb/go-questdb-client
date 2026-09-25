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

package questdb

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests isolate option creation/application from downstream resolution.
// Constructor/emission timing and panic containment use real client paths in
// TestQwpTransportCleanupLoggerRouting instead of duplicating full lifecycles
// for every permutation of options here.
func TestQwpLoggerOptionOrdering(t *testing.T) {
	families := []struct {
		name string
		// prepare creates the public options; the returned function applies
		// them in order, as the corresponding constructor does.
		prepare func([]*slog.Logger) func() *slog.Logger
	}{
		{"sender", func(loggers []*slog.Logger) func() *slog.Logger {
			var opts []LineSenderOption
			for _, l := range loggers {
				opts = append(opts, WithLogger(l))
			}
			return func() *slog.Logger {
				var cfg lineSenderConfig
				for _, opt := range opts {
					opt(&cfg)
				}
				return cfg.logger
			}
		}},
		{"facade", func(loggers []*slog.Logger) func() *slog.Logger {
			var opts []QuestDBOption
			for _, l := range loggers {
				opts = append(opts, WithQuestDBLogger(l))
			}
			return func() *slog.Logger {
				var cfg questDBConfig
				for _, opt := range opts {
					opt(&cfg)
				}
				return cfg.logger
			}
		}},
		{"query", func(loggers []*slog.Logger) func() *slog.Logger {
			var opts []QwpQueryClientOption
			for _, l := range loggers {
				opts = append(opts, WithQwpQueryClientLogger(l))
			}
			return func() *slog.Logger {
				var cfg qwpQueryClientConfig
				for _, opt := range opts {
					opt(&cfg)
				}
				return cfg.logger
			}
		}},
	}
	for _, family := range families {
		for _, order := range []string{"omitted", "nil", "configured", "configured-then-nil", "nil-then-configured"} {
			t.Run(family.name+"/"+order, func(t *testing.T) {
				previous := slog.Default()
				t.Cleanup(func() { slog.SetDefault(previous) })
				creation, application, resolution, configured := &recordCapturingHandler{}, &recordCapturingHandler{}, &recordCapturingHandler{}, &recordCapturingHandler{}
				custom := slog.New(configured)
				var loggers []*slog.Logger
				wantConfigured := false
				switch order {
				case "nil":
					loggers = []*slog.Logger{nil}
				case "configured":
					loggers, wantConfigured = []*slog.Logger{custom}, true
				case "configured-then-nil":
					loggers = []*slog.Logger{custom, nil}
				case "nil-then-configured":
					loggers, wantConfigured = []*slog.Logger{nil, custom}, true
				}

				slog.SetDefault(slog.New(creation))
				apply := family.prepare(loggers)
				slog.SetDefault(slog.New(application))
				logger := apply()
				slog.SetDefault(slog.New(resolution))
				qwpEffectiveLogger(logger).Info("selected sink")

				require.Empty(t, creation.messages(), "option creation must not capture the default")
				require.Empty(t, application.messages(), "nil must not capture the default during option application")
				if wantConfigured {
					require.Equal(t, []string{"selected sink"}, configured.messages())
					require.Empty(t, resolution.messages())
				} else {
					require.Equal(t, []string{"selected sink"}, resolution.messages())
					require.Empty(t, configured.messages(), "nil must clear an earlier option")
				}
			})
		}
	}
}
