//go:build windows

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

// Windows documents how to open a directory handle with
// FILE_FLAG_BACKUP_SEMANTICS, but does not document FlushFileBuffers as a
// supported directory-handle operation; FlushFileBuffers on a volume requires
// administrative privilege. There is therefore no supported unprivileged
// equivalent of Unix directory fsync available to this client. SF still
// recovers across process restarts, but it does not promise that namespace
// mutations separated here survive a Windows host crash in that order. Keep
// this explicit no-op aligned with the platform qualification in README.md.
func qwpSfSyncDir(string) error { return nil }
