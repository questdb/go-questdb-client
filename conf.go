/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
	"strings"
)

type configData struct {
	schema        string
	keyValuePairs map[string]string
}

func parseConfigString(conf string) (configData, error) {
	var (
		key   = &strings.Builder{}
		value = &strings.Builder{}
		isKey = true

		nextRune             rune
		isEscaping           bool
		hasTrailingSemicolon bool
		result               configData
	)

	schemaStr, conf, found := strings.Cut(conf, "::")
	if !found {
		return result, NewConfigStrParseError("no schema separator found '::'")
	}

	result.schema = schemaStr

	if len(conf) == 0 {
		return result, NewConfigStrParseError("'addr' key not found")
	}

	if strings.HasSuffix(conf, ";") {
		hasTrailingSemicolon = true
	} else {
		conf = conf + ";" // add trailing semicolon if it doesn't exist
	}

	keyValueStr := []rune(conf)
	for idx, rune := range keyValueStr {
		if idx < len(conf)-1 {
			nextRune = keyValueStr[idx+1]
		} else {
			nextRune = 0
		}
		switch rune {
		case ';':
			if isKey {
				if nextRune == 0 && !hasTrailingSemicolon {
					return result, NewConfigStrParseError("unexpected end of string")
				}
				return result, NewConfigStrParseError("invalid key character ';'")
			}

			if !isEscaping && nextRune == ';' {
				isEscaping = true
				continue
			}

			if isEscaping {
				value.WriteRune(rune)
				isEscaping = false
				continue
			}

			result.keyValuePairs[key.String()] = value.String()

			key.Reset()
			value.Reset()
			isKey = true
		case '=':
			if isKey {
				isKey = false
			} else {
				value.WriteRune(rune)
			}
		default:
			if isKey {
				key.WriteRune(rune)
			} else {
				value.WriteRune(rune)
			}
		}
	}

	if isEscaping {
		return result, NewConfigStrParseError("unescaped ';'")
	}

	return result, nil
}
