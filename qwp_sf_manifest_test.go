package questdb

import (
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQwpSfDualRecordJavaGoldenBytes(t *testing.T) {
	tests := []struct {
		name                      string
		magic                     uint32
		generation, first, second int64
		golden                    string
	}{
		{
			name:       "manifest",
			magic:      qwpSfManifestMagic,
			generation: 1,
			first:      2,
			second:     7,
			golden:     "53464d3101000000010000000000000002000000000000000700000000000000000000000000000000000000000000000000000000000000000000007a4d6d90",
		},
		{
			name:       "ack-watermark",
			magic:      qwpSfAckWatermarkMagic,
			generation: 1,
			first:      42,
			second:     0,
			golden:     "414b57310100000001000000000000002a00000000000000000000000000000000000000000000000000000000000000000000000000000000000000ba81507e",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			want, err := hex.DecodeString(tc.golden)
			require.NoError(t, err)
			got := make([]byte, qwpSfDualRecordSize)
			qwpSfEncodeDualRecord(got, tc.magic, tc.generation, tc.first, tc.second)
			assert.Equal(t, want, got)
		})
	}
}

func TestQwpSfManifestRoundTripAlternationAndClamps(t *testing.T) {
	dir := t.TempDir()
	m, err := qwpSfManifestCreate(dir, 10, 20)
	require.NoError(t, err)
	path := filepath.Join(dir, qwpSfManifestFileName)
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Len(t, b, int(qwpSfDualRecordFileSize))
	assert.Equal(t, make([]byte, qwpSfDualRecordSize), b[:qwpSfDualRecordSize])
	assert.Equal(t, qwpSfManifestMagic, binary.LittleEndian.Uint32(b[qwpSfDualRecordSlotSize:qwpSfDualRecordSlotSize+4]))

	require.NoError(t, m.update(5, 15))
	assert.Equal(t, int64(1), m.generation, "independent regressions clamp to a no-op")
	require.NoError(t, m.update(12, 18))
	assert.Equal(t, int64(12), m.headBase)
	assert.Equal(t, int64(20), m.activeBase)
	require.NoError(t, m.update(12, 25))
	require.NoError(t, m.close())

	reopened, err := qwpSfManifestOpen(dir)
	require.NoError(t, err)
	require.NotNil(t, reopened)
	defer reopened.close()
	assert.Equal(t, int64(12), reopened.headBase)
	assert.Equal(t, int64(25), reopened.activeBase)
}

func TestQwpSfManifestSurvivesOneTornRecord(t *testing.T) {
	dir := t.TempDir()
	m, err := qwpSfManifestCreate(dir, 10, 20)
	require.NoError(t, err)
	require.NoError(t, m.update(12, 25))
	require.NoError(t, m.close())

	path := filepath.Join(dir, qwpSfManifestFileName)
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt(make([]byte, qwpSfDualRecordSize), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	reopened, err := qwpSfManifestOpen(dir)
	require.NoError(t, err)
	require.NotNil(t, reopened)
	defer reopened.close()
	assert.Equal(t, int64(10), reopened.headBase)
	assert.Equal(t, int64(20), reopened.activeBase)
}

func TestQwpSfManifestQuarantinesCreationDebris(t *testing.T) {
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{name: "wrong-size", body: []byte("bad")},
		{name: "both-invalid", body: make([]byte, qwpSfDualRecordFileSize)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, qwpSfManifestFileName)
			require.NoError(t, os.WriteFile(path, tc.body, 0o644))
			m, err := qwpSfManifestOpen(dir)
			require.NoError(t, err)
			assert.Nil(t, m)
			_, err = os.Stat(path + ".corrupt")
			require.NoError(t, err)
			_, err = os.Stat(path)
			assert.True(t, os.IsNotExist(err))
		})
	}
}
