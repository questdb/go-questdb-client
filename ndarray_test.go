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

package questdb_test

import (
	"testing"

	qdb "github.com/questdb/go-questdb-client/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_ValidShapes(t *testing.T) {
	testCases := []struct {
		name     string
		shape    []uint
		expected []uint
	}{
		{"1D array", []uint{5}, []uint{5}},
		{"2D array", []uint{3, 4}, []uint{3, 4}},
		{"3D array", []uint{2, 3, 4}, []uint{2, 3, 4}},
		{"single element", []uint{1}, []uint{1}},
		{"large array", []uint{100, 200}, []uint{100, 200}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			arr, err := qdb.NewNDArray[float64](tc.shape...)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, arr.Shape())
			assert.Equal(t, len(tc.shape), arr.NDims())

			expectedSize := uint(1)
			for _, dim := range tc.shape {
				expectedSize *= dim
			}
			assert.Equal(t, int(expectedSize), arr.Size())
		})
	}
}

func TestNew_InvalidShapes(t *testing.T) {
	testCases := []struct {
		name  string
		shape []uint
	}{
		{"empty shape", []uint{}},
		{"too many dimensions", make([]uint, qdb.MaxDimensions+1)},
		{"too many elements", []uint{qdb.MaxElements + 1}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Initialize shape with 1 for "too many dimensions" test
			if tc.name == "too many dimensions" {
				for i := range tc.shape {
					tc.shape[i] = 1
				}
			}

			arr, err := qdb.NewNDArray[float64](tc.shape...)
			assert.Error(t, err)
			assert.Nil(t, arr)
		})
	}
}

func TestSetGet_ValidPositions(t *testing.T) {
	arr, err := qdb.NewNDArray[float64](3, 4)
	require.NoError(t, err)

	testCases := []struct {
		positions []uint
		value     float64
	}{
		{[]uint{0, 0}, 1.5},
		{[]uint{2, 3}, 42.0},
		{[]uint{1, 2}, -7.5},
	}

	for _, tc := range testCases {
		err := arr.Set(tc.value, tc.positions...)
		require.NoError(t, err)

		retrieved, err := arr.Get(tc.positions...)
		require.NoError(t, err)
		assert.Equal(t, tc.value, retrieved)
	}
}

func TestSetGet_InvalidPositions(t *testing.T) {
	arr, err := qdb.NewNDArray[float64](3, 4)
	require.NoError(t, err)

	testCases := []struct {
		name      string
		positions []uint
	}{
		{"wrong dimensions", []uint{0}},
		{"out of bounds first dim", []uint{3, 0}},
		{"out of bounds second dim", []uint{0, 4}},
		{"too many dimensions", []uint{0, 0, 0}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := arr.Set(1.0, tc.positions...)
			assert.Error(t, err)

			_, err = arr.Get(tc.positions...)
			assert.Error(t, err)
		})
	}
}

func TestReshape_ValidShapes(t *testing.T) {
	arr, err := qdb.NewNDArray[float64](2, 3)
	require.NoError(t, err)

	value := 1.0
	for i := uint(0); i < 2; i++ {
		for j := uint(0); j < 3; j++ {
			err := arr.Set(value, i, j)
			require.NoError(t, err)
			value++
		}
	}

	testCases := []struct {
		name     string
		newShape []uint
	}{
		{"1D reshape", []uint{6}},
		{"3D reshape", []uint{1, 2, 3}},
		{"different 2D", []uint{3, 2}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reshaped, err := arr.Reshape(tc.newShape...)
			require.NoError(t, err)
			assert.Equal(t, tc.newShape, reshaped.Shape())
			assert.Equal(t, arr.Size(), reshaped.Size())
			assert.Equal(t, arr.GetData(), reshaped.GetData())
		})
	}
}

func TestReshape_InvalidShapes(t *testing.T) {
	arr, err := qdb.NewNDArray[float64](2, 3)
	require.NoError(t, err)

	testCases := []struct {
		name     string
		newShape []uint
	}{
		{"wrong size", []uint{5}},
		{"empty shape", []uint{}},
		{"too large", []uint{qdb.MaxElements + 1}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reshaped, err := arr.Reshape(tc.newShape...)
			assert.Error(t, err)
			assert.Nil(t, reshaped)
		})
	}
}

func TestAppend(t *testing.T) {
	arr, err := qdb.NewNDArray[float64](2, 2)
	require.NoError(t, err)

	values := []float64{1.0, 2.0, 3.0, 4.0}
	for i, val := range values {
		hasMore, err := arr.Append(val)
		require.NoError(t, err)

		if i < len(values)-1 {
			assert.True(t, hasMore, "should have more space")
		} else {
			assert.False(t, hasMore, "should be full")
		}
	}

	assert.Equal(t, values, arr.GetData())
	_, err = arr.Append(5.0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "array is full")
}

func TestResetAppendIndex(t *testing.T) {
	arr, err := qdb.NewNDArray[float64](2, 2)
	require.NoError(t, err)
	for i := 0; i < 4; i++ {
		_, err := arr.Append(float64(i))
		require.NoError(t, err)
	}

	// Reset
	arr.ResetAppendIndex()
	hasMore, err := arr.Append(10.0)
	require.NoError(t, err)
	assert.True(t, hasMore)

	// The first element was overwritten
	val, err := arr.Get(0, 0)
	require.NoError(t, err)
	assert.Equal(t, 10.0, val)
}

func TestFill(t *testing.T) {
	arr, err := qdb.NewNDArray[float64](2, 3)
	require.NoError(t, err)

	fillValue := 42.0
	arr.Fill(fillValue)

	for i := uint(0); i < 2; i++ {
		for j := uint(0); j < 3; j++ {
			val, err := arr.Get(i, j)
			require.NoError(t, err)
			assert.Equal(t, fillValue, val)
		}
	}

	_, err = arr.Append(1.0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "array is full")
}

func TestShape_ReturnsImmutableCopy(t *testing.T) {
	originalShape := []uint{3, 4}
	arr, err := qdb.NewNDArray[float64](originalShape...)
	require.NoError(t, err)

	shape := arr.Shape()
	shape[0] = 999
	actualShape := arr.Shape()
	assert.Equal(t, originalShape, actualShape)
	assert.NotEqual(t, uint(999), actualShape[0])
}

func TestGetData_SharedReference(t *testing.T) {
	arr, err := qdb.NewNDArray[float64](2, 2)
	require.NoError(t, err)

	data := arr.GetData()
	data[0] = 42.0
	val, err := arr.Get(0, 0)
	require.NoError(t, err)
	assert.Equal(t, 42.0, val)
}

func TestMaxLimits(t *testing.T) {
	t.Run("max dimensions", func(t *testing.T) {
		shape := make([]uint, qdb.MaxDimensions)
		for i := range shape {
			shape[i] = 1
		}

		arr, err := qdb.NewNDArray[float64](shape...)
		require.NoError(t, err)
		assert.Equal(t, qdb.MaxDimensions, arr.NDims())
	})

	t.Run("max elements", func(t *testing.T) {
		arr, err := qdb.NewNDArray[float64](qdb.MaxElements)
		require.NoError(t, err)
		assert.Equal(t, qdb.MaxElements, arr.Size())
	})
}

func TestPositionsToIndex(t *testing.T) {
	arr, err := qdb.NewNDArray[float64](3, 4)
	require.NoError(t, err)

	testCases := []struct {
		positions []uint
		value     float64
	}{
		{[]uint{0, 0}, 100.0},
		{[]uint{0, 1}, 101.0},
		{[]uint{1, 0}, 102.0},
		{[]uint{2, 3}, 103.0},
	}

	for _, tc := range testCases {
		err := arr.Set(tc.value, tc.positions...)
		require.NoError(t, err)

		retrieved, err := arr.Get(tc.positions...)
		require.NoError(t, err)
		assert.Equal(t, tc.value, retrieved)
	}
}
