package questdb

import (
	"errors"
	"fmt"
)

const (
	// MaxDimensions defines the maximum dims of NdArray
	MaxDimensions = 32
)

// NdArrayElementType represents the constraint for numeric types that can be used in NdArray
type NdArrayElementType interface {
	~float64
}

// NdArray represents a generic n-dimensional array with shape validation.
// It's designed to be used with the [LineSender.Float64ArrayNDColumn] method for sending
// multi-dimensional arrays to QuestDB via the ILP protocol.
//
// NdArray instances are meant to be reused across multiple calls to the sender
// to avoid memory allocations. Use Append to populate data and
// ResetAppendIndex to reset the array for reuse after sending data.
//
// By default, all values in the array are initialized to zero.
type NdArray[T NdArrayElementType] struct {
	data        []T
	shape       []uint
	appendIndex uint
}

// NewNDArray creates a new NdArray with the specified shape.
// All elements are initialized to zero by default.
func NewNDArray[T NdArrayElementType](shape ...uint) (*NdArray[T], error) {
	if err := validateShape(shape); err != nil {
		return nil, fmt.Errorf("invalid shape: %w", err)
	}
	totalElements := product(shape)
	data := make([]T, totalElements)
	shapeSlice := make([]uint, len(shape))
	copy(shapeSlice, shape)
	return &NdArray[T]{
		shape:       shapeSlice,
		data:        data,
		appendIndex: 0,
	}, nil
}

// Shape returns a copy of the array's shape
func (n *NdArray[T]) Shape() []uint {
	shape := make([]uint, len(n.shape))
	copy(shape, n.shape)
	return shape
}

// NDims returns the number of dimensions
func (n *NdArray[T]) NDims() int {
	return len(n.shape)
}

// Size returns the total number of elements
func (n *NdArray[T]) Size() int {
	return len(n.data)
}

// Set sets a value at the specified multi-dimensional position
func (n *NdArray[T]) Set(v T, positions ...uint) error {
	if len(positions) != n.NDims() {
		return fmt.Errorf("position dimensions (%d) don't match array dimensions (%d)", len(positions), n.NDims())
	}

	index, err := n.positionsToIndex(positions)
	if err != nil {
		return err
	}

	n.data[index] = v
	return nil
}

// Get retrieves a value at the specified multi-dimensional position
func (n *NdArray[T]) Get(positions ...uint) (T, error) {
	var zero T
	if len(positions) != n.NDims() {
		return zero, fmt.Errorf("position dimensions (%d) don't match array dimensions (%d)", len(positions), n.NDims())
	}

	index, err := n.positionsToIndex(positions)
	if err != nil {
		return zero, err
	}

	return n.data[index], nil
}

// Reshape creates a new NdArray with a different shape but same data
func (n *NdArray[T]) Reshape(newShape ...uint) (*NdArray[T], error) {
	if err := validateShape(newShape); err != nil {
		return nil, fmt.Errorf("invalid new shape: %v", err)
	}

	if uint(len(n.data)) != product(newShape) {
		return nil, fmt.Errorf("new shape size (%d) doesn't match data size (%d)",
			product(newShape), len(n.data))
	}

	// Create new array sharing the same data
	newArray := &NdArray[T]{
		shape: make([]uint, len(newShape)),
		data:  n.data, // Share the same underlying data
	}
	copy(newArray.shape, newShape)

	return newArray, nil
}

// Append adds a value to the array sequentially at the current append index.
// Returns true if there's more space for additional values, false if the array is now full.
// Use ResetAppendIndex() to reuse the array for multiple ILP messages.
//
// Example:
//
//	arr, _ := NewNDArray[float64](2, 3) // 2x3 array (6 elements total)
//	hasMore, _ := arr.Append(1.0)       // hasMore = true, index now at 1
//	hasMore, _ = arr.Append(2.0)        // hasMore = true, index now at 2
//	// ... append 4 more values
//	hasMore, _ = arr.Append(6.0)        // hasMore = false, array is full
//
//	// To reuse the array:
//	arr.ResetAppendIndex()
//	arr.Append(10.0)                    // overwrites
//	...
func (n *NdArray[T]) Append(val T) (bool, error) {
	if n.appendIndex >= uint(len(n.data)) {
		return false, errors.New("array is full")
	}
	n.data[n.appendIndex] = val
	n.appendIndex++
	return n.appendIndex < uint(len(n.data)), nil
}

// ResetAppendIndex resets the append index to 0, allowing the NdArray to be reused
// for multiple append operations. This is useful for reusing arrays across multiple
// messages/rows ingestion without reallocating memory.
//
// Example:
//
//	arr, _ := NewNDArray[float64](2) // 1D array with 3 elements
//	arr.Append(2.0)
//	arr.Append(3.0) // array is now full
//
//	// sender.Float64ArrayNDColumn(arr)
//
//	arr.ResetAppendIndex() // reset for reuse
//	arr.Append(4.0)
//	arr.Append(5.0)
func (n *NdArray[T]) ResetAppendIndex() {
	n.appendIndex = 0
}

// Data returns the underlying data slice
func (n *NdArray[T]) Data() []T {
	return n.data
}

// Fill fills the entire array with the specified value
func (n *NdArray[T]) Fill(value T) {
	for i := range n.data {
		n.data[i] = value
	}
	n.appendIndex = uint(len(n.data)) // Mark as full
}

func (n *NdArray[T]) positionsToIndex(positions []uint) (int, error) {
	for i, pos := range positions {
		if pos >= n.shape[i] {
			return 0, fmt.Errorf("position[%d]=%d is out of bounds for dimension size %d",
				i, pos, n.shape[i])
		}
	}

	index := 0
	for i, pos := range positions {
		index += int(pos) * int(product(n.shape[i+1:]))
	}
	return index, nil
}

func validateShape(shape []uint) error {
	if len(shape) == 0 {
		return errors.New("shape cannot be empty")
	}

	if len(shape) > MaxDimensions {
		return fmt.Errorf("too many dimensions: %d exceeds maximum of %d",
			len(shape), MaxDimensions)
	}

	totalElements := product(shape)
	if totalElements > MaxArrayElements {
		return fmt.Errorf("array too large: %d elements exceeds maximum of %d",
			totalElements, MaxArrayElements)
	}

	return nil
}

func product(s []uint) uint {
	if len(s) == 0 {
		return 1
	}
	p := uint(1)
	for _, v := range s {
		if v != 0 && p > MaxArrayElements/v {
			return MaxArrayElements + 1
		}
		p *= v
	}
	return p
}
