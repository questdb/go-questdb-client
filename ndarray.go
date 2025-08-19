package questdb

import (
	"errors"
	"fmt"
)

const (
	// MaxDimensions defines the maximum dims of NdArray
	MaxDimensions = 32

	// MaxElements defines the maximum total number of elements of NdArray
	MaxElements = (1 << 28) - 1
)

// Numeric represents the constraint for numeric types that can be used in NdArray
type Numeric interface {
	~float64
}

// NdArray represents a generic n-dimensional array with shape validation
type NdArray[T Numeric] struct {
	data        []T
	shape       []uint
	appendIndex uint
}

// NewNDArray creates a new NdArray with the specified shape
func NewNDArray[T Numeric](shape ...uint) (*NdArray[T], error) {
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

// Append adds a value to the array sequentially
func (n *NdArray[T]) Append(val T) (bool, error) {
	if n.appendIndex >= uint(len(n.data)) {
		return false, errors.New("array is full")
	}
	n.data[n.appendIndex] = val
	n.appendIndex++
	return n.appendIndex < uint(len(n.data)), nil
}

// ResetAppendIndex resets the append index to 0
func (n *NdArray[T]) ResetAppendIndex() {
	n.appendIndex = 0
}

// GetData returns the underlying data slice
func (n *NdArray[T]) GetData() []T {
	return n.data
}

// Fill fills the entire array with the specified value
func (n *NdArray[T]) Fill(value T) {
	for i := range n.data {
		n.data[i] = value
	}
	n.appendIndex = uint(len(n.data)) // Mark as full
}

// positionsToIndex converts multi-dimensional positions to a flat index
func (n *NdArray[T]) positionsToIndex(positions []uint) (int, error) {
	// Validate positions are within bounds
	for i, pos := range positions {
		if pos >= n.shape[i] {
			return 0, fmt.Errorf("position[%d]=%d is out of bounds for dimension size %d",
				i, pos, n.shape[i])
		}
	}

	// Calculate flat index
	index := 0
	for i, pos := range positions {
		index += int(pos) * int(product(n.shape[i+1:]))
	}
	return index, nil
}

// validateShape validates that shape dimensions are valid
func validateShape(shape []uint) error {
	if len(shape) == 0 {
		return errors.New("shape cannot be empty")
	}

	// Check maximum dimensions limit
	if len(shape) > MaxDimensions {
		return fmt.Errorf("too many dimensions: %d exceeds maximum of %d",
			len(shape), MaxDimensions)
	}

	// Check maximum elements limit
	totalElements := product(shape)
	if totalElements > MaxElements {
		return fmt.Errorf("array too large: %d elements exceeds maximum of %d",
			totalElements, MaxElements)
	}

	return nil
}

// product calculates the product of slice elements with overflow protection
func product(s []uint) uint {
	if len(s) == 0 {
		return 1
	}
	p := uint(1)
	for _, v := range s {
		// Check for potential overflow before multiplication
		if v > 0 && p > MaxElements/v {
			return MaxElements + 1 // Return a value that will trigger validation error
		}
		p *= v
	}
	return p
}
