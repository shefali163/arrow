// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compute

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/bitutil"
	"github.com/apache/arrow/go/v10/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v10/arrow/internal"
	"github.com/apache/arrow/go/v10/arrow/internal/debug"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/arrow/scalar"
)

// ExecCtx holds simple contextual information for execution
// such as the default ChunkSize for batch iteration, whether or not
// to ensure contiguous preallocations for kernels that want preallocation,
// and a reference to the desired function registry to use.
//
// An ExecCtx should be placed into a context.Context by using
// SetExecCtx and GetExecCtx to pass it along for execution.
type ExecCtx struct {
	ChunkSize          int64
	PreallocContiguous bool
	Registry           FunctionRegistry
	ExecChannelSize    int
}

type ctxExecKey struct{}

const DefaultMaxChunkSize = math.MaxInt64

// global default ExecCtx object, initialized with the
// default max chunk size, contiguous preallocations, and
// the default function registry.
var defaultExecCtx ExecCtx

func init() {
	defaultExecCtx.ChunkSize = DefaultMaxChunkSize
	defaultExecCtx.PreallocContiguous = true
	defaultExecCtx.Registry = GetFunctionRegistry()
	defaultExecCtx.ExecChannelSize = 10
}

// SetExecCtx returns a new child context containing the passed in ExecCtx
func SetExecCtx(ctx context.Context, e ExecCtx) context.Context {
	return context.WithValue(ctx, ctxExecKey{}, e)
}

// GetExecCtx returns an embedded ExecCtx from the provided context.
// If it does not contain an ExecCtx, then the default one is returned.
func GetExecCtx(ctx context.Context) ExecCtx {
	e, ok := ctx.Value(ctxExecKey{}).(ExecCtx)
	if ok {
		return e
	}
	return defaultExecCtx
}

// ExecBatch is a unit of work for kernel execution. It contains a collection
// of Array and Scalar values.
//
// ExecBatch is semantically similar to a RecordBatch but for a SQL-style
// execution context. It represents a collection or records, but constant
// "columns" are represented by Scalar values rather than having to be
// converted into arrays with repeated values.
type ExecBatch struct {
	Values []Datum
	// Guarantee is a predicate Expression guaranteed to evaluate to true for
	// all rows in this batch.
	Guarantee Expression
	// Len is the semantic length of this ExecBatch. When the values are
	// all scalars, the length should be set to 1 for non-aggregate kernels.
	// Otherwise the length is taken from the array values. Aggregate kernels
	// can have an ExecBatch formed by projecting just the partition columns
	// from a batch in which case it would have scalar rows with length > 1
	//
	// If the array values are of length 0, then the length is 0 regardless of
	// whether any values are Scalar.
	Len int64
}

func (e ExecBatch) NumValues() int { return len(e.Values) }

// simple struct for defining how to preallocate a particular buffer.
type bufferPrealloc struct {
	bitWidth int
	addLen   int
}

func allocateDataBuffer(ctx *exec.KernelCtx, length, bitWidth int) *memory.Buffer {
	switch bitWidth {
	case 1:
		return ctx.AllocateBitmap(int64(length))
	default:
		bufsiz := int(bitutil.BytesForBits(int64(length * bitWidth)))
		return ctx.Allocate(bufsiz)
	}
}

func addComputeDataPrealloc(dt arrow.DataType, widths []bufferPrealloc) []bufferPrealloc {
	if typ, ok := dt.(arrow.FixedWidthDataType); ok {
		return append(widths, bufferPrealloc{bitWidth: typ.BitWidth()})
	}

	switch dt.ID() {
	case arrow.BINARY, arrow.STRING, arrow.LIST, arrow.MAP:
		return append(widths, bufferPrealloc{bitWidth: 32, addLen: 1})
	case arrow.LARGE_BINARY, arrow.LARGE_STRING, arrow.LARGE_LIST:
		return append(widths, bufferPrealloc{bitWidth: 64, addLen: 1})
	}
	return widths
}

// enum to define a generalized assumption of the nulls in the inputs
type nullGeneralization int8

const (
	nullGenPerhapsNull nullGeneralization = iota
	nullGenAllValid
	nullGenAllNull
)

func getNullGen(val *exec.ExecValue) nullGeneralization {
	dtID := val.Type().ID()
	switch {
	case dtID == arrow.NULL:
		return nullGenAllNull
	case !internal.DefaultHasValidityBitmap(dtID):
		return nullGenAllValid
	case val.IsScalar():
		if val.Scalar.IsValid() {
			return nullGenAllValid
		}
		return nullGenAllNull
	default:
		arr := val.Array
		// do not count if they haven't been counted already
		if arr.Nulls == 0 || arr.Buffers[0].Buf == nil {
			return nullGenAllValid
		}

		if arr.Nulls == arr.Len {
			return nullGenAllNull
		}
	}
	return nullGenPerhapsNull
}

func getNullGenDatum(datum Datum) nullGeneralization {
	var val exec.ExecValue
	switch datum.Kind() {
	case KindArray:
		val.Array.SetMembers(datum.(*ArrayDatum).Value)
	case KindScalar:
		val.Scalar = datum.(*ScalarDatum).Value
	case KindChunked:
		return nullGenPerhapsNull
	default:
		debug.Assert(false, "should be array, scalar, or chunked!")
		return nullGenPerhapsNull
	}
	return getNullGen(&val)
}

// populate the validity bitmaps with the intersection of the nullity
// of the arguments. If a preallocated bitmap is not provided, then one
// will be allocated if needed (in some cases a bitmap can be zero-copied
// from the arguments). If any Scalar value is null, then the entire
// validity bitmap will be set to null.
func propagateNulls(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ArraySpan) (err error) {
	if out.Type.ID() == arrow.NULL {
		// null output type is a no-op (rare but it happens)
		return
	}

	// this function is ONLY able to write into output with non-zero offset
	// when the bitmap is preallocated.
	if out.Offset != 0 && out.Buffers[0].Buf == nil {
		return fmt.Errorf("%w: can only propagate nulls into pre-allocated memory when output offset is non-zero", arrow.ErrInvalid)
	}

	var (
		arrsWithNulls = make([]*exec.ArraySpan, 0)
		isAllNull     bool
		prealloc      bool = out.Buffers[0].Buf != nil
	)

	for i := range batch.Values {
		v := &batch.Values[i]
		nullGen := getNullGen(v)
		if nullGen == nullGenAllNull {
			isAllNull = true
		}
		if nullGen != nullGenAllValid && v.IsArray() {
			arrsWithNulls = append(arrsWithNulls, &v.Array)
		}
	}

	outBitmap := out.Buffers[0].Buf
	if isAllNull {
		// an all-null value gives us a short circuit opportunity
		// output should all be null
		out.Nulls = out.Len
		if prealloc {
			bitutil.SetBitsTo(outBitmap, out.Offset, out.Len, false)
			return
		}

		// walk all the values with nulls instead of breaking on the first
		// in case we find a bitmap that can be reused in the non-preallocated case
		for _, arr := range arrsWithNulls {
			if arr.Nulls == arr.Len && arr.Buffers[0].Owner != nil {
				buf := arr.GetBuffer(0)
				buf.Retain()
				out.Buffers[0].Buf = buf.Bytes()
				out.Buffers[0].Owner = buf
				return
			}
		}

		buf := ctx.AllocateBitmap(int64(out.Len))
		out.Buffers[0].Owner = buf
		out.Buffers[0].Buf = buf.Bytes()
		out.Buffers[0].SelfAlloc = true
		bitutil.SetBitsTo(out.Buffers[0].Buf, out.Offset, out.Len, false)
		return
	}

	out.Nulls = array.UnknownNullCount
	switch len(arrsWithNulls) {
	case 0:
		out.Nulls = 0
		if prealloc {
			bitutil.SetBitsTo(outBitmap, out.Offset, out.Len, true)
		}
	case 1:
		arr := arrsWithNulls[0]
		out.Nulls = arr.Nulls
		if prealloc {
			bitutil.CopyBitmap(arr.Buffers[0].Buf, int(arr.Offset), int(arr.Len), outBitmap, int(out.Offset))
			return
		}

		switch {
		case arr.Offset == 0:
			out.Buffers[0] = arr.Buffers[0]
			out.Buffers[0].Owner.Retain()
		case arr.Offset%8 == 0:
			buf := memory.SliceBuffer(arr.GetBuffer(0), int(arr.Offset)/8, int(bitutil.BytesForBits(arr.Len)))
			out.Buffers[0].Buf = buf.Bytes()
			out.Buffers[0].Owner = buf
		default:
			buf := ctx.AllocateBitmap(int64(out.Len))
			out.Buffers[0].Owner = buf
			out.Buffers[0].Buf = buf.Bytes()
			out.Buffers[0].SelfAlloc = true
			bitutil.CopyBitmap(arr.Buffers[0].Buf, int(arr.Offset), int(arr.Len), out.Buffers[0].Buf, 0)
		}
		return

	default:
		if !prealloc {
			buf := ctx.AllocateBitmap(int64(out.Len))
			out.Buffers[0].Owner = buf
			out.Buffers[0].Buf = buf.Bytes()
			out.Buffers[0].SelfAlloc = true
			outBitmap = out.Buffers[0].Buf
		}

		acc := func(left, right *exec.ArraySpan) {
			debug.Assert(left.Buffers[0].Buf != nil, "invalid intersection for null propagation")
			debug.Assert(right.Buffers[0].Buf != nil, "invalid intersection for null propagation")
			bitutil.BitmapAnd(left.Buffers[0].Buf, right.Buffers[0].Buf, left.Offset, right.Offset, outBitmap, out.Offset, out.Len)
		}

		acc(arrsWithNulls[0], arrsWithNulls[1])
		for _, arr := range arrsWithNulls[2:] {
			acc(out, arr)
		}
	}
	return
}

func inferBatchLength(values []Datum) (length int64, allSame bool) {
	length, allSame = -1, true
	areAllScalar := true
	for _, arg := range values {
		switch arg := arg.(type) {
		case *ArrayDatum:
			argLength := arg.Len()
			if length < 0 {
				length = argLength
			} else {
				if length != argLength {
					allSame = false
					return
				}
			}
			areAllScalar = false
		case *ChunkedDatum:
			argLength := arg.Len()
			if length < 0 {
				length = argLength
			} else {
				if length != argLength {
					allSame = false
					return
				}
			}
			areAllScalar = false
		}
	}

	if areAllScalar && len(values) > 0 {
		length = 1
	} else if length < 0 {
		length = 0
	}
	allSame = true
	return
}

// kernelExecutor is the interface for all executors to initialize and
// call kernel execution functions on batches.
type kernelExecutor interface {
	// Init must be called *after* the kernel's init method and any
	// KernelState must be set into the KernelCtx *before* calling
	// this Init method. This is to faciliate the case where
	// Init may be expensive and does not need to be called
	// again for each execution of the kernel. For example,
	// the same lookup table can be re-used for all scanned batches
	// in a dataset filter.
	Init(*exec.KernelCtx, exec.KernelInitArgs) error
	// Execute the kernel for the provided batch and pass the resulting
	// Datum values to the provided channel.
	Execute(context.Context, *ExecBatch, chan<- Datum) error
	// WrapResults exists for the case where an executor wants to post process
	// the batches of result datums. Such as creating a ChunkedArray from
	// multiple output batches or so on. Results from individual batch
	// executions should be read from the out channel, and WrapResults should
	// return the final Datum result.
	WrapResults(ctx context.Context, out <-chan Datum, chunkedArgs bool) Datum
	// CheckResultType checks the actual result type against the resolved
	// output type. If the types don't match an error is returned
	CheckResultType(out Datum) error

	clear()
}

// the base implementation for executing non-aggregate kernels.
type nonAggExecImpl struct {
	ctx              *exec.KernelCtx
	ectx             ExecCtx
	kernel           exec.NonAggKernel
	outType          arrow.DataType
	numOutBuf        int
	dataPrealloc     []bufferPrealloc
	preallocValidity bool
}

func (e *nonAggExecImpl) clear() {
	e.ctx, e.kernel, e.outType = nil, nil, nil
	if e.dataPrealloc != nil {
		e.dataPrealloc = e.dataPrealloc[:0]
	}
}

func (e *nonAggExecImpl) Init(ctx *exec.KernelCtx, args exec.KernelInitArgs) (err error) {
	e.ctx, e.kernel = ctx, args.Kernel.(exec.NonAggKernel)
	e.outType, err = e.kernel.GetSig().OutType.Resolve(ctx, args.Inputs)
	e.ectx = GetExecCtx(ctx.Ctx)
	return
}

func (e *nonAggExecImpl) prepareOutput(length int) *exec.ExecResult {
	var nullCount int = array.UnknownNullCount

	if e.kernel.GetNullHandling() == exec.NullNoOutput {
		nullCount = 0
	}

	output := &exec.ArraySpan{
		Type:  e.outType,
		Len:   int64(length),
		Nulls: int64(nullCount),
	}

	if e.preallocValidity {
		buf := e.ctx.AllocateBitmap(int64(length))
		output.Buffers[0].Owner = buf
		output.Buffers[0].Buf = buf.Bytes()
		output.Buffers[0].SelfAlloc = true
	}

	for i, pre := range e.dataPrealloc {
		if pre.bitWidth >= 0 {
			buf := allocateDataBuffer(e.ctx, length+pre.addLen, pre.bitWidth)
			output.Buffers[i+1].Owner = buf
			output.Buffers[i+1].Buf = buf.Bytes()
			output.Buffers[i+1].SelfAlloc = true
		}
	}

	return output
}

func (e *nonAggExecImpl) CheckResultType(out Datum) error {
	typ := out.(ArrayLikeDatum).Type()
	if typ != nil && !arrow.TypeEqual(e.outType, typ) {
		return fmt.Errorf("%w: kernel type result mismatch: declared as %s, actual is %s",
			arrow.ErrType, e.outType, typ)
	}
	return nil
}

type spanIterator func() (exec.ExecSpan, int64, bool)

type scalarExecutor struct {
	nonAggExecImpl

	elideValidityBitmap bool
	preallocAllBufs     bool
	preallocContiguous  bool
	allScalars          bool
	iter                spanIterator
	iterLen             int64
}

func (s *scalarExecutor) Execute(ctx context.Context, batch *ExecBatch, data chan<- Datum) (err error) {
	s.allScalars, s.iter, err = iterateExecSpans(batch, s.ectx.ChunkSize, true)
	if err != nil {
		return
	}

	s.iterLen = batch.Len

	if batch.Len == 0 {
		result := array.MakeArrayOfNull(exec.GetAllocator(s.ctx.Ctx), s.outType, 0)
		defer result.Release()
		out := &exec.ArraySpan{}
		out.SetMembers(result.Data())
		return s.emitResult(out, data)
	}

	if err = s.setupPrealloc(batch.Len, batch.Values); err != nil {
		return
	}

	return s.executeSpans(data)
}

func (s *scalarExecutor) WrapResults(ctx context.Context, out <-chan Datum, hasChunked bool) Datum {
	var (
		output Datum
		acc    []arrow.Array
	)

	toChunked := func() {
		acc = output.(ArrayLikeDatum).Chunks()
		output.Release()
		output = nil
	}

	// get first output
	select {
	case <-ctx.Done():
		return nil
	case output = <-out:
		// if the inputs contained at least one chunked array
		// then we want to return chunked output
		if hasChunked {
			toChunked()
		}
	}

	for {
		select {
		case <-ctx.Done():
			// context is done, either cancelled or a timeout.
			// either way, we end early and return what we've got so far.
			return output
		case o, ok := <-out:
			if !ok { // channel closed, wrap it up
				if output != nil {
					return output
				}

				for _, c := range acc {
					defer c.Release()
				}

				chkd := arrow.NewChunked(s.outType, acc)
				defer chkd.Release()
				return NewDatum(chkd)
			}

			// if we get multiple batches of output, then we need
			// to return it as a chunked array.
			if acc == nil {
				toChunked()
			}

			defer o.Release()
			if o.Len() == 0 { // skip any empty batches
				continue
			}

			acc = append(acc, o.(*ArrayDatum).MakeArray())
		}
	}
}

func (s *scalarExecutor) executeSpans(data chan<- Datum) (err error) {
	var (
		input  exec.ExecSpan
		output exec.ExecResult
		next   bool
	)

	if s.preallocContiguous {
		// make one big output alloc
		prealloc := s.prepareOutput(int(s.iterLen))
		output = *prealloc

		output.Offset = 0
		var resultOffset int64
		var nextOffset int64
		for err == nil {
			if input, nextOffset, next = s.iter(); !next {
				break
			}
			output.SetSlice(resultOffset, input.Len)
			err = s.executeSingleSpan(&input, &output)
			resultOffset = nextOffset
		}
		if err != nil {
			return
		}

		return s.emitResult(prealloc, data)
	}

	// fully preallocating, but not contiguously
	// we (maybe) preallocate only for the output of processing
	// the current chunk
	for err == nil {
		if input, _, next = s.iter(); !next {
			break
		}

		output = *s.prepareOutput(int(input.Len))
		if err = s.executeSingleSpan(&input, &output); err != nil {
			return
		}
		err = s.emitResult(&output, data)
	}

	return
}

func (s *scalarExecutor) executeSingleSpan(input *exec.ExecSpan, out *exec.ExecResult) error {
	switch {
	case out.Type.ID() == arrow.NULL:
		out.Nulls = out.Len
	case s.kernel.GetNullHandling() == exec.NullIntersection:
		if !s.elideValidityBitmap {
			propagateNulls(s.ctx, input, out)
		}
	case s.kernel.GetNullHandling() == exec.NullNoOutput:
		out.Nulls = 0
	}
	return s.kernel.Exec(s.ctx, input, out)
}

func (s *scalarExecutor) setupPrealloc(totalLen int64, args []Datum) error {
	s.numOutBuf = len(s.outType.Layout().Buffers)
	outTypeID := s.outType.ID()
	// default to no validity pre-allocation for the following cases:
	// - Output Array is NullArray
	// - kernel.NullHandling is ComputeNoPrealloc or OutputNotNull
	s.preallocValidity = false

	if outTypeID != arrow.NULL {
		switch s.kernel.GetNullHandling() {
		case exec.NullComputedPrealloc:
			s.preallocValidity = true
		case exec.NullIntersection:
			s.elideValidityBitmap = true
			for _, a := range args {
				nullGen := getNullGenDatum(a) == nullGenAllValid
				s.elideValidityBitmap = s.elideValidityBitmap && nullGen
			}
			s.preallocValidity = !s.elideValidityBitmap
		case exec.NullNoOutput:
			s.elideValidityBitmap = true
		}
	}

	if s.kernel.GetMemAlloc() == exec.MemPrealloc {
		s.dataPrealloc = addComputeDataPrealloc(s.outType, s.dataPrealloc)
	}

	// validity bitmap either preallocated or elided, and all data buffers allocated
	// this is basically only true for primitive types that are not dict-encoded
	s.preallocAllBufs =
		((s.preallocValidity || s.elideValidityBitmap) && len(s.dataPrealloc) == (s.numOutBuf-1) &&
			!arrow.IsNested(outTypeID) && outTypeID != arrow.DICTIONARY)

	// contiguous prealloc only possible on non-nested types if all
	// buffers are preallocated. otherwise we have to go chunk by chunk
	//
	// some kernels are also unable to write into sliced outputs, so
	// we respect the kernel's attributes
	s.preallocContiguous =
		(s.ectx.PreallocContiguous && s.kernel.CanFillSlices() &&
			s.preallocAllBufs)

	return nil
}

func (s *scalarExecutor) emitResult(resultData *exec.ArraySpan, data chan<- Datum) error {
	var output Datum
	if s.allScalars {
		// we boxed scalar inputs as ArraySpan so now we have to unbox the output
		arr := resultData.MakeArray()
		defer arr.Release()
		sc, err := scalar.GetScalar(arr, 0)
		if err != nil {
			return err
		}
		output = NewDatum(sc)
	} else {
		d := resultData.MakeData()
		defer d.Release()
		output = NewDatum(d)
	}
	data <- output
	return nil
}

func checkAllIsValue(vals []Datum) error {
	for _, v := range vals {
		if !DatumIsValue(v) {
			return fmt.Errorf("%w: tried executing function with non-value type: %s",
				arrow.ErrInvalid, v)
		}
	}
	return nil
}

func checkIfAllScalar(batch *ExecBatch) bool {
	for _, v := range batch.Values {
		if v.Kind() != KindScalar {
			return false
		}
	}
	return batch.NumValues() > 0
}

// iterateExecSpans sets up and returns a function which can iterate a batch
// according to the chunk sizes. If the inputs contain chunked arrays, then
// we will find the min(chunk sizes, maxChunkSize) to ensure we return
// contiguous spans to execute on.
//
// the iteration function returns the next span to execute on, the current
// position in the full batch, and a boolean indicating whether or not
// a span was actually returned (there is data to process).
func iterateExecSpans(batch *ExecBatch, maxChunkSize int64, promoteIfAllScalar bool) (haveAllScalars bool, itr spanIterator, err error) {
	if batch.NumValues() > 0 {
		inferred, allArgsSame := inferBatchLength(batch.Values)
		if inferred != batch.Len {
			return false, nil, fmt.Errorf("%w: value lengths differed from execbatch length", arrow.ErrInvalid)
		}
		if !allArgsSame {
			return false, nil, fmt.Errorf("%w: array args must all be the same length", arrow.ErrInvalid)
		}
	}

	var (
		args           []Datum = batch.Values
		haveChunked    bool
		chunkIdxes           = make([]int, len(args))
		valuePositions       = make([]int64, len(args))
		valueOffsets         = make([]int64, len(args))
		pos, length    int64 = 0, batch.Len
	)
	haveAllScalars = checkIfAllScalar(batch)
	maxChunkSize = exec.Min(length, maxChunkSize)

	span := exec.ExecSpan{Values: make([]exec.ExecValue, len(args)), Len: 0}
	for i, a := range args {
		switch arg := a.(type) {
		case *ScalarDatum:
			span.Values[i].Scalar = arg.Value
		case *ArrayDatum:
			span.Values[i].Array.SetMembers(arg.Value)
			valueOffsets[i] = int64(arg.Value.Offset())
		case *ChunkedDatum:
			// populate from first chunk
			carr := arg.Value
			if len(carr.Chunks()) > 0 {
				arr := carr.Chunk(0).Data()
				span.Values[i].Array.SetMembers(arr)
				valueOffsets[i] = int64(arr.Offset())
			} else {
				// fill as zero len
				exec.FillZeroLength(carr.DataType(), &span.Values[i].Array)
			}
			haveChunked = true
		}
	}

	if haveAllScalars && promoteIfAllScalar {
		exec.PromoteExecSpanScalars(span)
	}

	nextChunkSpan := func(iterSz int64, span exec.ExecSpan) int64 {
		for i := 0; i < len(args) && iterSz > 0; i++ {
			// if the argument is not chunked, it's either a scalar or an array
			// in which case it doesn't influence the size of the span
			chunkedArg, ok := args[i].(*ChunkedDatum)
			if !ok {
				continue
			}

			arg := chunkedArg.Value
			if len(arg.Chunks()) == 0 {
				iterSz = 0
				continue
			}

			var curChunk arrow.Array
			for {
				curChunk = arg.Chunk(chunkIdxes[i])
				if valuePositions[i] == int64(curChunk.Len()) {
					// chunk is zero-length, or was exhausted in the previous
					// iteration, move to next chunk
					chunkIdxes[i]++
					curChunk = arg.Chunk(chunkIdxes[i])
					span.Values[i].Array.SetMembers(curChunk.Data())
					valuePositions[i] = 0
					valueOffsets[i] = int64(curChunk.Data().Offset())
					continue
				}
				break
			}
			iterSz = exec.Min(int64(curChunk.Len())-valuePositions[i], iterSz)
		}
		return iterSz
	}

	return haveAllScalars, func() (exec.ExecSpan, int64, bool) {
		if pos == length {
			return exec.ExecSpan{}, pos, false
		}

		iterationSize := exec.Min(length-pos, maxChunkSize)
		if haveChunked {
			iterationSize = nextChunkSpan(iterationSize, span)
		}

		span.Len = iterationSize
		for i, a := range args {
			if a.Kind() != KindScalar {
				span.Values[i].Array.SetSlice(valuePositions[i]+valueOffsets[i], iterationSize)
				valuePositions[i] += iterationSize
			}
		}

		pos += iterationSize
		debug.Assert(pos <= length, "bad state for iteration exec span")
		return span, pos, true
	}, nil
}

var (
	// have a pool of scalar executors to avoid excessive object creation
	scalarExecPool = sync.Pool{
		New: func() any { return &scalarExecutor{} },
	}
)
