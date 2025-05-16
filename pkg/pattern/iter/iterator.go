package iter

import (
	iter "github.com/grafana/loki/v3/pkg/iter/v2"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/prometheus/prometheus/model/labels"
)

type Iterator interface {
	iter.CloseIterator[logproto.PatternSample]

	Pattern() string
}

type StreamPatterns interface {
	Iterator

	Labels() labels.Labels
}

func NewSlice(pattern string, s []logproto.PatternSample) *PatternIter {
	return &PatternIter{
		CloseIterator: iter.WithClose(iter.NewSliceIter(s), nil),
		pattern:       pattern,
	}
}

func NewEmpty(pattern string) *PatternIter {
	return &PatternIter{
		CloseIterator: iter.WithClose(iter.NewEmptyIter[logproto.PatternSample](), nil),
		pattern:       pattern,
	}
}

type PatternIter struct {
	iter.CloseIterator[logproto.PatternSample]
	pattern string
}

func (s *PatternIter) Pattern() string {
	return s.pattern
}

type nonOverlappingPatternIterator struct {
	iterators []Iterator
	curr      Iterator
	pattern   string
}

// NewNonOverlappingIterator gives a chained iterator over a list of iterators.
func NewNonOverlappingIterator(pattern string, iterators []Iterator) Iterator {
	return &nonOverlappingPatternIterator{
		iterators: iterators,
		pattern:   pattern,
	}
}

func (i *nonOverlappingPatternIterator) Next() bool {
	for i.curr == nil || !i.curr.Next() {
		if len(i.iterators) == 0 {
			if i.curr != nil {
				i.curr.Close()
			}
			return false
		}
		if i.curr != nil {
			i.curr.Close()
		}
		i.curr, i.iterators = i.iterators[0], i.iterators[1:]
	}

	return true
}

func (i *nonOverlappingPatternIterator) At() logproto.PatternSample {
	return i.curr.At()
}

func (i *nonOverlappingPatternIterator) Pattern() string {
	return i.pattern
}

func (i *nonOverlappingPatternIterator) Err() error {
	if i.curr != nil {
		return i.curr.Err()
	}
	return nil
}

func (i *nonOverlappingPatternIterator) Close() error {
	if i.curr != nil {
		i.curr.Close()
	}
	for _, iter := range i.iterators {
		iter.Close()
	}
	i.iterators = nil
	return nil
}

type streamPatternsIterator struct {
	Iterator
	labels labels.Labels
}

// NewStreamPatternsIterator gives a chained iterator over a list of iterators.
func NewStreamPatternsIterator(lbls labels.Labels, it Iterator) StreamPatterns {
	return &streamPatternsIterator{
		Iterator: it,
		labels:   lbls,
	}
}

func (i *streamPatternsIterator) Labels() labels.Labels {
	return i.labels
}
