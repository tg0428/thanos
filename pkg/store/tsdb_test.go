// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestTSDBStore_Info(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := e2eutil.NewTSDB()
	defer func() { testutil.Ok(t, db.Close()) }()
	testutil.Ok(t, err)

	tsdbStore := NewTSDBStore(nil, nil, db, component.Rule, labels.FromStrings("region", "eu-west"))

	resp, err := tsdbStore.Info(ctx, &storepb.InfoRequest{})
	testutil.Ok(t, err)

	testutil.Equals(t, []storepb.Label{{Name: "region", Value: "eu-west"}}, resp.Labels)
	testutil.Equals(t, storepb.StoreType_RULE, resp.StoreType)
	testutil.Equals(t, int64(math.MaxInt64), resp.MinTime)
	testutil.Equals(t, int64(math.MaxInt64), resp.MaxTime)

	app := db.Appender(context.Background())
	_, err = app.Add(labels.FromStrings("a", "a"), 12, 0.1)
	testutil.Ok(t, err)
	testutil.Ok(t, app.Commit())

	resp, err = tsdbStore.Info(ctx, &storepb.InfoRequest{})
	testutil.Ok(t, err)

	testutil.Equals(t, []storepb.Label{{Name: "region", Value: "eu-west"}}, resp.Labels)
	testutil.Equals(t, storepb.StoreType_RULE, resp.StoreType)
	testutil.Equals(t, int64(12), resp.MinTime)
	testutil.Equals(t, int64(math.MaxInt64), resp.MaxTime)
}

func TestTSDBStore_Series(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := e2eutil.NewTSDB()
	defer func() { testutil.Ok(t, db.Close()) }()
	testutil.Ok(t, err)

	tsdbStore := NewTSDBStore(nil, nil, db, component.Rule, labels.FromStrings("region", "eu-west"))

	appender := db.Appender(context.Background())

	for i := 1; i <= 3; i++ {
		_, err = appender.Add(labels.FromStrings("a", "1"), int64(i), float64(i))
		testutil.Ok(t, err)
	}
	err = appender.Commit()
	testutil.Ok(t, err)

	for _, tc := range []struct {
		title          string
		req            *storepb.SeriesRequest
		expectedSeries []rawSeries
		expectedError  string
	}{
		{
			title: "total match series",
			req: &storepb.SeriesRequest{
				MinTime: 1,
				MaxTime: 3,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
				},
			},
			expectedSeries: []rawSeries{
				{
					lset:   []storepb.Label{{Name: "a", Value: "1"}, {Name: "region", Value: "eu-west"}},
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
			},
		},
		{
			title: "partially match time range series",
			req: &storepb.SeriesRequest{
				MinTime: 1,
				MaxTime: 2,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
				},
			},
			expectedSeries: []rawSeries{
				{
					lset:   []storepb.Label{{Name: "a", Value: "1"}, {Name: "region", Value: "eu-west"}},
					chunks: [][]sample{{{1, 1}, {2, 2}}},
				},
			},
		},
		{
			title: "dont't match time range series",
			req: &storepb.SeriesRequest{
				MinTime: 4,
				MaxTime: 6,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
				},
			},
			expectedSeries: []rawSeries{},
		},
		{
			title: "only match external label",
			req: &storepb.SeriesRequest{
				MinTime: 1,
				MaxTime: 3,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "eu-west"},
				},
			},
			expectedError: "rpc error: code = InvalidArgument desc = no matchers specified (excluding external labels)",
		},
		{
			title: "dont't match labels",
			req: &storepb.SeriesRequest{
				MinTime: 1,
				MaxTime: 3,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "b", Value: "1"},
				},
			},
			expectedSeries: []rawSeries{},
		},
		{
			title: "no chunk",
			req: &storepb.SeriesRequest{
				MinTime: 1,
				MaxTime: 3,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
				},
				SkipChunks: true,
			},
			expectedSeries: []rawSeries{
				{
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "region", Value: "eu-west"}},
				},
			},
		},
	} {
		if ok := t.Run(tc.title, func(t *testing.T) {
			srv := newStoreSeriesServer(ctx)
			err := tsdbStore.Series(tc.req, srv)
			if len(tc.expectedError) > 0 {
				testutil.NotOk(t, err)
				testutil.Equals(t, tc.expectedError, err.Error())
			} else {
				testutil.Ok(t, err)
				seriesEquals(t, tc.expectedSeries, srv.SeriesSet)
			}
		}); !ok {
			return
		}
	}
}

func TestTSDBStore_LabelNames(t *testing.T) {
	var err error
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := e2eutil.NewTSDB()
	defer func() { testutil.Ok(t, db.Close()) }()
	testutil.Ok(t, err)

	appender := db.Appender(context.Background())
	addLabels := func(lbs []string, timestamp int64) {
		if len(lbs) > 0 {
			_, err = appender.Add(labels.FromStrings(lbs...), timestamp, 1)
			testutil.Ok(t, err)
		}
	}

	tsdbStore := NewTSDBStore(nil, nil, db, component.Rule, labels.FromStrings("region", "eu-west"))

	now := time.Now()
	head := db.Head()
	for _, tc := range []struct {
		title         string
		labels        []string
		expectedNames []string
		timestamp     int64
		start         func() int64
		end           func() int64
	}{
		{
			title:     "no label in tsdb",
			labels:    []string{},
			timestamp: now.Unix(),
			start: func() int64 {
				return timestamp.FromTime(minTime)
			},
			end: func() int64 {
				return timestamp.FromTime(maxTime)
			},
		},
		{
			title:         "add one label",
			labels:        []string{"foo", "foo"},
			expectedNames: []string{"foo"},
			timestamp:     now.Unix(),
			start: func() int64 {
				return timestamp.FromTime(minTime)
			},
			end: func() int64 {
				return timestamp.FromTime(maxTime)
			},
		},
		{
			title:  "add another label",
			labels: []string{"bar", "bar"},
			// We will get two labels here.
			expectedNames: []string{"bar", "foo"},
			timestamp:     now.Unix(),
			start: func() int64 {
				return timestamp.FromTime(minTime)
			},
			end: func() int64 {
				return timestamp.FromTime(maxTime)
			},
		},
		{
			title:     "query range outside tsdb head",
			labels:    []string{},
			timestamp: now.Unix(),
			start: func() int64 {
				return timestamp.FromTime(minTime)
			},
			end: func() int64 {
				return head.MinTime() - 1
			},
		},
		{
			title:         "get all labels",
			labels:        []string{"buz", "buz"},
			expectedNames: []string{"bar", "buz", "foo"},
			timestamp:     now.Unix(),
			start: func() int64 {
				return timestamp.FromTime(minTime)
			},
			end: func() int64 {
				return timestamp.FromTime(maxTime)
			},
		},
	} {
		if ok := t.Run(tc.title, func(t *testing.T) {
			addLabels(tc.labels, tc.timestamp)
			resp, err := tsdbStore.LabelNames(ctx, &storepb.LabelNamesRequest{
				Start: tc.start(),
				End:   tc.end(),
			})
			testutil.Ok(t, err)
			testutil.Equals(t, tc.expectedNames, resp.Names)
			testutil.Equals(t, 0, len(resp.Warnings))
		}); !ok {
			return
		}
	}
}

func TestTSDBStore_LabelValues(t *testing.T) {
	var err error
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := e2eutil.NewTSDB()
	defer func() { testutil.Ok(t, db.Close()) }()
	testutil.Ok(t, err)

	appender := db.Appender(context.Background())
	addLabels := func(lbs []string, timestamp int64) {
		if len(lbs) > 0 {
			_, err = appender.Add(labels.FromStrings(lbs...), timestamp, 1)
			testutil.Ok(t, err)
		}
	}

	tsdbStore := NewTSDBStore(nil, nil, db, component.Rule, labels.FromStrings("region", "eu-west"))
	now := time.Now()
	head := db.Head()
	for _, tc := range []struct {
		title          string
		addedLabels    []string
		queryLabel     string
		expectedValues []string
		timestamp      int64
		start          func() int64
		end            func() int64
	}{
		{
			title:       "no label in tsdb",
			addedLabels: []string{},
			queryLabel:  "foo",
			timestamp:   now.Unix(),
			start: func() int64 {
				return timestamp.FromTime(minTime)
			},
			end: func() int64 {
				return timestamp.FromTime(maxTime)
			},
		},
		{
			title:          "add one label value",
			addedLabels:    []string{"foo", "test"},
			queryLabel:     "foo",
			expectedValues: []string{"test"},
			timestamp:      now.Unix(),
			start: func() int64 {
				return timestamp.FromTime(minTime)
			},
			end: func() int64 {
				return timestamp.FromTime(maxTime)
			},
		},
		{
			title:          "add another label value",
			addedLabels:    []string{"foo", "test1"},
			queryLabel:     "foo",
			expectedValues: []string{"test", "test1"},
			timestamp:      now.Unix(),
			start: func() int64 {
				return timestamp.FromTime(minTime)
			},
			end: func() int64 {
				return timestamp.FromTime(maxTime)
			},
		},
		{
			title:       "query time range outside head",
			addedLabels: []string{},
			queryLabel:  "foo",
			timestamp:   now.Unix(),
			start: func() int64 {
				return timestamp.FromTime(minTime)
			},
			end: func() int64 {
				return head.MinTime() - 1
			},
		},
	} {
		if ok := t.Run(tc.title, func(t *testing.T) {
			addLabels(tc.addedLabels, tc.timestamp)
			resp, err := tsdbStore.LabelValues(ctx, &storepb.LabelValuesRequest{
				Label: tc.queryLabel,
				Start: tc.start(),
				End:   tc.end(),
			})
			testutil.Ok(t, err)
			testutil.Equals(t, tc.expectedValues, resp.Values)
			testutil.Equals(t, 0, len(resp.Warnings))
		}); !ok {
			return
		}
	}
}

// Regression test for https://github.com/thanos-io/thanos/issues/1038.
func TestTSDBStore_Series_SplitSamplesIntoChunksWithMaxSizeOf120(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	db, err := e2eutil.NewTSDB()
	defer func() { testutil.Ok(t, db.Close()) }()
	testutil.Ok(t, err)

	testSeries_SplitSamplesIntoChunksWithMaxSizeOf120(t, db.Appender(context.Background()), func() storepb.StoreServer {
		tsdbStore := NewTSDBStore(nil, nil, db, component.Rule, labels.FromStrings("region", "eu-west"))

		return tsdbStore
	})
}

// Regression test for: https://github.com/thanos-io/thanos/issues/3013 .
func TestTSDBStore_SeriesChunkBytesCopied(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)
	t.Cleanup(func() {
		testutil.Ok(t, os.RemoveAll(tmpDir))
	})

	var (
		random = rand.New(rand.NewSource(120))
		logger = log.NewNopLogger()
	)

	// Generate one series in two parts. Put first part in block, second in just WAL.
	head, _ := storetestutil.CreateHeadWithSeries(t, 0, storetestutil.HeadGenOptions{
		TSDBDir:          tmpDir,
		SamplesPerSeries: 300,
		Series:           2,
		Random:           random,
		SkipChunks:       true,
	})
	_ = createBlockFromHead(t, tmpDir, head)
	testutil.Ok(t, head.Close())

	head, _ = storetestutil.CreateHeadWithSeries(t, 1, storetestutil.HeadGenOptions{
		TSDBDir:          tmpDir,
		SamplesPerSeries: 300,
		Series:           2,
		WithWAL:          true,
		Random:           random,
		SkipChunks:       true,
	})
	testutil.Ok(t, head.Close())

	db, err := tsdb.OpenDBReadOnly(tmpDir, logger)
	testutil.Ok(t, err)

	extLabels := labels.FromStrings("ext", "1")
	store := NewTSDBStore(logger, nil, &mockedStartTimeDB{DBReadOnly: db, startTime: 0}, component.Receive, extLabels)

	t.Cleanup(func() {
		if db != nil {
			testutil.Ok(t, db.Close())
		}
	})

	// Call series.
	srv := storetestutil.NewSeriesServer(context.Background())
	t.Run("call series and access results", func(t *testing.T) {
		testutil.Ok(t, store.Series(&storepb.SeriesRequest{
			MinTime: 0,
			MaxTime: math.MaxInt64,
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
			},
			PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		}, srv))
		testutil.Equals(t, 0, len(srv.Warnings))
		testutil.Equals(t, 0, len(srv.HintsSet))
		testutil.Equals(t, 4, len(srv.SeriesSet))

		// All chunks should be accessible for read and write (copied).
		for _, s := range srv.SeriesSet {
			testutil.Equals(t, 3, len(s.Chunks))
			for _, c := range s.Chunks {
				testutil.Ok(t, testutil.FaultOrPanicToErr(func() {
					_ = string(c.Raw.Data) // Access bytes by converting them to different type.
				}))
				testutil.Ok(t, testutil.FaultOrPanicToErr(func() {
					c.Raw.Data[0] = 0 // Check if we can write to the byte range.
				}))
			}
		}
	})
	t.Run("flush WAL and access results", func(t *testing.T) {
		testutil.Ok(t, db.FlushWAL(tmpDir))

		// All chunks should be still accessible for read and write (copied).
		for _, s := range srv.SeriesSet {
			for _, c := range s.Chunks {
				testutil.Ok(t, testutil.FaultOrPanicToErr(func() {
					_ = string(c.Raw.Data) // Access bytes by converting them to different type.
				}))
				testutil.Ok(t, testutil.FaultOrPanicToErr(func() {
					c.Raw.Data[0] = 0 // Check if we can write to the byte range.
				}))
			}
		}
	})

	t.Run("close db with block readers and access results", func(t *testing.T) {
		// This should not block, as select finished.
		testutil.Ok(t, db.Close())
		db = nil

		// All chunks should be still accessible for read and write (copied).
		for _, s := range srv.SeriesSet {
			for _, c := range s.Chunks {
				testutil.Ok(t, testutil.FaultOrPanicToErr(func() {
					_ = string(c.Raw.Data) // Access bytes by converting them to different type.
				}))
				testutil.Ok(t, testutil.FaultOrPanicToErr(func() {
					c.Raw.Data[0] = 0 // Check if we can write to the byte range.
				}))
			}
		}
	})
}

func TestTSDBStoreSeries(t *testing.T) {
	tb := testutil.NewTB(t)
	storetestutil.RunSeriesInterestingCases(tb, 10e6, 10e5, func(t testutil.TB, samplesPerSeries, series int) {
		benchTSDBStoreSeries(t, samplesPerSeries, series)
	})
}

func BenchmarkTSDBStoreSeries(b *testing.B) {
	tb := testutil.NewTB(b)
	storetestutil.RunSeriesInterestingCases(tb, 10e6, 10e5, func(t testutil.TB, samplesPerSeries, series int) {
		benchTSDBStoreSeries(t, samplesPerSeries, series)
	})
}

func benchTSDBStoreSeries(t testutil.TB, totalSamples, totalSeries int) {
	tmpDir, err := ioutil.TempDir("", "testorbench-testtsdbseries")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	// This means 3 blocks and the head.
	const numOfBlocks = 4

	samplesPerSeriesPerBlock := totalSamples / numOfBlocks
	if samplesPerSeriesPerBlock == 0 {
		samplesPerSeriesPerBlock = 1
	}
	seriesPerBlock := totalSeries / numOfBlocks
	if seriesPerBlock == 0 {
		seriesPerBlock = 1
	}

	var (
		resps  = make([][]*storepb.SeriesResponse, 4)
		random = rand.New(rand.NewSource(120))
		logger = log.NewNopLogger()
	)

	for j := 0; j < 3; j++ {
		head, created := storetestutil.CreateHeadWithSeries(t, j, storetestutil.HeadGenOptions{
			TSDBDir:          tmpDir,
			SamplesPerSeries: samplesPerSeriesPerBlock,
			Series:           seriesPerBlock,
			Random:           random,
			SkipChunks:       t.IsBenchmark(),
		})
		for i := 0; i < len(created); i++ {
			resps[j] = append(resps[j], storepb.NewSeriesResponse(created[i]))
		}

		_ = createBlockFromHead(t, tmpDir, head)
		testutil.Ok(t, head.Close())
	}

	head, created := storetestutil.CreateHeadWithSeries(t, 3, storetestutil.HeadGenOptions{
		TSDBDir:          tmpDir,
		SamplesPerSeries: samplesPerSeriesPerBlock,
		Series:           seriesPerBlock,
		WithWAL:          true,
		Random:           random,
		SkipChunks:       t.IsBenchmark(),
	})
	testutil.Ok(t, head.Close())

	for i := 0; i < len(created); i++ {
		resps[3] = append(resps[3], storepb.NewSeriesResponse(created[i]))
	}

	db, err := tsdb.OpenDBReadOnly(tmpDir, logger)
	testutil.Ok(t, err)

	defer func() { testutil.Ok(t, db.Close()) }()

	extLabels := labels.FromStrings("ext", "1")
	store := NewTSDBStore(logger, nil, &mockedStartTimeDB{DBReadOnly: db, startTime: 0}, component.Receive, extLabels)

	var expected []*storepb.Series
	var lastLabels storepb.Series
	var frameBytesLeft int
	for _, resp := range resps {
		for _, r := range resp {
			// TSDB Store splits into multiple frames based on maxBytesPerFrame option. Prepare for that.

			// Add external labels.
			x := storepb.Series{
				Labels: make([]storepb.Label, 0, len(r.GetSeries().Labels)+len(extLabels)),
			}
			for _, l := range r.GetSeries().Labels {
				x.Labels = append(x.Labels, storepb.Label{
					Name:  l.Name,
					Value: l.Value,
				})
			}
			for _, l := range extLabels {
				x.Labels = append(x.Labels, storepb.Label{
					Name:  l.Name,
					Value: l.Value,
				})
			}
			sort.Slice(x.Labels, func(i, j int) bool {
				return x.Labels[i].Name < x.Labels[j].Name
			})

			for _, c := range r.GetSeries().Chunks {
				if x.String() == lastLabels.String() && frameBytesLeft > 0 {
					expected[len(expected)-1].Chunks = append(expected[len(expected)-1].Chunks, c)
					frameBytesLeft -= c.Size()
					continue
				}

				frameBytesLeft = store.maxBytesPerFrame
				for _, lbl := range x.Labels {
					frameBytesLeft -= lbl.Size()
				}
				lastLabels = x
				expected = append(expected, &storepb.Series{Labels: x.Labels, Chunks: []storepb.AggrChunk{c}})
				frameBytesLeft -= c.Size()
			}
		}
	}

	storetestutil.TestServerSeries(t, store,
		&storetestutil.SeriesCase{
			Name: fmt.Sprintf("%d blocks and one WAL with %d samples, %d series each", numOfBlocks-1, samplesPerSeriesPerBlock, seriesPerBlock),
			Req: &storepb.SeriesRequest{
				MinTime: 0,
				MaxTime: math.MaxInt64,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				},
				PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
			},
			ExpectedSeries: expected,
		},
	)
}
