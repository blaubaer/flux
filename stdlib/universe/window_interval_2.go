package universe

import (
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/internal/errors"
	"github.com/influxdata/flux/interval"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/runtime"
	"github.com/influxdata/flux/values"
)

const WindowIntervalKind2 = "windowInterval2"
const WindowKind2 = "window2"
const DefaultStartColumn = "_start"
const DefaultStopColumn = "_stop"
const DefaultTimeColumn = "_time"

type WindowIntervalOpSpec2 struct {
	Every       flux.Duration `json:"every"`
	Period      flux.Duration `json:"period"`
	Offset      flux.Duration `json:"offset"`
	CreateEmpty bool          `json:"createEmpty"`
}

func init() {
	windowSignature := runtime.MustLookupBuiltinType("universe", "window2")
	runtime.RegisterPackageValue("universe", WindowKind2, flux.MustValue(flux.FunctionValue(WindowKind2, createWindowOpSpec, windowSignature)))
	execute.RegisterTransformation(WindowIntervalKind2, createWindowIntervalTransformation2)
}

func (s *WindowIntervalOpSpec2) Kind2() flux.OperationKind {
	return WindowIntervalKind2
}

type WindowIntervalProcedureSpec2 struct {
	plan.DefaultCost
	Window      plan.WindowSpec
	CreateEmpty bool
}

func (s *WindowIntervalProcedureSpec2) Kind2() plan.ProcedureKind {
	return WindowIntervalKind2
}

func (s *WindowIntervalProcedureSpec) Copy2() plan.ProcedureSpec {
	ns := new(WindowIntervalProcedureSpec)
	ns.Window = s.Window
	ns.CreateEmpty = s.CreateEmpty
	return ns
}

func createWindowIntervalTransformation2(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*WindowIntervalProcedureSpec)
	if !ok {
		return nil, nil, errors.Newf(codes.Internal, "invalid spec type %T", spec)
	}
	cache := execute.NewTableBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)

	bounds := a.StreamContext().Bounds()
	if bounds == nil {
		const docURL = "https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/window/#nil-bounds-passed-to-window"
		return nil, nil, errors.New(codes.Invalid, "nil bounds passed to window; use range to set the window range").
			WithDocURL(docURL)
	}

	newBounds := interval.NewBounds(bounds.Start, bounds.Stop)

	w, err := interval.NewWindow(
		s.Window.Every,
		s.Window.Period,
		s.Window.Offset,
	)

	if err != nil {
		return nil, nil, err
	}
	t := NewIntervalFixedWindowTransformation2(
		d,
		cache,
		newBounds,
		w,
		s.CreateEmpty,
	)
	return t, d, nil
}

type intervalFixedWindowTransformation2 struct {
	execute.ExecutionNode
	d         execute.Dataset
	cache     execute.TableBuilderCache
	w         interval.Window
	bounds    interval.Bounds
	allBounds []interval.Bounds

	createEmpty bool
}

func NewIntervalFixedWindowTransformation2(
	d execute.Dataset,
	cache execute.TableBuilderCache,
	bounds interval.Bounds,
	w interval.Window,
	createEmpty bool,
) execute.Transformation {
	t := &intervalFixedWindowTransformation2{
		d:           d,
		cache:       cache,
		w:           w,
		bounds:      bounds,
		createEmpty: createEmpty,
	}

	if createEmpty {
		t.generateWindowsWithinBounds()
	}

	return t
}

func (t *intervalFixedWindowTransformation2) RetractTable(id execute.DatasetID, key flux.GroupKey) (err error) {
	panic("not implemented")
}

func (t *intervalFixedWindowTransformation2) Process(id execute.DatasetID, tbl flux.Table) error {
	timeIdx := execute.ColIdx(DefaultTimeColumn, tbl.Cols())
	if timeIdx < 0 {
		const docURL = "https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/window/#missing-time-column"
		return errors.Newf(codes.FailedPrecondition, "missing time column %q", DefaultTimeColumn).
			WithDocURL(docURL)
	}

	newCols := make([]flux.ColMeta, 0, len(tbl.Cols())+2)
	keyCols := make([]flux.ColMeta, 0, len(tbl.Cols())+2)
	keyColMap := make([]int, 0, len(tbl.Cols())+2)
	startColIdx := -1
	stopColIdx := -1
	for j, c := range tbl.Cols() {
		keyIdx := execute.ColIdx(c.Label, tbl.Key().Cols())
		keyed := keyIdx >= 0
		if c.Label == DefaultStartColumn {
			startColIdx = j
			keyed = true
		}
		if c.Label == DefaultStopColumn {
			stopColIdx = j
			keyed = true
		}
		newCols = append(newCols, c)
		if keyed {
			keyCols = append(keyCols, c)
			keyColMap = append(keyColMap, keyIdx)
		}
	}
	if startColIdx == -1 {
		startColIdx = len(newCols)
		c := flux.ColMeta{
			Label: DefaultStartColumn,
			Type:  flux.TTime,
		}
		newCols = append(newCols, c)
		keyCols = append(keyCols, c)
		keyColMap = append(keyColMap, len(keyColMap))
	}
	if stopColIdx == -1 {
		stopColIdx = len(newCols)
		c := flux.ColMeta{
			Label: DefaultStopColumn,
			Type:  flux.TTime,
		}
		newCols = append(newCols, c)
		keyCols = append(keyCols, c)
		keyColMap = append(keyColMap, len(keyColMap))
	}

	// Abort processing if no data will match bounds
	if t.bounds.IsEmpty() {
		return nil
	}

	for _, bnds := range t.allBounds {
		key := t.newWindowGroupKey(tbl, keyCols, bnds, keyColMap)
		builder, created := t.cache.TableBuilder(key)
		if created {
			for _, c := range newCols {
				_, err := builder.AddCol(c)
				if err != nil {
					return err
				}
			}
		}
	}

	return tbl.Do(func(cr flux.ColReader) error {
		l := cr.Len()
		for i := 0; i < l; i++ {
			tm := values.Time(cr.Times(timeIdx).Value(i))
			bounds := t.getWindowBounds(tm)

			for _, bnds := range bounds {
				key := t.newWindowGroupKey(tbl, keyCols, bnds, keyColMap)
				builder, created := t.cache.TableBuilder(key)
				if created {
					for _, c := range newCols {
						_, err := builder.AddCol(c)
						if err != nil {
							return err
						}
					}
				}

				for j, c := range builder.Cols() {
					switch c.Label {
					case DefaultStartColumn:
						if err := builder.AppendTime(startColIdx, bnds.Start()); err != nil {
							return err
						}
					case DefaultStopColumn:
						if err := builder.AppendTime(stopColIdx, bnds.Stop()); err != nil {
							return err
						}
					default:
						if err := builder.AppendValue(j, execute.ValueForRow(cr, i, j)); err != nil {
							return err
						}
					}
				}
			}
		}
		return nil
	})
}

func (t *intervalFixedWindowTransformation2) newWindowGroupKey(tbl flux.Table, keyCols []flux.ColMeta, bnds interval.Bounds, keyColMap []int) flux.GroupKey {
	cols := make([]flux.ColMeta, len(keyCols))
	vs := make([]values.Value, len(keyCols))
	for j, c := range keyCols {
		cols[j] = c
		switch c.Label {
		case DefaultStartColumn:
			vs[j] = values.NewTime(bnds.Start())
		case DefaultStopColumn:
			vs[j] = values.NewTime(bnds.Stop())
		default:
			vs[j] = tbl.Key().Value(keyColMap[j])
		}
	}
	return execute.NewGroupKey(cols, vs)
}

func (t *intervalFixedWindowTransformation2) clipBounds(bs []interval.Bounds) {
	for i := range bs {
		bs[i] = t.bounds.Intersect(bs[i])
	}
}

func (t *intervalFixedWindowTransformation2) getWindowBounds(tm execute.Time) []interval.Bounds {
	if t.w.Every() == infinityVar.Duration() {
		return []interval.Bounds{t.bounds}
	}
	bs := t.w.GetOverlappingBounds(tm, tm+1)
	t.clipBounds(bs)
	return bs
}

func (t *intervalFixedWindowTransformation2) generateWindowsWithinBounds() {
	if t.w.Every() == infinityVar.Duration() {
		bounds := interval.NewBounds(interval.MinTime, interval.MaxTime)
		t.allBounds = []interval.Bounds{bounds}
		return
	}
	bs := t.w.GetOverlappingBounds(t.bounds.Start(), t.bounds.Stop())
	t.clipBounds(bs)
	t.allBounds = bs
}

func (t *intervalFixedWindowTransformation2) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *intervalFixedWindowTransformation2) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *intervalFixedWindowTransformation2) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}
