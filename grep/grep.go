package grep

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// Stop is a special value that can be returned by GrepFunc to indicate that
// search needs to be terminated early.
var Stop = errors.New("stop")

// Result describes one search match result.
type Result struct {
	// Text holds the match as a byte slice
	Text []byte
	// QuerySubmatch is a byte index pair identifying the query submatch in Text
	QuerySubmatch []int
	// QuerySubmatch is a byte index pair identifying the result submatch in Text
	ResultSubmatch []int
}

func (r *Result) String() string {
	return fmt.Sprintf("Result {Text: %q, QuerySubmatch:%v, ResultSubmatch:%v}", string(r.Text), r.QuerySubmatch, r.ResultSubmatch)
}

type Results []*Result

// GrepFunc is called for each found match and will be passed the path where
// match was found and a slice of match results.  A Special error value Stop
// can be returned to terminate search early.
type GrepFunc func(path string, res Results, err error) error

// Grep searches port Makefiles, looking for matches described by rxs.  It
// starts looking for Makefiles in root directory, and descends up to two
// levels down (category/port).  If cats slice is not empty, Grep descends only
// to categories listed in cats.  By default, multiple regular expressions in
// rxs are AND-ed together, this can be changed by setting rxsOred to true.
// By default, only the first match per Makefile is returned; set rxsAll to
// emit every non-overlapping match.  The search will be run by using up to
// jobs goroutines, the usual practice is to set this to runtime.GOMAXPROCS(0)
// for the best results.
func Grep(portsRoot string, categories []string, rxs []*Regexp, rxsOred, rxsAll bool, gfn GrepFunc, maxJobs int) error {
	walkCh, err := walk(portsRoot, categories, maxJobs)
	if err != nil {
		return err
	}
	grepCh, err := walkCh.grep(rxs, rxsOred, rxsAll, maxJobs)
	if err != nil {
		return err
	}

	for x := range grepCh {
		if err := gfn(x.path, x.results, x.err); err != nil {
			if err == Stop {
				break
			}
			return err
		}
	}

	return nil
}

var ignores = map[string]struct{}{
	".git":      {},
	".hooks":    {},
	".svn":      {},
	"Keywords":  {},
	"Mk":        {},
	"Templates": {},
	"Tools":     {},
	"distfiles": {},
	"packages":  {},
}

type walkResult struct {
	path string
	err  error
}

type walkChan chan walkResult

func walk(portsRoot string, categories []string, maxJobs int) (walkChan, error) {
	dir, err := os.ReadDir(portsRoot)
	if err != nil {
		return nil, err
	}

	out := make(walkChan, maxJobs)

	go func() {
		defer close(out)

		// prepare category filter set for fast lookup
		catSet := make(map[string]struct{})
		for _, c := range categories {
			catSet[c] = struct{}{}
		}

		var wg sync.WaitGroup
		sem := make(chan int, maxJobs)

		for _, fi := range dir {
			if !fi.IsDir() {
				continue
			}

			name := fi.Name()
			if _, ok := ignores[name]; ok {
				continue
			}

			if len(catSet) != 0 {
				if _, ok := catSet[name]; !ok {
					continue
				}
			}

			sem <- 1
			wg.Go(func() {
				defer func() { <-sem }()

				catRoot := filepath.Join(portsRoot, name)
				dir, err := os.ReadDir(catRoot)
				if err != nil {
					out <- walkResult{err: err}
					return
				}
				for _, fi := range dir {
					if fi.IsDir() {
						out <- walkResult{path: filepath.Join(catRoot, fi.Name())}
					}
				}
			})
		}

		wg.Wait()
	}()

	return out, nil
}

type grepResult struct {
	path    string
	results Results
	err     error
}

type grepChan chan grepResult

func (walk walkChan) grep(rxs []*Regexp, rxsOr, rxsAll bool, maxJobs int) (grepChan, error) {
	out := make(grepChan, maxJobs)

	go func() {
		defer close(out)

		var wg sync.WaitGroup
		sem := make(chan int, maxJobs)

		for w := range walk {
			if w.err != nil {
				out <- grepResult{err: w.err}
				continue
			}

			// no regexp provided, everything matches
			if len(rxs) == 0 {
				out <- grepResult{path: w.path}
				continue
			}

			sem <- 1
			wg.Go(func() {
				defer func() { <-sem }()

				buf, err := readFile(filepath.Join(w.path, "Makefile"))
				if err != nil {
					if errors.Is(err, fs.ErrNotExist) {
						// Makefile doesn't exist at path... odd, but okay
						return
					}
					out <- grepResult{err: err}
					return
				}
				defer bufPut(buf)

				// inline replace of newlines ("\\\n") with "\x00\x00"
				b := buf.Bytes()
				for i := 0; i < len(b)-1; i++ {
					if b[i] == '\\' && b[i+1] == '\n' {
						b[i], b[i+1] = 0, 0
						i++
					}
				}

				var res Results
				for _, r := range rxs {
					var ms Results
					if rxsAll {
						ms, err = r.MatchAll(b)
					} else {
						var m *Result
						m, err = r.Match(b)
						if m != nil {
							ms = Results{m}
						}
					}
					if err != nil {
						out <- grepResult{err: err}
						return
					}
					if !rxsOr && len(ms) == 0 {
						return // results are ANDed and the current rx doesn't match
					}
					for _, m := range ms {
						m.Text = bytes.ReplaceAll(m.Text, []byte{0, 0}, []byte{'\\', '\n'})
						res = append(res, m)
					}
				}

				if res != nil {
					out <- grepResult{path: w.path, results: res}
				}
			})
		}

		wg.Wait()
	}()

	return out, nil
}

func readFile(filename string) (*bytes.Buffer, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	buf := bufGet()
	buf.Grow(int(fi.Size()) + bytes.MinRead)
	_, err = buf.ReadFrom(f)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

var bufPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

func bufGet() *bytes.Buffer {
	return bufPool.Get().(*bytes.Buffer)
}

func bufPut(b *bytes.Buffer) {
	b.Reset()
	bufPool.Put(b)
}
