package scheduler

import "time"

// ticker 抽象便于测试注入假时钟。
type tickerIface interface {
	C() <-chan time.Time
	Stop()
}

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }

func newTicker(d time.Duration) tickerIface { return realTicker{t: time.NewTicker(d)} }
