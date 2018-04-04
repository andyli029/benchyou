/*
 * benchyou
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package sysbench

import (
	"fmt"
	"log"
	"math"
//	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"xcommon"
	"xworker"
	"strconv"
)

// Update tuple.
type Update struct {
	stop     bool
	requests uint64
	conf     *xcommon.Conf
	workers  []xworker.Worker
	lock     sync.WaitGroup
}

// NewUpdate creates the new update handler.
func NewUpdate(conf *xcommon.Conf, workers []xworker.Worker) xworker.Handler {
	return &Update{
		conf:    conf,
		workers: workers,
	}
}

// Run used to start the worker.
func (update *Update) Run() {
	threads := len(update.workers)
	for i := 0; i < threads; i++ {
		update.lock.Add(1)
		go update.Update(&update.workers[i], threads, i)
	}
}

// Stop used to stop the worker.
func (update *Update) Stop() {
	update.stop = true
	update.lock.Wait()
}

// Rows returns the row numbers.
func (update *Update) Rows() uint64 {
	return atomic.LoadUint64(&update.requests)
}

// Update used to execute the update query.
func (update *Update) Update(worker *xworker.Worker, num int, id int) {
	session := worker.S
	bs := int64(math.MaxInt64) / int64(num)
	lo := bs * int64(id)
	hi := bs * int64(id+1)

	for !update.stop {
		var sql string
		var id int64

		if update.conf.Random {
			id = xcommon.RandInt64(lo, hi)
		} else {
			id = lo
			lo++
		}
		c := xcommon.RandString(xcommon.Ctemplate)
		c = strconv.Itoa(10)
		//table := rand.Int31n(int32(worker.N))
		sql = fmt.Sprintf("update mbk_modou_total_v2  set modou=modou+'%s' where user_id=%v",  c, id)

		t := time.Now()
		
		if err := session.Exec(sql); err != nil {
			log.Panicf("update.error[%v]", err)
		}
		
		elapsed := time.Since(t)

		// stats
		nsec := uint64(elapsed.Nanoseconds())
		worker.M.WCosts += nsec
		if worker.M.WMax == 0 && worker.M.WMin == 0 {
			worker.M.WMax = nsec
			worker.M.WMin = nsec
		}

		if nsec > worker.M.WMax {
			worker.M.WMax = nsec
		}
		if nsec < worker.M.WMin {
			worker.M.WMin = nsec
		}
		worker.M.WNums++
		atomic.AddUint64(&update.requests, 1)
	}
	update.lock.Done()
}
