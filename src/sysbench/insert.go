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

      //	"github.com/xelabs/go-mysqlstack/common"
)

// Insert tuple.
type Insert struct {
	stop     bool
	requests uint64
	conf     *xcommon.Conf
	workers  []xworker.Worker
	lock     sync.WaitGroup
}

// NewInsert creates the new insert handler.
func NewInsert(conf *xcommon.Conf, workers []xworker.Worker) xworker.Handler {
	return &Insert{
		conf:    conf,
		workers: workers,
	}
}

// Run used to start the worker.
func (insert *Insert) Run() {
	threads := len(insert.workers)
	for i := 0; i < threads; i++ {
		insert.lock.Add(1)
		go insert.Insert(&insert.workers[i], threads, i)
	}
}

// Stop used to stop the worker.
func (insert *Insert) Stop() {
	insert.stop = true
	insert.lock.Wait()
}

// Rows returns the row numbers.
func (insert *Insert) Rows() uint64 {
	return atomic.LoadUint64(&insert.requests)
}

// Insert used to execute the insert query.
func (insert *Insert) Insert(worker *xworker.Worker, num int, id int) {
	session := worker.S
	bs := int64(math.MaxInt64) / int64(num)
	lo := bs * int64(id)
	hi := bs * int64(id+1)
	sqls := `INSERT INTO  mbk_modou_v2  (
		modou_id,user_id,city_code,type,exchange_status,order_id,activity_id,modou,description, has_apply, apply_expire_time,expire_time )
		VALUES ('MD1522294684493579271222', '%v', '010', 4, null, null, null, 2, 'MOCOIN_TYPE_SHARE', '1', null, null )`
	/*columns1 := "k,c,pad"
	columns2 := "k,c,pad,id"
	valfmt1 := "(%v,'%s', '%s'),"
	valfmt2 := "(%v,'%s', '%s', %v),"
        */

	for !insert.stop {
		var sql  string
		user_id := xcommon.RandInt64(lo, hi)
		sql = fmt.Sprintf(sqls, user_id)
	//	buf := common.NewBuffer(256)

		//table := rand.Int31n(int32(worker.N))
	/*	if insert.conf.Random {
			sql = fmt.Sprintf("insert into sysbench(%s) values", columns2)
		} else {
			sql = fmt.Sprintf("insert into sysbench(%s) values",  columns1)
		}

		// pack requests
		for n := 0; n < insert.conf.RowsPerInsert; n++ {
			pad := xcommon.RandString(xcommon.Padtemplate)
			c := xcommon.RandString(xcommon.Ctemplate)

			if insert.conf.Random {
				value = fmt.Sprintf(valfmt2,
					xcommon.RandInt64(lo, hi),
					c,
					pad,
					xcommon.RandInt64(lo, hi),
				)
			} else {
				value = fmt.Sprintf(valfmt1,
					xcommon.RandInt64(lo, hi),
					c,
					pad,
				)
			}
			buf.WriteString(value)
		}
		// -1 to trim right ','
		vals, err := buf.ReadString(buf.Length() - 1)
		if err != nil {
			log.Panicf("insert.error[%v]", err)
		}
		sql += vals
            */

		t := time.Now()
               // fmt.Printf(sql)		
		if err := session.Exec(sql); err != nil {
			log.Panicf("insert.error[%v]", err)
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
		atomic.AddUint64(&insert.requests, 1)
	}
	insert.lock.Done()
}

