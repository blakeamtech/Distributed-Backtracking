package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"container/list"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const PARAMS = "--rand-freq=0.001 "
const TIMER = "perl post.costas.timer.pl"
const POST = "perl post.costas.simple.pl"
const WORKER_PROG = "costas.clingo"
const WORKER_COMP = "clingo"
const SOL = 0
const T = 5
const D = 0
const N = 4

const CHAN_LEN = 100000
const OPSPS = 500000
const MAP_SET = 1
const PAUSE = 100
const BAR = 50

const BAR_OUT = 0
const SUMMARY_OUT = 1
const LOG_OUT = 2
const RESTART_OUT = 3
const RESTART_IN = 4
const DEFAULT = (1<<4)-1

/*** STRUCTURES ***/

// Worker return
type worker struct {
	assignment []int
	time       float64
	err        string
	uid        int
	sol        int
}

// Commander unit
type commander struct {
	// Assignment queue and com channels
	dispatch_chan	chan []int
	report_chan	chan worker
	queue		*list.List
	// Log files
	resin *os.File
	sum   *os.File
	log   *os.File
	res   *os.File
	// Search settings
	head	[]int
	time	int
	d	int
	m	int
	n	int
	// Output settings
	settings	int
	mx_sol		int
	// Hash
	table	map[string]int
	fac	[]int
	// Counters and flags
	drawing	*sync.Mutex
	mutex	*sync.Mutex
	timer	time.Time
	started	bool
	done    int
	jobs    int
	sols    int
}

/*** UTILITIES ***/

// Progress bar
func (c *commander) bar() {
	if c.settings&(1<<BAR_OUT) == 0 {
		return
	}
	c.drawing.Lock()
	fmt.Print("\r|")
	q := float64(c.done) / float64(c.done+c.jobs)
	for i := 1; i <= int(BAR*q); i++ {
		fmt.Print("#")
	}
	for i := int(BAR * q); i < BAR; i++ {
		fmt.Print("-")
	}
	fmt.Print("|\t", c.done, "/", c.done+c.jobs)
	c.drawing.Unlock()
}

// Slice to string
func to_string(a []int) string {
	s := ""
	if len(a) == 0 {
		return s
	}
	s += strconv.Itoa(a[0])
	for i, n := range a {
		if i > 0 {
			s += " " + strconv.Itoa(n)
		}
	}
	return s
}

// String to slice
func to_slice(s string) []int {
	f := strings.Fields(s)
	a := []int{}
	for _, n := range f {
		t, _ := strconv.ParseInt(n, 10, 32)
		a = append(a, int(t))
	}
	return a
}

// interface{} to slice
func unwrap(t interface{}) []int {
	a := []int{}
	nt, _ := t.([]int)
	for _, n := range nt {
		a = append(a, int(n))
	}
	return a
}

// Two slices are equal
func equals(a []int, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i, n := range a {
		if n != b[i] {
			return false
		}
	}
	return true
}

// Return whether slice contains element
func contains(s []int, e int) bool {
	for _, n := range s {
		if n == e {
			return true
		}
	}
	return false
}

// Difficulty heuristic -- factorial
func (c *commander) diff(s []int) int {
	if c.fac[c.m-len(s)] > OPSPS*c.time {
		return 0
	}
	return c.time
}

/*** WORKER UNIT ***/

// Assign next task to worker
func assign_unit(uid int, com *commander) {
	for {
		select {
		case nxt := <-com.dispatch_chan:
			// Receive assignment and write to *.data file
			if com.settings&(1<<LOG_OUT) != 0 {
				fmt.Fprintf(com.log, "Assigned\t%v\t\tID %d\n", nxt, uid)
			}
			// If assignment too difficult, skip computing
			if com.diff(nxt) == 0 {
				com.report_chan <- worker{nxt, 0, "not attempted", uid, 0}
				continue
			}
			assignment := "assignments/costas.worker_" + strconv.Itoa(uid) + ".data"
			file, ferr := os.Create(assignment)
			if ferr != nil {
				if com.settings&(1<<LOG_OUT) != 0 {
					fmt.Fprintf(com.log, "ID %d: Error occurred while opening data assignment\n", uid)
				}
				fmt.Println("ID %d: Error occurred while opening data assignment", uid)
			}
			for i, j := range nxt {
				fmt.Fprintf(file, "setting(%d, %d).\n", i+1, j)
			}
			file.Close()
			// Execute costas.clingo and write to file
			if com.settings&(1<<LOG_OUT) != 0 {
				fmt.Fprintf(com.log, "Engaging\t%v\t\tID %d\n", nxt, uid)
			}
			out, err := exec.Command("bash", "-c", WORKER_COMP+" --time-limit="+strconv.Itoa(com.diff(nxt))+" "+PARAMS+"-n 0 -c m="+strconv.Itoa(com.m)+" "+assignment+" "+WORKER_PROG).Output()
			if com.settings&(1<<LOG_OUT) != 0 {
				fmt.Fprintf(com.log, "Computed\t%v\t\tID %d\t%q\n", nxt, uid, err)
			}
			post_assignment := "assignments/costas.post-processor_" + strconv.Itoa(uid) + ".in"
			post_file, pferr := os.Create(post_assignment)
			if pferr != nil {
				if com.settings&(1<<LOG_OUT) != 0 {
					fmt.Fprintf(com.log, "ID %d: Error occurred while opening post-process assignment\n", uid)
				}
				fmt.Println("ID %d: Error occurred while opening post-process assignment", uid)
			}
			fmt.Fprintf(post_file, string(out[:len(out)]))
			post_file.Close()
			// Run post-processor
			if com.settings&(1<<LOG_OUT) != 0 {
				fmt.Fprintf(com.log, "Prcssing\t%v\t\tID %d\n", nxt, uid)
			}
			pst := exec.Command("bash", "-c", POST)
			stdin, _ := pst.StdinPipe()
			io.WriteString(stdin, post_assignment)
			stdin.Close()
			pstout, psterr := pst.Output()
			if com.settings&(1<<LOG_OUT) != 0 {
				fmt.Fprintf(com.log, "PstPrcss\t%v\t\tID %d\n", nxt, uid)
			}
			var sol int
			if psterr != nil {
				if com.settings&(1<<LOG_OUT) != 0 {
					fmt.Fprintf(com.log, "ID %d: Error occurred in post-processing\n", uid)
				}
				fmt.Println("ID %d: Error occurred in post-processing", uid)
				sol = 0
			} else {
				t, _ := strconv.ParseInt(string(pstout[:len(pstout)-1]), 10, 32)
				sol = int(t)
			}
			// Run timer
			tmr := exec.Command("bash", "-c", TIMER)
			tmrin, _ := tmr.StdinPipe()
			io.WriteString(tmrin, post_assignment)
			tmrin.Close()
			tmrout, _ := tmr.Output()
			if com.settings&(1<<LOG_OUT) != 0 {
				fmt.Fprintf(com.log, "Finished\t%v\t\tID %d\n", nxt, uid)
			}
			tm, _ := strconv.ParseFloat(string(tmrout[:len(tmrout)-1]), 64)
			com.report_chan <- worker{nxt, float64(tm), err.Error(), uid, sol}
		default:
                        time.Sleep(PAUSE * time.Millisecond)
		}
	}
}

/*** COMMANDER FRAMEWORK ***/

// Central dispatcher - feeds queue to dispatch channel
func (c *commander) dispatcher() {
	for {
		if c.queue.Len() != 0 {
			c.mutex.Lock()
			uw := unwrap(c.queue.Front().Value)
			c.mutex.Unlock()
			c.dispatch_chan <- uw
			if c.settings&(1<<LOG_OUT) != 0 {
				fmt.Fprintf(c.log, "Dispatch\t%v\n", uw)
			}
			c.mutex.Lock()
			c.queue.Remove(c.queue.Front())
			c.mutex.Unlock()
		} else {
                        time.Sleep(PAUSE * time.Millisecond)
		}
	}
}

// Commander constructor
func (c *commander) construct(m int, n int, tm int, settings int, mx_sol int, resin string, d int, head []int) {
	// Log files
	c.resin, _ = os.Open(resin)
	c.sum, _ = os.Create("summary.txt")
	c.log, _ = os.Create("log.txt")
	// Assignment queue and com channels
	c.dispatch_chan = make(chan []int, CHAN_LEN)
	c.report_chan = make(chan worker, CHAN_LEN)
	c.queue = list.New()
	// Hash
	c.table = make(map[string]int)
	c.fac = []int{}
	// Counters and flags
	c.drawing = &sync.Mutex{}
	c.mutex = &sync.Mutex{}
	c.timer = time.Now()
	c.started = true
	c.done = 0
	c.jobs = 0
	c.sols = 0
	// Set parameters
	c.settings = settings
	c.mx_sol = mx_sol
	c.head = head
	c.time = tm
	c.d = d
	c.m = m
	c.n = n
}

// Process result
func (c *commander) process(result worker) bool {

	// Log reception
	if c.settings&(1<<LOG_OUT) != 0 {
		fmt.Fprintf(c.log, "Received\t%v\t\tID %d\t%q\t%d\t%fs\t%d\n", result.assignment, result.uid, result.err, result.sol, result.time, c.jobs-1)
	}
	if c.settings&(1<<SUMMARY_OUT) != 0 && result.uid >= 0 {
		fmt.Fprintf(c.sum, "%v   \tID %d\t%q\t%d solution(s)\t%fs", result.assignment, result.uid, result.err, result.sol, result.time)
	}
	if c.settings&(1<<RESTART_OUT) != 0 && (result.uid >= 0 || c.diff(result.assignment) == 0) {
		fmt.Fprintf(c.res, "%d", len(result.assignment))
		for _, n := range result.assignment {
			fmt.Fprintf(c.res, " %d", n)
		}
		fmt.Fprintf(c.res, " %d ", result.sol)
	}
	c.bar()

	// Initial call
	if result.uid < 0 {
		if c.diff(result.assignment) == 0 || c.d > len(result.assignment) {
                        if c.settings&(1<<RESTART_OUT) != 0 {
                                fmt.Fprintf(c.res, "-1.0\n")
                        }
			for i := 1; i <= c.m; i++ {
				if !contains(result.assignment, i) {
					c.jobs++
					c.process(worker{append(append([]int(nil), result.assignment...), i), 0, "initial assignment", -1, 0})
				}
			}
			c.jobs--
			c.done++
		} else {
			c.mutex.Lock()
			c.queue.PushBack(append([]int(nil), result.assignment...))
			c.mutex.Unlock()
		}
		return false
	}

	// Worker result
	c.jobs--
	c.done++
	if result.err != "exit status 10" && result.err != "exit status 30" && result.err != "exit status 20" {
		if c.settings&(1<<SUMMARY_OUT) != 0 {
			fmt.Fprintf(c.sum, " (killed)\n")
		}
		if c.settings&(1<<RESTART_OUT) != 0 {
			fmt.Fprintf(c.res, "-1.0\n")
		}
		for i := 1; i <= c.m; i++ {
			if !contains(result.assignment, i) {
				c.mutex.Lock()
				c.queue.PushBack(append(append([]int(nil), result.assignment...), i))
				c.mutex.Unlock()
				if c.settings&(1<<LOG_OUT) != 0 {
					fmt.Fprintf(c.log, "Enqueued\t%v\t\t%d\n", append(append([]int(nil), result.assignment...), i), c.jobs+1)
				}
				c.jobs++
			}
		}
	} else {
		if c.settings&(1<<SUMMARY_OUT) != 0 {
			fmt.Fprintf(c.sum, "\n")
		}
		if c.settings&(1<<RESTART_OUT) != 0 {
			fmt.Fprintf(c.res, "%f\n", result.time)
		}
		c.sols += result.sol
	}
	if c.mx_sol != 0 && c.sols >= c.mx_sol {
		if c.settings&(1<<LOG_OUT) != 0 {
			fmt.Fprintf(c.log, "Sounding Recall\n")
		}
		return true
	}
	return false
}

// Launch search
func (c *commander) launch() {

	// Fill fac and exp
	c.fac = append(c.fac, 1)
	for i := 1; i <= c.m; i++ {
		c.fac = append(c.fac, int(math.Min(float64(OPSPS*c.time+1), float64(c.fac[i-1]*i))))
	}

        // Hash base cases
        for i := 1; i <= c.m; i++ {
                	if !(contains(c.head, i)) {
				c.table[to_string(append(c.head, i))] = MAP_SET
			if c.settings&(1<<LOG_OUT) != 0 {
                        	fmt.Fprintf(c.log, "Reloaded\t[%v]\n", i)
                	}
		}
        }

	// Restart
	if c.settings&(1<<RESTART_IN) != 0 {

		fmt.Print("Restarting...")

		scanner := bufio.NewScanner(c.resin)
		scanner.Split(bufio.ScanWords)

		for scanner.Scan() {

			// Scan line
			n, _ := strconv.ParseInt(scanner.Text(), 10, 32)
			p := []int{}
			for i := 0; i < int(n); i++ {
				scanner.Scan()
				pa, _ := strconv.ParseInt(scanner.Text(), 10, 32)
				p = append(p, int(pa))
			}
			scanner.Scan()
			sol, _ := strconv.ParseInt(scanner.Text(), 10, 32)
			scanner.Scan()
			kill, _ := strconv.ParseFloat(scanner.Text(), 64)

			// Log
			if c.settings & (1<<SUMMARY_OUT) != 0 {
				fmt.Fprintf(c.sum, "%v   \t   \t%q\t%d solution(s)", p, "loaded", sol)
				if kill < 0.0 {
					fmt.Fprintf(c.sum, " (killed)\n")
				} else {
					fmt.Fprintf(c.sum, "\n")
				}
			}

			// Delete from hash table
			delete(c.table, to_string(p))

			// Enqueue children or add result
			if kill >= 0.0 {
				c.sols += int(sol)
			} else {
				for i := 1; i <= c.m; i++ {
					if contains(p, i) {
						continue
					}
					if len(p) == int(n) {
						p = append(p, 0)
					}
					p[n] = i
					if c.table[to_string(p)] == 0 {
						c.table[to_string(p)] = MAP_SET
					}
				}
			}

		}

                fmt.Print("\r\033[2K")

	}

	all_done := (len(c.table) == 0)

	// Queue hash
	go func() {
		for p := range c.table {
			c.jobs++
			c.report_chan <- worker{to_slice(p), 0, "initial assignment", -1, 0}
			if c.settings & (1 << LOG_OUT) != 0 {
				fmt.Fprintf(c.log, "Chnnling\t%v\t\t%d\n", to_slice(p), c.jobs+1)
			}
		}
		c.table = nil
	}()

        // All done
        if all_done {
                fmt.Println(c.sols, "SOLUTIONS")
                return
        }

	// Restart file
	if c.settings & (1 << RESTART_OUT) != 0 {
		if c.settings & (1 << RESTART_IN) != 0 {
			c.res, _ = os.OpenFile(c.resin.Name(), os.O_APPEND|os.O_WRONLY, 0600)
		} else {
			c.res, _ = os.Create("restart.txt")
		}
	}

	// Launch the dispatcher
	go c.dispatcher()

	// First round of assignments
	os.Mkdir("./assignments", 0755)
	for i := 0; i < c.n; i++ {
		go assign_unit(i, c)
	}

	// Main loop
Mainloop:
	for {
		select {
		case result := <-c.report_chan:
			if c.process(result) {
				break Mainloop
			}
		default:
			if c.jobs == 0 {
				if c.started {
					if c.settings&(1<<LOG_OUT) != 0 {
						fmt.Fprintf(c.log, "Sounding Recall\n")
					}
					break Mainloop
				}
			} else {
				c.started = true
			}
		}
	}

	c.bar()
	fmt.Println()
	tm := time.Now().Sub(c.timer).Seconds()
	fmt.Println(c.sols, "SOLUTIONS")
	fmt.Printf("%fs\n", tm)
	if c.settings&(1<<LOG_OUT) != 0 {
		fmt.Fprintf(c.log, "%d SOLUTIONS\n", c.sols)
		fmt.Fprintf(c.log, "%fs\n", tm)
	}
	c.log.Close()
	c.sum.Close()

}

// Take input and launch commander unit
func main() {

	com := []rune{}
        resin := "restart.txt"
        settings := DEFAULT
	head := []int{}
	mx_sol := SOL
        t := T
	d := D
        m := -1
        n := N

	foo1 := func(n *int, s string) {
		nn, _ := strconv.ParseInt(s[1:], 10, 32)
		*n = int(nn)
	}
	foo2 := func(n *int, s string) {
		var v int
		if s[0] == 'L' { v = LOG_OUT } else { if s[0] == 'S' { v = SUMMARY_OUT } else { if s[0] == 'R' { v = RESTART_OUT }  else { v = BAR_OUT } } }
		if s [1:] == "0" { *n&=^(int(1)<<uint(v)) } else { *n|=(int(1)<<uint(v)) }
	}
	fun := map[rune]func(*int, string) { 'n':foo1, 'm':foo1, 't':foo1, 'd':foo1, 's':foo1, 'L':foo2, 'S':foo2, 'R':foo2, 'B': foo2}
	arg := map[rune]*int { 'n':&n, 'm':&m, 't':&t, 'd':&d, 'L':&settings, 's':&mx_sol, 'S':&settings, 'R':&settings, 'B':&settings }

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Special settings: ")
	line , _ := reader.ReadString('\n')
	set := strings.Fields(line)

	for _, s := range set {
		if s[0] == '-' {
			for _, c := range s[1:] {
				com = append(com, c)
			}
		} else if len(com) > 0 {
			c := com[0]
			com = com[1:]
			if c == 'r' {
				settings|=(1<<RESTART_IN)
				resin = s
			} else if c == 'H' {
				hd := strings.FieldsFunc(s, func(r rune) bool { return r == ':' || r == '-' || r == '_'; })
				for _, e := range hd {
					i, _ := strconv.ParseInt(e, 10, 32)
					head = append(head, int(i))
				}
			} else if _, ok := arg[c]; ok {
                                fun[c](arg[c], string(c) + s)
			} else {
				fmt.Print("Command not recognized: -%s\n", c)
			}
		}
	}

	if m < 1 {
		fmt.Print(string(27) + "[1A:\r\033[2KM: ")
		mln, _ := reader.ReadString('\n')
		mm, _ := strconv.ParseInt(mln[:len(mln)-1], 10, 32)
		m = int(mm)
	}
	fmt.Print(string(27) + "[1A:\r\033[2K")

	var c commander
	c.construct(m, n, t, settings, mx_sol, resin, d, head)
	c.launch()

}
