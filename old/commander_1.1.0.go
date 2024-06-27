package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"strconv"
	"time"
)

const PARAMS = "--rand-freq=0.001 "
const TIMER = "perl post.costas.timer.pl"
const POST = "perl post.costas.simple.pl"
const WORKER_PROG = "costas.clingo"
const WORKER_COMP = "clingo"
const OPSPS = 500000
const PAUSE = 100
const TIME = 5
const BAR = 50

const BAR_OUT = 0
const SUMMARY_OUT = 1
const LOG_OUT = 2
const RESTART_OUT = 3
const RESTART_IN = 4

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
	dispatch_chan chan []int
	report_chan   chan worker
	queue         [][]int
	// Log files
	resin *os.File
	sum   *os.File
	log   *os.File
	res   *os.File
	// Output settings
	settings int
	// Counters and flags
	fac     []int
	started bool
	done    int
	jobs    int
	sols    int
	// Parameters
	m int
	n int
}

/*** UTILITIES ***/

// Progress bar
func (c *commander) bar() {
	if c.settings&(1<<BAR_OUT) == 0 {
		return
	}
	fmt.Print("\r|")
	q := float64(c.done) / float64(c.done+c.jobs)
	for i := 1; i <= int(BAR*q); i++ {
		fmt.Print("#")
	}
	for i := int(BAR * q); i < BAR; i++ {
		fmt.Print("-")
	}
	fmt.Print("|\t", c.done, "/", c.done+c.jobs)
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

// Return index of element in slice
func find(s [][]int, e []int) int {
	for i, n := range s {
		if equals(n, e) {
			return i
		}
	}
	return -1
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
	if c.fac[c.m-len(s)] > OPSPS*TIME {
		return 0
	}
	return TIME
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
		if len(c.queue) != 0 {
			c.dispatch_chan <- c.queue[0]
			if c.settings&(1<<LOG_OUT) != 0 {
				fmt.Fprintf(c.log, "Dispatch\t%v\n", c.queue[0])
			}
			c.queue = c.queue[1:]
		} else {
			time.Sleep(PAUSE * time.Millisecond)
		}
	}
}

// Commander constructor
func (c *commander) construct(m int, n int, settings int, resin string) {
	// Log files
	c.resin, _ = os.Open(resin)
	if resin == "restart.txt" {
		c.res = c.resin
	} else {
		c.res, _ = os.Create("restart.txt")
	}
	c.sum, _ = os.Create("summary.txt")
	c.log, _ = os.Create("log.txt")
	// Assignment queue and com channels
	c.dispatch_chan = make(chan []int, 2*n)
	c.report_chan = make(chan worker, 2*n)
	c.queue = [][]int{}
	// Counters and flags
	c.fac = []int{}
	c.started = true
	c.done = 0
	c.jobs = 0
	c.sols = 0
	// Set parameters
	c.settings = settings
	c.m = m
	c.n = n
}

// Launch search
func (c *commander) launch() {

	// Fill fac
	c.fac = append(c.fac, 1)
	for i := 1; i <= c.m; i++ {
		c.fac = append(c.fac, int(math.Min(float64(OPSPS)*TIME+1, float64(c.fac[i-1]*i))))
	}

	// Enqueue base cases
	for i := 1; i <= c.m; i++ {
		c.queue = append(c.queue, []int{i})
		if c.settings&(1<<LOG_OUT) != 0 {
			fmt.Fprintf(c.log, "Enqueued\t%v\n", i)
		}
		c.jobs++
	}

	// Restart
	if c.settings&1<<RESTART_IN != 0 {

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
			kill, _ := strconv.ParseInt(scanner.Text(), 10, 32)
			c.jobs--

			// Delete from queue
			i := find(c.queue, p)
			c.queue[i] = c.queue[len(c.queue)-1]
			c.queue = c.queue[:len(c.queue)-1]

			// Enqueue children or add result
			if kill == 0 {
				c.sols += int(sol)
			} else {
				for i := 1; i <= c.m; i++ {
					if !contains(p, i) {
						c.queue = append(c.queue, append(p, i))
						c.jobs++
					}
				}
			}

		}

	}

	// All done
	if c.jobs == 0 {
		fmt.Println(c.sols, "SOLUTIONS")
		return
	}

	// Launch the dispatcher
	go c.dispatcher()

	// First round of assignments
	os.Mkdir("./assignments", 0777)
	for i := 0; i < c.n; i++ {
		go assign_unit(i, c)
	}

	// Main loop
Mainloop:
	for {
		select {
		case result := <-c.report_chan:
			if c.settings&(1<<LOG_OUT) != 0 {
				fmt.Fprintf(c.log, "Received\t%v\t\tID %d\t%q\t%d\t%d\n", result.assignment, result.uid, result.err, result.sol, c.jobs-1)
			}
			if c.settings&(1<<SUMMARY_OUT) != 0 {
				fmt.Fprintf(c.sum, "%v   \tID %d\t%q\t%d solution(s)\t%fs", result.assignment, result.uid, result.err, result.sol, result.time)
			}
			if c.settings&(1<<RESTART_OUT) != 0 {
				fmt.Fprintf(c.res, "%d", len(result.assignment))
				for _, n := range result.assignment {
					fmt.Fprintf(c.res, " %d", n)
				}
				fmt.Fprintf(c.res, " %d ", result.sol)
			}
			c.jobs--
			c.done++
			c.bar()
			if result.err != "exit status 10" && result.err != "exit status 30" && result.err != "exit status 20" {
				if c.settings&(1<<SUMMARY_OUT) != 0 {
					fmt.Fprintf(c.sum, " (killed)\n")
				}
				if c.settings&(1<<RESTART_OUT) != 0 {
					fmt.Fprintf(c.res, "1\n")
				}
				for i := 1; i <= c.m; i++ {
					if !contains(result.assignment, i) {
						c.queue = append(c.queue, append(result.assignment, i))
						if c.settings&(1<<LOG_OUT) != 0 {
							fmt.Fprintf(c.log, "Enqueued\t%v\t\t%d\n", append(result.assignment, i), c.jobs+1)
						}
						c.jobs++
					}
				}
			} else {
				if c.settings&(1<<SUMMARY_OUT) != 0 {
					fmt.Fprintf(c.sum, "\n")
				}
				if c.settings&(1<<RESTART_OUT) != 0 {
					fmt.Fprintf(c.res, "0\n")
				}
				c.sols += result.sol
			}
		default:
			time.Sleep(PAUSE * time.Millisecond)
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

	fmt.Println()
	fmt.Println(c.sols, "SOLUTIONS")
	if c.settings&(1<<LOG_OUT) != 0 {
		fmt.Fprintf(c.log, "%d SOLUTIONS\n", c.sols)
	}
	c.log.Close()
	c.sum.Close()

}

// Launch commander unit
func main() {
	var c commander
	c.construct(11, 4, (1<<5)-1, "restart.txt")
	c.launch()
}
