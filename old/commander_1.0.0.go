package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"time"
)

const POST = "perl post.costas.simple.pl"
const WORKER_PROG = "costas.clingo"
const WORKER_COMP = "clingo"
const NUM_THREAD = 8
const PAUSE = 100
const TIME = 1
const M = 11

// Worker return
type worker struct {
	assignment []int
	err        string
	uid        int
	sol        int
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

// Assign next task to worker
func assign_unit(dispatch chan []int, uid int, com chan worker) {
	for {
		select {
		case nxt := <-dispatch:
			// Receive assignment and write to *.data file
			fmt.Println("Assigned ", nxt, " ID ", uid)
			assignment := "assignments/costas.worker_" + strconv.Itoa(uid) + ".data"
			file, ferr := os.Create(assignment)
			if ferr != nil {
				fmt.Println("ID ", uid, ": Error occurred while opening data assignment")
			}
			for i, j := range nxt {
				fmt.Fprintf(file, "setting(%d, %d).\n", i+1, j)
			}
			file.Close()
			// Execute costas.clingo and write to file
			fmt.Println("Engaging ", nxt, " ID ", uid)
			out, err := exec.Command("bash", "-c", WORKER_COMP+" --time-limit="+strconv.Itoa(TIME)+" -n 0 -c m="+strconv.Itoa(M)+" "+assignment+" "+WORKER_PROG).Output()
			fmt.Println("Computed ", nxt, " ID ", uid, " ", err)
			post_assignment := "assignments/costas.post-processor_" + strconv.Itoa(uid) + ".in"
			post_file, pferr := os.Create(post_assignment)
			if pferr != nil {
				fmt.Println("ID ", uid, ": Error occurred while opening post-process assignment")
			}
			fmt.Fprintf(post_file, string(out[:len(out)]))
			post_file.Close()
			// Run post-processor
			fmt.Println("Prcssing ", nxt, " ID ", uid)
			pst := exec.Command("bash", "-c", POST)
			stdin, _ := pst.StdinPipe()
			io.WriteString(stdin, post_assignment)
			stdin.Close()
			pstout, psterr := pst.Output()
			fmt.Println("PstPrcss ", nxt, " ID ", uid, " ", psterr)
			var sol int
			if psterr != nil {
				fmt.Println("ID ", uid, ": Error occurred in post-processing")
				sol = 0
			} else {
				t, _ := strconv.ParseInt(string(pstout[:len(pstout)-1]), 10, 32)
				sol = int(t)
			}
			fmt.Println("Finished ", nxt, " ID ", uid)
			com <- worker{nxt, err.Error(), uid, sol}
		}
	}
}

// Central dispatcher - feeds queue to dispatch channel
func dispatcher(q *[][]int, dispatch chan []int) {
	for {
		if len(*q) != 0 {
			dispatch <- (*q)[0]
			fmt.Println("Dispatch ", (*q)[0])
			*q = (*q)[1:]
		} else {
			time.Sleep(PAUSE * time.Millisecond)
		}
	}
}

// Commander unit
func main() {

	// Assignment queue and com channels
	dispatch_chan := make(chan []int, 2*NUM_THREAD)
	com := make(chan worker, 2*NUM_THREAD)
	assignment_queue := [][]int{}
	started := true
	jobs := 0
	sols := 0
	for i := 1; i <= M; i++ {
		assignment_queue = append(assignment_queue, []int{i})
		fmt.Println("Enqueued ", assignment_queue[len(assignment_queue)-1])
		jobs++
	}

	// Launch the dispatcher
	go dispatcher(&assignment_queue, dispatch_chan)

	// First round of assignments
	os.Mkdir("./assignments", 0777)
	for i := 0; i < NUM_THREAD; i++ {
		go assign_unit(dispatch_chan, i, com)
	}

	// Main loop
Mainloop:
	for {
		select {
		case result := <-com:
			fmt.Println("Received ", result.assignment, " ID ", result.uid, " ", result.err, " ", result.sol, " ", jobs-1)
			jobs--
			if result.err != "exit status 10" && result.err != "exit status 30" && result.err != "exit status 20" {
				for i := 1; i <= M; i++ {
					if !contains(result.assignment, i) {
						assignment_queue = append(assignment_queue, append(result.assignment, i))
						fmt.Println("Enqueued ", append(result.assignment, i), " ", jobs+1)
						jobs++
					}
				}
			} else {
				sols += result.sol
			}
		default:
			if jobs == 0 {
				if started {
					fmt.Println("Sounding  Recall")
					break Mainloop
				}
			} else {
				started = true
			}
		}
	}

	fmt.Println(sols, " SOLUTIONS")

}
