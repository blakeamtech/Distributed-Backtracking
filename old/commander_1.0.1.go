package main

import (
        "fmt"
        "io"
        "os"
        "os/exec"
        "strconv"
        "time"
)

const TIMER = "perl post.costas.timer.pl"
const POST = "perl post.costas.simple.pl"
const WORKER_PROG = "costas.clingo"
const WORKER_COMP = "clingo"
const NUM_THREAD = 4
const PAUSE = 100
const TIME = 5
const BAR = 50
const M = 12

const BAR_OUT = 0
const SUMMARY_OUT = 1
const LOG_OUT = 2
const OUTPUT_SETTING = (1 << BAR_OUT) | (1 << SUMMARY_OUT) | (1 << LOG_OUT)

// Worker return
type worker struct {
        assignment []int
        time       float64
        err        string
        uid        int
        sol        int
}

// Progress bar
func bar(done int, queued int) {
        if OUTPUT_SETTING&(1<<BAR_OUT) == 0 {
                return
        }
        fmt.Print("\r|")
        q := float64(done) / float64(done+queued)
        for i := 1; i <= int(BAR*q); i++ {
                fmt.Print("#")
        }
        for i := int(BAR * q); i < BAR; i++ {
                fmt.Print("-")
        }
        fmt.Print("|\t", done, "/", done+queued)
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
func assign_unit(dispatch chan []int, uid int, com chan worker, log *os.File) {
        for {
                select {
                case nxt := <-dispatch:
                        // Receive assignment and write to *.data file
                        if OUTPUT_SETTING&(1<<LOG_OUT) != 0 {
                                fmt.Fprintf(log, "Assigned\t%v\t\tID %d\n", nxt, uid)
                        }
                        assignment := "assignments/costas.worker_" + strconv.Itoa(uid) + ".data"
                        file, ferr := os.Create(assignment)
                        if ferr != nil {
                                if OUTPUT_SETTING&(1<<LOG_OUT) != 0 {
                                        fmt.Fprintf(log, "ID %d: Error occurred while opening data assignment\n", uid)
                                }
                                fmt.Println("ID %d: Error occurred while opening data assignment", uid)
                        }
                        for i, j := range nxt {
                                fmt.Fprintf(file, "setting(%d, %d).\n", i+1, j)
                        }
                        file.Close()
                        // Execute costas.clingo and write to file
                        if OUTPUT_SETTING&(1<<LOG_OUT) != 0 {
                                fmt.Fprintf(log, "Engaging\t%v\t\tID %d\n", nxt, uid)
                        }
                        out, err := exec.Command("bash", "-c", WORKER_COMP+" --time-limit="+strconv.Itoa(TIME)+" -n 0 -c m="+strconv.Itoa(M)+" "+assignment+" "+WORKER_PROG).Output()
                        if OUTPUT_SETTING&(1<<LOG_OUT) != 0 {
                                fmt.Fprintf(log, "Computed\t%v\t\tID %d\t%q\n", nxt, uid, err)
                        }
                        post_assignment := "assignments/costas.post-processor_" + strconv.Itoa(uid) + ".in"
                        post_file, pferr := os.Create(post_assignment)
                        if pferr != nil {
                                if OUTPUT_SETTING&(1<<LOG_OUT) != 0 {
                                        fmt.Fprintf(log, "ID %d: Error occurred while opening post-process assignment\n", uid)
                                }
                                fmt.Println("ID %d: Error occurred while opening post-process assignment", uid)
                        }
                        fmt.Fprintf(post_file, string(out[:len(out)]))
                        post_file.Close()
                        // Run post-processor
                        if OUTPUT_SETTING&(1<<LOG_OUT) != 0 {
                                fmt.Fprintf(log, "Prcssing\t%v\t\tID %d\n", nxt, uid)
                        }
                        pst := exec.Command("bash", "-c", POST)
                        stdin, _ := pst.StdinPipe()
                        io.WriteString(stdin, post_assignment)
                        stdin.Close()
                        pstout, psterr := pst.Output()
                        if OUTPUT_SETTING&(1<<LOG_OUT) != 0 {
                                fmt.Fprintf(log, "PstPrcss\t%v\t\tID %d\n", nxt, uid)
                        }
                        var sol int
                        if psterr != nil {
                                if OUTPUT_SETTING&(1<<LOG_OUT) != 0 {
                                        fmt.Fprintf(log, "ID %d: Error occurred in post-processing\n", uid)
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
                        if OUTPUT_SETTING&(1<<LOG_OUT) != 0 {
                                fmt.Fprintf(log, "Finished\t%v\t\tID %d\n", nxt, uid)
                        }
                        tm, _ := strconv.ParseFloat(string(tmrout[:len(tmrout)-1]), 64)
                        com <- worker{nxt, float64(tm), err.Error(), uid, sol}
                default:
                        time.Sleep(PAUSE * time.Millisecond)
                }
        }
}

// Central dispatcher - feeds queue to dispatch channel
func dispatcher(q *[][]int, dispatch chan []int, log *os.File) {
        for {
                if len(*q) != 0 {
                        dispatch <- (*q)[0]
                        if OUTPUT_SETTING&(1<<LOG_OUT) != 0 {
                                fmt.Fprintf(log, "Dispatch\t%v\n", (*q)[0])
                        }
                        *q = (*q)[1:]
                } else {
                        time.Sleep(PAUSE * time.Millisecond)
                }
        }
}

// Commander unit
func main() {

        // Log files
        sum, _ := os.Create("summary.txt")
        log, _ := os.Create("log.txt")

        // Assignment queue and com channels
        dispatch_chan := make(chan []int, 2*NUM_THREAD)
        com := make(chan worker, 2*NUM_THREAD)
        assignment_queue := [][]int{}
        started := true
        done := 0
        jobs := 0
        sols := 0
        for i := 1; i <= M; i++ {
                assignment_queue = append(assignment_queue, []int{i})
                if OUTPUT_SETTING&(1<<LOG_OUT) != 0 {
                        fmt.Fprintf(log, "Enqueued\t%v\n", assignment_queue[len(assignment_queue)-1])
                }
                jobs++
        }

        // Launch the dispatcher
        go dispatcher(&assignment_queue, dispatch_chan, log)

        // First round of assignments
        os.Mkdir("./assignments", 0777)
        for i := 0; i < NUM_THREAD; i++ {
                go assign_unit(dispatch_chan, i, com, log)
        }

        // Main loop
Mainloop:
        for {
                select {
                case result := <-com:
                        if OUTPUT_SETTING&(1<<LOG_OUT) != 0 {
                                fmt.Fprintf(log, "Received\t%v\t\tID %d\t%q\t%d\t%d\n", result.assignment, result.uid, result.err, result.sol, jobs-1)
                        }
                        if OUTPUT_SETTING&(1<<SUMMARY_OUT) != 0 {
                                fmt.Fprintf(sum, "%v   \tID %d\t%q\t%d solution(s)\t%fs", result.assignment, result.uid, result.err, result.sol, result.time)
                        }
                        jobs--
                        done++
                        bar(done, jobs)
                        if result.err != "exit status 10" && result.err != "exit status 30" && result.err != "exit status 20" {
                                if OUTPUT_SETTING&(1<<SUMMARY_OUT) != 0 {
                                        fmt.Fprintf(sum, " (killed)\n")
                                }
                                for i := 1; i <= M; i++ {
                                        if !contains(result.assignment, i) {
                                                assignment_queue = append(assignment_queue, append(result.assignment, i))
                                                if OUTPUT_SETTING&(1<<LOG_OUT) != 0 {
                                                        fmt.Fprintf(log, "Enqueued\t%v\t\t%d\n", append(result.assignment, i), jobs+1)
                                                }
                                                jobs++
                                        }
                                }
                        } else {
                                if OUTPUT_SETTING&(1<<SUMMARY_OUT) != 0 {
                                        fmt.Fprintf(sum, "\n")
                                }
                                sols += result.sol
                        }
                default:
                        time.Sleep(PAUSE * time.Millisecond)
                        if jobs == 0 {
                                if started {
                                        if OUTPUT_SETTING&(1<<LOG_OUT) != 0 {
                                                fmt.Fprintf(log, "Sounding Recall\n")
                                        }
                                        break Mainloop
                                }
                        } else {
                                started = true
                        }
                }
        }

        fmt.Println()
        fmt.Println(sols, "SOLUTIONS")
        if OUTPUT_SETTING&(1<<LOG_OUT) != 0 {
                fmt.Fprintf(log, "%d SOLUTIONS\n", sols)
        }
        log.Close()
        sum.Close()

}

