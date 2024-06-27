package main

import (
	"bufio"
	"container/list"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

const Params = "--rand-freq=0.001 "				// Extra parameters for clingo worker
const Timer = "perl post.costas.timer.pl"			// Timer post-processor invocation
const PostProcessor = "perl post.costas.simple.pl"		// Solution-counting post-processor invocation
const WorkerProgram = "sum_free_part.clingo"			// Default worker program file location
const WorkerCompiler = "~raphael/bin/local.x86_64/clingo"	// Default worker compiler location
const MessageLengthBytes = 4					// Length of opening exchange in byte -- transmits length of remaining message as base 256 integer
const MaxByte = 1 << Byte					// One more than the greatest value of a byte -- 256
const MaxTime = 3600						// Default time limit for expected 'easy' tasks
const Net = "tcp"						// Protocol used -- TCP
const HiMark = 10						// High water mark in terms of number of workers
const LoMark = 2						// Low water mark in terms of number of workers
const Byte = 8							// Length of byte -- 8
const Sol = 0							// Default solution cap (0 -> find all)
const G = 10							// Default gauge case size -- 10
const T = 0							// Default thread time limit (0 -> no time limit)
const D = 0							// Default starting depth of dispatched work
const N = 4							// Default number of worker threads
const S = 1							// Default number of startup DFS threads

const ChannelLength = 100000					// Length of channels
const PermutationsPerSecond = 50000000				// Estimate of processing speed
const MapSet = 1						// Value in restart map denoting visited/collected child
const Pause = 100						// Sleep length of threads while idling
const Bar = 50							// Length of status bar (characters)

const Offline = 0						// netCode value for a program that does all work locally
const Client = 1						// netCode value for a 'worker' program that does work dispatched from remote server
const Server = 2						// netCode value for a 'commander' program that dispatches work to clients

const Inconsistent = "exit status 20"				// Return code of inconsistent clingo worker
const Unchecked = "Unchecked"					// Error code stored in failsafe hash for assignments that have not been sent to a backup worker
const Success = "TCP/IP-Worker-Done"				// Error code returned by successful runs of client worker
const RequestCode = "0"						// Opening character of a client message requesting work from a server
const ReturnCode = "1"						// Opening character of a client message reporting results to server
const Killed = "kill"						// Error code returned by failed/terminated run of client worker
const Checked = "Checked"					// Error code stored in failsafe hash for assignments that have been sent to a backup worker

const BarOut = 0						// Bar bit in settings -- non-server program produces status bar iff 1
const SummaryOut = 1						// Summary bit in settings -- program produces summary file iff 1
const LogOut = 2						// Log bit in settings -- program produces log file iff 1
const RestartOut = 3						// Restart (out) bit in settings -- program produces restart file iff 1
const RestartIn = 4						// Restart (in) bit in settings -- program restarts from file iff 1
const AssignmentMap = 5						// Failsafe map bit in settings -- server program produces and cross-checks a map of assignments iff 1
const Repeat = 6						// Repeats allowed in permutations
const ClingoCheck = 7						// Check permutations for validity for 1 sec before pushing children
const Default = 111						// Default settings: produces all files and status bar, uses map and runs initial worker gauge, does repeat in permutations

/*** STRUCTURES ***/

// Worker return						// Encapsulates various information about a completed task
type returnInfo struct {
	assignment []int					// Slice that stores prefix to define sub-tree
	time       float64					// Time elapsed during completion
	err        string					// Error code
	uid        int						// ID of worker unit
	sol        int						// Solutions found
	run        int						// ID of commander run
}

// Commander unit						// Encapsulates information about commander, namely search&network settings, locks, and communication channels
type commander struct {
	// Network information
	ipString	string					// String representation of target IP address
	targetIP	*net.TCPAddr				// Target IP sddress as usable object
	netCode		int					// Code that determines networking behaviour of commander
	TCPLis		*net.TCPListener			// Listener used by server commanders
	wid		int					// ID of worker client programs
	// Failsafe assignment map
	safetyHash	map[string]*list.List			// Failsafe hash to be crossreferenced when accepting work
	// Assignment queue and com channels
	dispatchChan	chan []int				// Channel for dispatching assignment to local worker threads
	startupChan	chan returnInfo				// Channel for processing initial assignments into DFS threads
	reportChan	chan returnInfo				// Channel for reporting work from local worker threads
	queue		*list.List				// Queue populated by commander and fed into dispatchChan by dispatcher
	// Log files
	resinstr	string					// String that is the location of the restart file to be read while restarting
	namesuf		string					// Suffix for all restart files
	resin		*os.File				// Restart file to br read while restarting
	sum		*os.File				// Summary file
	log		*os.File				// Log file
	res		*os.File				// Restart file that is written by current run
	// Search settings
	comp		string					// Worker complier file location
	prog		string					// Worker program file location
	head		[]int					// Common prefix that define root of tree
	hiwater		int					// High watermark for stored problems
	lowater		int					// Low watermark for stored problems
	wtime		int					// Time limit on workers
	ctime		int					// Time limit on commander
	opcap		int					// Estimated permutation per second frequency
	depth		int					// Minimum depth
	binr		int					// Cap on binary search -- right bound
	binl		int					// Floor of binary search -- left bound
	plen		int					// Dimension of arrays
	prng		int					// Upper bound of permutation values
	nwrk		int					// Number of threads
	ndfs		int					// Number of startup DFS threads
	// Output settings
	settings	int					// Settings integer; different bits represent different options
	mxSol		int					// Solution cap: program ends after mxSol solutions have been found
	// Hash
	table		map[string]int				// Table used during restart process
	fac		[]int					// Pre-cached factorial slice for estimating difficulty
	// Counters and flags
	maplock		*sync.Mutex				// Lock used for controlling access to assignment map
	reslock		*sync.Mutex				// Lock used for controlling access to commander run ID and worker return processing
	mutex		*sync.Mutex				// Lock used for controlling access to work queue by commander and dispatcher
	timer		time.Time				// Used to measure time elapsed since started for log info and to exit if time limit exceeded
	drawing		chan int				// Channel used to ensure that only one bar is drawn at a time
	started		bool					// Flag indicating whether search has begun
	mxFound		int					// Maximu solvable case for binary search
	start		int					// Count of active startup DFS threads
	gauge		int					// Size of gauge case
	wjobs		int					// Number of tasks for workers not completed
	done		int					// Number of tasks completed
	jobs		int					// Number of tasks not completed
	sols		int					// Total solutions found
	run		int					// Run ID of commander -- used for clients that frequently start a new search after completing an old one
}

/*** UTILITIES ***/

// Progress bar
func (c *commander) bar(d int, j int) {
	if c.settings&(1<<BarOut) == 0 || d+j == 0 || c.netCode == Server {
		return
	}
	var lck int
LockLoop:
	for {
		select {
		case lck = <-c.drawing:
			break LockLoop
		default:
			return
		}
	}
	if c.settings&(1<<LogOut) != 0 {
		fmt.Fprintf(c.log, "BarStart\t%d/%d\n", d, d+j)
	}
	fmt.Print("\r|")
	q := float64(d) / float64(d+j)
	for i := 1; i <= int(Bar*q); i++ {
		fmt.Print("#")
	}
	for i := int(Bar * q); i < Bar; i++ {
		fmt.Print("-")
	}
	fmt.Print("|\t", d, "/", d+j)
	if c.settings&(1<<LogOut) != 0 {
		fmt.Fprintf(c.log, "BarEnded\t%d/%d\n", d, d+j)
	}
	c.drawing <- lck
}

// Error handling utility
func (c *commander) hand(err error, s1 string, s2 string) bool {
	if err != nil {
		if s1 != "" {
			if c.settings&(1<<LogOut) != 0 {
				fmt.Fprintln(c.log, s1)
			}
			fmt.Println(s1)
		}
		return false
	}
	if s2 != "" {
		if c.settings&(1<<LogOut) != 0 {
			fmt.Fprintln(c.log, s2)
		}
		fmt.Println(s2)
	}
	return true
}

// Easier basics
func parseint(s string) int {
	t, _ := strconv.ParseInt(s, 10, 32)
	return int(t)
}
func parsefloat(s string) float64 {
	t, _ := strconv.ParseFloat(s, 64)
	return float64(t)
}
func max(i, j int) int {
	if i > j {
		return i
	}
	return j
}
func min(i, j int) int {
	if i > j {
		return j
	}
	return i
}

// List initialization
func newList(ret *returnInfo) *list.List {
	l := list.New()
	l.PushBack(ret)
	return l
}

// Slice to string
func toString(a []int) string {
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
func toSlice(s string) []int {
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
	if c.wtime == 0 {
		return MaxTime
	}
	if c.fac[c.plen-len(s)] > c.opcap*c.wtime {
		return 0
	}
	return c.wtime
}

/*** WORKER UNIT ***/

// Run clingo program on permutation
func (c *commander) runWorker(nxt []int, uid int, run int, time int) ([]byte, error) {
	// Open assignment file
	assignment := "assignments/costas.returnInfo_" + strconv.Itoa(uid) + ".data"
	file, ferr := os.Create(assignment)
	if ferr != nil {
		if c.settings&(1<<LogOut) != 0 {
			fmt.Fprintf(c.log, "ID %d %d: Error occurred while opening data assignment\n", uid, run)
		}
		fmt.Printf("ID %d %d: Error occurred while opening data assignment\n", uid, run)
	}
	// Write assignment
	for i, j := range nxt {
		fmt.Fprintf(file, "setting(%d, %d).\n", i+1, j)
	}
	file.Close()
	// Execute costas.clingo
	if c.settings&(1<<LogOut) != 0 {
		fmt.Fprintf(c.log, "Engaging\t%v\t\tID %d\t%d\n", nxt, uid, run)
	}
	comstr := c.comp
	if time != 0 {
		comstr += " --time-limit=" + strconv.Itoa(/*min(c.diff(nxt), */time)//)
	}
	comstr += " " + Params + "-n " + strconv.Itoa(max(c.mxSol - c.sols, 0)) + " -c m=" + strconv.Itoa(c.plen) + " -c k=" + strconv.Itoa(c.prng) + " " + assignment + " " + c.prog
	return exec.Command("bash", "-c", comstr).Output()
}

// Assign next task to worker
func assignUnit(uid int, com *commander, run int) {
	for {
		com.reslock.Lock()
		if run != com.run {
			com.reslock.Unlock()
			return
		}
		select {
		case nxt := <-com.dispatchChan:
			defer func() {
				r := recover()
				if r != nil && com.settings&(1<<LogOut) != 0 {
					fmt.Fprintf(com.log, "ID %d %d: Error occurred... recovering\n", uid, run)
				}
				com.reslock.Lock()
				if run == com.run {
					com.reslock.Unlock()
					com.reportChan <- returnInfo{nxt, 0, Killed, uid, 0, com.run}
				} else {
					com.reslock.Unlock()
					return
				}
			}()
			func() {
				com.reslock.Unlock()
				// Receive assignment and write to *.data file
				if com.settings&(1<<LogOut) != 0 {
					fmt.Fprintf(com.log, "Assigned\t%v\t\tID %d\t%d\n", nxt, uid, run)
				}
				// If assignment too difficult, skip computing
				if com.diff(nxt) == 0 {
					com.reportChan <- returnInfo{nxt, 0, "not attempted", uid, 0, run}
					return
				}
				// Compute and write to file
				out, err := com.runWorker(nxt, uid, run, com.wtime)
				if com.settings&(1<<LogOut) != 0 {
					fmt.Fprintf(com.log, "Computed\t%v\t\tID %d\t%d\t%q\n", nxt, uid, run, err)
				}
				postAssignment := "assignments/costas.post-processor_" + strconv.Itoa(uid) + ".in"
				postFile, pferr := os.Create(postAssignment)
				if pferr != nil {
					if com.settings&(1<<LogOut) != 0 {
						fmt.Fprintf(com.log, "ID %d %d: Error occurred while opening post-process assignment\n", uid, run)
					}
					fmt.Printf("ID %d %d: Error occurred while opening post-process assignment\n", uid, run)
				}
				fmt.Fprintf(postFile, string(out[:len(out)]))
				postFile.Close()
				// Run post-processor
				if com.settings&(1<<LogOut) != 0 {
					fmt.Fprintf(com.log, "Prcssing\t%v\t\tID %d\t%d\n", nxt, uid, run)
				}
				pst := exec.Command("bash", "-c", PostProcessor)
				stdin, _ := pst.StdinPipe()
				io.WriteString(stdin, postAssignment)
				stdin.Close()
				pstout, psterr := pst.Output()
				if com.settings&(1<<LogOut) != 0 {
					fmt.Fprintf(com.log, "PstPrcss\t%v\t\tID %d\t%d\n", nxt, uid, run)
				}
				var sol int
				if psterr != nil {
					if com.settings&(1<<LogOut) != 0 {
						fmt.Fprintf(com.log, "ID %d %d: Error occurred in post-processing\n", uid, run)
					}
					fmt.Printf("ID %d %d: Error occurred in post-processing\n", uid, run)
					sol = 0
				} else {
					t, _ := strconv.ParseInt(string(pstout[:len(pstout)-1]), 10, 32)
					sol = int(t)
				}
				// Run timer
				tmr := exec.Command("bash", "-c", Timer)
				tmrin, _ := tmr.StdinPipe()
				io.WriteString(tmrin, postAssignment)
				tmrin.Close()
				tmrout, _ := tmr.Output()
				if com.settings&(1<<LogOut) != 0 {
					fmt.Fprintf(com.log, "Finished\t%v\t\tID %d\t%d\n", nxt, uid, run)
				}
				tm, _ := strconv.ParseFloat(string(tmrout[:len(tmrout)-1]), 64)
				com.reslock.Lock()
				if run == com.run {
					com.reportChan <- returnInfo{nxt, float64(tm), err.Error(), uid, sol, run}
					com.reslock.Unlock()
				} else {
					com.reslock.Unlock()
					return
				}
			}()
		default:
			if run != com.run {
				com.reslock.Unlock()
				return
			}
			com.reslock.Unlock()
			time.Sleep(Pause * time.Millisecond)
		}
	}
}

/*** NETWORKING ***/

// Set port address
func (c *commander) acquire() bool {
	var err error
	c.targetIP, err = net.ResolveTCPAddr(Net, c.ipString)
	return c.hand(err, "Error acquiring target TCP/IP address: "+c.ipString, "Target TCP/IP address acquired: "+c.ipString)
}

// Repeatedly set address, prompting for new one on failure
func (c *commander) reaquire() {
	for !c.acquire() {
		fmt.Print("Enter new target TCP/IP address: ")
		reader := bufio.NewReader(os.Stdin)
		ln, _ := reader.ReadString('\n')
		c.ipString = ln[:len(ln)-1]
	}
}

// Create listener
func (c *commander) listen() bool {
	var err error
	c.TCPLis, err = net.ListenTCP(Net, c.targetIP)
	return c.hand(err, "Error creating TCPListener on port", "Listening to port")
}

// Open connection
func (c *commander) connect() *net.TCPConn {
	sock, err := net.DialTCP(Net, nil, c.targetIP)
	c.hand(err, "Error opening connection to target", "Opened connection to target")
	return sock
}

// Accept connection
func (c *commander) accept() *net.TCPConn {
	con, err := c.TCPLis.AcceptTCP()
	c.hand(err, "Error attempting connection", "Connected to "+con.RemoteAddr().String())
	return con
}

// Read from connection
func (c *commander) readbytes(con *net.TCPConn, n int) string {
	c.hand(nil, "", "Reading "+strconv.Itoa(n)+" bytes")
	r := make([]byte, n)
	_, err := con.Read(r)
	c.hand(err, "Error reading from connected port at "+con.RemoteAddr().String(), "Message ("+string(r)+") received from port at "+con.RemoteAddr().String())
	return string(r)
}
func (c *commander) read(con *net.TCPConn) string {
	ns := []byte(c.readbytes(con, MessageLengthBytes))
	n := 0
	for _, ch := range ns {
		n = n<<Byte + int(ch)
	}
	return c.readbytes(con, n)
}

// Write to connection
func (c *commander) writebytes(con *net.TCPConn, m string) bool {
	_, err := con.Write([]byte(m))
	return c.hand(err, "Error transmitting message ("+m+") to port at "+con.RemoteAddr().String(), "Transmitted message ("+m+") to port at "+con.RemoteAddr().String())
}
func (c *commander) write(con *net.TCPConn, m string) bool {
	n := len(m)
	s := ""
	for i := 0; i < MessageLengthBytes; i++ {
		//fmt.Println(n % MaxByte, byte(n % MaxByte))
		s = string(byte(n%MaxByte)) + s
		n /= MaxByte
	}
	return c.writebytes(con, s+m)
}

// Set up target ip and listening
func (c *commander) setUpNetwork() {
	if c.netCode == Offline {
		return
	}
	c.reaquire()
	if c.netCode == Server {
		if !c.listen() {
			c.setUpNetwork()
		}
	}
}

// Get work
func (c *commander) getWork() {
	con := c.connect()
	s := ""
	s += " -i " + strconv.Itoa(c.wid)
	c.write(con, RequestCode+s)
	sup := c.read(con)
	c.takeInput(sup)
	c.close(con)
}

// Report work
func (c *commander) report(tm float64, done bool) {
	con := c.connect()
	s := "1 -a "
	for _, n := range c.head {
		s += strconv.Itoa(n) + "-"
	}
	s = s[:len(s)-1]
	s += " -t " + strconv.FormatFloat(tm, 'f', -1, 64)
	s += " -s " + strconv.Itoa(c.sols)
	s += " -i " + strconv.Itoa(c.wid)
	s += " -e "
	if done {
		s += Success
	} else {
		s += Killed
	}
	c.write(con, s)
	c.close(con)
}

// Handle single request
func (c *commander) handleRequest(con *net.TCPConn) {
        s := c.read(con)
        if s == "" {
                c.close(con)
                return
        }
        set := strings.Fields(s)
        if set[0] == ReturnCode {
                func() {
			defer func() {
				r := recover()
				if r != nil && c.settings&(1<<LogOut) != 0 {
					fmt.Fprintf(c.log, "Request handler encountered error while processing work return, recovering...\n")
				}
				c.close(con)
			}()
                        w := c.takeOutput(s[2:])
                        c.reportChan <- w
                }()
        } else {
                func() {
			defer func() {
				r := recover()
				if r != nil && c.settings&(1<<LogOut) != 0 {
					fmt.Fprintf(c.log, "Request handler encountered error while processing work request, recovering...\n")
				}
				c.close(con)
			}()
                        req := c.takeRequest(s[2:])
                        fmt.Println("Preparing dispatch...")
                        a := <-c.dispatchChan
                        if c.settings&(1<<AssignmentMap) != 0 {
                                c.maplock.Lock()
                                l, ok := c.safetyHash[toString(a)]
				c.maplock.Unlock()
                                for ok && l == nil {
                                        if c.settings&(1<<LogOut) != 0 {
                                                fmt.Fprintf(c.log, "Dequeued\t%v\n", a)
                                        }
                                        a = <-c.dispatchChan
                                        c.maplock.Lock()
                                        l, ok = c.safetyHash[toString(a)]
					c.maplock.Unlock()
                                }
                        }
                        s := "-ml " + strconv.Itoa(c.prng) + " " + strconv.Itoa(c.plen) + " -T " + strconv.Itoa(c.wtime)
                        if req.wid == -1 {
                                s += " -i " + strconv.Itoa(c.nwrk)
                                req.wid = c.nwrk
                                c.nwrk++
                        }
                        if c.settings&(1<<AssignmentMap) != 0 {
                                c.maplock.Lock()
                                if c.settings&(1<<LogOut) != 0 {
				        fmt.Fprintf(c.log, "Inserted\tID %d\t%v\n", req.wid, a)
				}
		                l, ok := c.safetyHash[toString(a)]
	                        ret := &returnInfo{a, time.Now().Sub(c.timer).Seconds(), Unchecked, req.wid, 0, c.run}
                                if ok {
				        l.PushBack(ret)
				} else {
					c.safetyHash[toString(a)] = newList(ret)
				}
				c.maplock.Unlock()
                        }
                        s += " -H "
                        for _, n := range a {
                                s += strconv.Itoa(n) + "-"
                        }
                        c.write(con, s[:len(s)-1])
                }()
        }
}

// Catch all connection requests
func (c *commander) runService() {
	for {
		con := c.accept()
		go c.handleRequest(con)
	}
}

// Close connection
func (c *commander) close(con *net.TCPConn) {
	con.Close()
}

/*** COMMANDER FRAMEWORK ***/

// Central dispatcher - feeds queue to dispatch channel
func (c *commander) dispatcher() {
	for {
		c.mutex.Lock()
		if c.queue.Len() != 0 {
			uw := unwrap(c.queue.Front().Value)
			if c.settings&(1<<LogOut) != 0 {
				fmt.Fprintf(c.log, "Dispatch\t%v\n", uw)
			}
			c.queue.Remove(c.queue.Front())
			c.mutex.Unlock()
			c.dispatchChan <- uw
		} else {
			c.mutex.Unlock()
			time.Sleep(Pause * time.Millisecond)
		}
	}
}

// Commander constructor
func (c *commander) construct(m int, n int, tm int, ctm int, settings int, mxSol int, resin string, d int, head []int, s int, comp string, prog string, ipString string, netCode int, opcap int, gauge int) {
	// Network information
	c.ipString = ipString
	c.netCode = netCode
	c.wid = -1
	// Log files
	c.namesuf = ""
	c.resin, _ = os.Open(resin)
	c.sum, _ = os.Create("summary." + strconv.Itoa(netCode) + ".txt")
	c.log, _ = os.Create("log." + strconv.Itoa(netCode) + ".txt")
	// Assignment queue and com channels
	c.dispatchChan = make(chan []int, ChannelLength)
	c.startupChan = make(chan returnInfo, ChannelLength)
	c.reportChan = make(chan returnInfo, ChannelLength)
	c.queue = list.New()
	// Hash
	c.safetyHash = make(map[string]*list.List)
	c.table = make(map[string]int)
	c.fac = []int{}
	// Counters and flags
	c.drawing = make(chan int, 1)
	c.maplock = &sync.Mutex{}
	c.reslock = &sync.Mutex{}
	c.mutex = &sync.Mutex{}
	c.timer = time.Now()
	c.started = false
	c.drawing <- 1
	c.start = 0
	c.wjobs = 0
	c.done = 0
	c.jobs = 0
	c.sols = 0
	c.run = 0
	// Set parameters
	c.hiwater = HiMark * n
	c.lowater = LoMark * n
	c.settings = settings
	c.mxSol = mxSol
	c.opcap = opcap
	c.gauge = gauge
	c.binr = -1
	c.comp = comp
	c.prog = prog
	c.head = head
	c.ctime = ctm
	c.wtime = tm
	c.depth = d
	c.plen = 0
	c.prng = m
	c.nwrk = n
	c.ndfs = s
}
func defaultCom() commander {
	var c commander
	resin := "restart.txt"
	settings := Default
	wc := WorkerCompiler
	wp := WorkerProgram
	head := []int{}
	mxSol := Sol
	netCode := Offline
	ip := ""
	ct := T
	s := S
	t := T
	d := D
	m := -1
	n := N
	g := G
	c.construct(m, n, t, ct, settings, mxSol, resin, d, head, s, wc, wp, ip, netCode, PermutationsPerSecond, g)
	return c
}

// Approximate frequency of processors using simple run -- assume all processors the same
func (c *commander) gaugeWorker() bool {
	fmt.Print("Gauging worker frequency... ")
	tmr := time.Now()
	_, err := exec.Command("bash", "-c", c.comp + " -n 0 -c m=" + strconv.Itoa(c.gauge) + " " + c.prog).Output()
	diff := float64(time.Now().Sub(tmr).Seconds())
	c.opcap = int(float64(c.fac[c.gauge]) / diff)
	fmt.Print(string(27) + "[1A:\r\033[2K")
	if err != nil && (err.Error() == "exit status 30" || err.Error() == "exit status 20") {
		err = nil
	}
	return c.hand(err, "Error occurred while gauging worker frequency", "Gauged  \t" + strconv.Itoa(c.opcap))
}

// Backtracking check -- run clingo and backtrack if immediate 'inconsistent' return
func (c *commander) isValid(perm []int) bool {
	if c.settings&(1<<ClingoCheck) == 0 {
		return true
	}
	_, err := c.runWorker(perm, -1, c.run, 1)
	//fmt.Println(err.Error(), Inconsistent, err.Error() != Inconsistent)
	return err.Error() != Inconsistent
}

// Process result
func (c *commander) process(result returnInfo, run int) bool {

	// Log reception
	c.reslock.Lock()
	if c.run != run {
		c.reslock.Unlock()
		return false
	}
	if c.settings&(1<<LogOut) != 0 {
		fmt.Fprintf(c.log, "Received\t%v\t\tID %d\t%q\t%d\t%fs\t%d\n", result.assignment, result.uid, result.err, result.sol, result.time, c.jobs-1)
	}
	if c.settings&(1<<SummaryOut) != 0 && result.uid >= 0 {
		fmt.Fprintf(c.sum, "%v   \tID %d\t%q\t%d solution(s)\t%fs", result.assignment, result.uid, result.err, result.sol, result.time)
	}
	go c.bar(c.done, c.jobs)

	// Check validity of run
	if result.run != run {
		c.reslock.Unlock()
		return false
	}

	// Check validity of worker id
	if c.settings&(1<<AssignmentMap) != 0 && result.uid >= 0 {
		c.reslock.Unlock()
		c.maplock.Lock()
		if c.settings&(1<<LogOut) != 0 {
			fmt.Fprintf(c.log, "Checking\t%v\t\tID %d...\n", result.assignment, result.uid)
		}
		workerList, ok := c.safetyHash[toString(result.assignment)]
		if !ok || workerList == nil {
			if c.settings&(1<<AssignmentMap) != 0 {
				fmt.Fprint(c.log, "Checking Failed: No Assignment\n")
			}
			c.maplock.Unlock()
			return false
		}
		found := true
		for w := workerList.Front(); w != nil; w = w.Next() {
			if w.Value.(*returnInfo).uid == result.uid {
				found = true
				break
			}
		}
		if !found {
			if c.settings&(1<<LogOut) != 0 {
				fmt.Fprint(c.log, "Checking Failed: ID Mismatch\n")
                        }
			c.maplock.Unlock()
			return false
		}
		c.safetyHash[toString(result.assignment)] = nil
		c.maplock.Unlock()
		c.reslock.Lock()
	}

	// Begin restart file entry
        if c.settings&(1<<RestartOut) != 0 && (result.uid >= 0 || c.diff(result.assignment) == 0 || c.depth > len(result.assignment)) {
                fmt.Fprintf(c.res, "%d", len(result.assignment))
                for _, n := range result.assignment {
                        fmt.Fprintf(c.res, " %d", n)
                }
                fmt.Fprintf(c.res, " %d ", result.sol)
        }

	// Initial call
	if result.uid < 0 {
		if c.diff(result.assignment) == 0 || c.depth > len(result.assignment) {
			if c.settings&(1<<RestartOut) != 0 {
				fmt.Fprintf(c.res, "-1.0\n")
			}
			for i := 1; i <= c.prng; i++ {
				c.reslock.Unlock()
				if c.isValid(append(append([]int(nil), result.assignment...), i)) && (c.settings&(1<<Repeat) != 0 || !contains(result.assignment, i)) {
					c.reslock.Lock()
					c.jobs++
					c.reslock.Unlock()
					if c.start < c.ndfs {
						c.startupChan <- returnInfo{append(append([]int(nil), result.assignment...), i), 0, "initial assignment", -1, 0, run}
					} else {
						c.process(returnInfo{append(append([]int(nil), result.assignment...), i), 0, "initial assignment", -1, 0, run}, run)
					}
					c.reslock.Lock()
					if c.run != run {
						c.reslock.Unlock()
						return false
					}
				} else { c.reslock.Lock() }
			}
			c.jobs--
			c.done++
			if c.wjobs - c.nwrk >= c.hiwater {
				c.reslock.Unlock()
				for c.wjobs - c.nwrk > c.lowater {
					time.Sleep(Pause * time.Millisecond)
				}
				c.reslock.Lock()
			}
			c.reslock.Unlock()
		} else {
			c.wjobs++
			c.reslock.Unlock()
			c.mutex.Lock()
			c.queue.PushBack(append([]int(nil), result.assignment...))
			c.mutex.Unlock()
			if c.wjobs - c.nwrk >= c.hiwater {
				for c.wjobs - c.nwrk > c.lowater {
					time.Sleep(Pause * time.Millisecond)
				}
			}

		}
		return false
	}

	// Worker result
	if result.err != "exit status 10" && result.err != "exit status 30" && result.err != "exit status 20" && result.err != Success {
		if c.settings&(1<<SummaryOut) != 0 {
			fmt.Fprintf(c.sum, " (killed)\n")
		}
		if c.settings&(1<<RestartOut) != 0 {
			fmt.Fprintf(c.res, "-1.0\n")
		}
		for i := 1; i <= c.prng; i++ {
			if c.isValid(append(append([]int(nil), result.assignment...), i)) && (c.settings&(1<<Repeat) != 0 || !contains(result.assignment, i)) {
				c.reslock.Unlock()
				c.mutex.Lock()
				c.queue.PushBack(append(append([]int(nil), result.assignment...), i))
				c.mutex.Unlock()
				c.reslock.Lock()
				if c.run != run {
					c.reslock.Unlock()
					return false
				}
				if c.settings&(1<<LogOut) != 0 {
					fmt.Fprintf(c.log, "Enqueued\t%v\t\t%d\n", append(append([]int(nil), result.assignment...), i), c.jobs+1)
				}
				c.wjobs++
				c.jobs++
			}
		}
		c.reslock.Unlock()
	} else {
		if c.settings&(1<<SummaryOut) != 0 {
			fmt.Fprintf(c.sum, "\n")
		}
		if c.settings&(1<<RestartOut) != 0 {
			fmt.Fprintf(c.res, "%f\n", result.time)
		}
		c.sols += result.sol
		c.reslock.Unlock()
	}
	c.wjobs--
	c.jobs--
        c.done++
	if c.mxSol != 0 && c.sols >= c.mxSol {
		if c.settings&(1<<LogOut) != 0 {
			fmt.Fprintf(c.log, "Sounding Recall\n")
		}
		return true
	}
	return false
}

// Launch search
func (c *commander) launch() {

	// Outer loop
	for {

		// Clone permutation range to permutation length
                if c.plen == 0 {
                        c.plen = c.prng
                }

		// Binary step
		if c.binr >= 0 {
			c.plen = (c.binr + c.binl) / 2
		}

                // Fill fac and exp
                if len(c.fac) == 0 || c.binr < 0 {
			c.timer = time.Now()
		}
                c.fac = append(c.fac, 1)
		mx := max(max(c.plen, c.binr), c.gauge)
		for i := 1; i <= mx; i++ {
			val := i
			if c.settings&(1<<Repeat) != 0 {
				val = c.prng
			}
                        c.fac = append(c.fac, int(math.Min(float64(c.opcap*c.wtime+1), float64(c.fac[i-1]*val))))
                }
		if c.gauge > 0 && c.netCode != Server {
			c.gaugeWorker()
		}

		// Prepare network
		c.setUpNetwork()
		if c.netCode == Server {
			c.nwrk = 0
		} else {
			c.settings &= ^(1<<AssignmentMap)
		}
		if c.netCode == Client {
			c.getWork()
		}

                // Fill fac and exp
                c.timer = time.Now()
                c.fac = append(c.fac, 1)
		for i := mx + 1; i <= max(c.prng, c.binr); i++ {
			val := i
			if c.settings&(1<<Repeat) != 0 {
				val = c.prng
			}
                        c.fac = append(c.fac, int(math.Min(float64(c.opcap*c.wtime+1), float64(c.fac[i-1]*val))))
                }
		c.gauge = -1

		// Hash base cases
		c.table = make(map[string]int)
		for i := 1; i <= c.prng; i++ {
			if c.settings&(1<<Repeat) != 0 || !(contains(c.head, i)) {
				c.table[toString(append(c.head, i))] = MapSet
				if c.settings&(1<<LogOut) != 0 {
					fmt.Fprintf(c.log, "Reloaded\t[%v]\n", i)
				}
			}
		}

		// Restart
		c.resin, _ = os.Open(c.resinstr)
		if c.settings&(1<<RestartIn) != 0 {

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
				if c.settings&(1<<SummaryOut) != 0 {
					fmt.Fprintf(c.sum, "%v   \t   \t%q\t%d solution(s)", p, "loaded", sol)
					if kill < 0.0 {
						fmt.Fprintf(c.sum, " (killed)\n")
					} else {
						fmt.Fprintf(c.sum, "\n")
					}
				}

				// Delete from hash table
				delete(c.table, toString(p))

				// Enqueue children or add result
				if kill >= 0.0 {
					c.sols += int(sol)
				} else {
					for i := 1; i <= c.prng; i++ {
						if c.isValid(append(append([]int(nil), p...), i)) && (c.settings&(1<<Repeat) == 0 && contains(p, i)) {
							continue
						}
						if len(p) == int(c.nwrk) {
							p = append(p, 0)
						}
						p[n] = i
						if c.table[toString(p)] == 0 {
							c.table[toString(p)] = MapSet
						}
					}
				}

			}

			fmt.Print("\r\033[2K")

		}

		allDone := (len(c.table) == 0)

		// Queue hash
		go func() {
			for p := range c.table {
				c.jobs++
				c.startupChan <- returnInfo{toSlice(p), 0, "initial assignment", -1, 0, c.run}
				if c.settings&(1<<LogOut) != 0 {
					fmt.Fprintf(c.log, "Chnnling\t%v\t\t%d\n", toSlice(p), c.jobs+1)
				}
			}
			c.table = nil
		}()

		// All done
		if allDone {
			fmt.Println(c.sols, "SOLUTIONS")
			return
		}

		// Restart file
		if c.settings&(1<<RestartOut) != 0 {
			if c.settings&(1<<RestartIn) != 0 {
				c.res, _ = os.OpenFile(c.resin.Name(), os.O_APPEND|os.O_WRONLY, 0600)
			} else {
				c.res, _ = os.Create("restart." + strconv.Itoa(c.netCode) + c.namesuf + ".txt")
			}
		}

		// Launch the dispatcher
		go c.dispatcher()

		// First round of assignments
		if c.netCode == Server {
			go c.runService()
		} else {
			os.Mkdir("./assignments", 0755)
			for i := 0; i < c.nwrk; i++ {
				go assignUnit(i, c, c.run)
			}
		}
		c.started = false

		// Main loop
	Mainloop:
		for {
			select {
			case result := <-c.reportChan:
				if c.process(result, c.run) {
					break Mainloop
				}
			case result := <-c.startupChan:
				run := c.run
				go func() {
					for c.start >= c.ndfs {
						time.Sleep(Pause * time.Millisecond)
					}
					c.start++
					c.process(result, run)
					c.start--
				}()
			default:
				if c.ctime != 0 && time.Now().Sub(c.timer).Seconds() > float64(c.ctime) {
					break Mainloop
				}
				if c.jobs <= 0 {
					if c.started {
						if c.settings&(1<<LogOut) != 0 {
							fmt.Fprintf(c.log, "Sounding Recall\n")
						}
						break Mainloop
					}
				} else {
					c.started = true
				}
				if c.settings&(1<<AssignmentMap) != 0 {
					c.maplock.Lock()
					for k, v := range c.safetyHash {
						tmlim := 1.5 * float64(c.wtime)
						if tmlim == 0.0 {
							tmlim = MaxTime
						}
						if v != nil && v.Back().Value.(*returnInfo).err == Unchecked && v.Back().Value.(*returnInfo).time >= tmlim {
							if c.settings&(1<<LogOut) != 0 {
								fmt.Fprintf(c.log, "Reinsert\t%v\n", toSlice(k))
							}
							v.Back().Value.(*returnInfo).err = Checked
							c.mutex.Lock()
							c.queue.PushBack(toSlice(k))
							c.mutex.Unlock()
						}
					}
					c.maplock.Unlock()
				}
			}
		}

		c.reslock.Lock()
		c.run++
		c.reslock.Unlock()
		c.bar(c.done, c.jobs)
		lck := <-c.drawing
		fmt.Println()
		tm := time.Now().Sub(c.timer).Seconds()
		fmt.Println(c.binr)
		if c.binr < 0 {
			fmt.Println(c.sols, "SOLUTIONS")
			fmt.Printf("%fs\n", tm)
			if c.settings&(1<<LogOut) != 0 {
				fmt.Fprintf(c.log, "%d SOLUTIONS\n", c.sols)
				fmt.Fprintf(c.log, "%fs\n", tm)
			}
		}
		if c.netCode == Client {
			c.report(tm, c.jobs == 0)
			c.jobs = 0
			c.done = 0
			c.sols = 0
			c.drawing <- lck
		} else if c.binr >= 0 {
			m := (c.binl + c.binr) / 2
			fmt.Println(m)
			if c.sols != 0 {
				fmt.Println("l", m + 1)
				c.binl = m + 1
				c.mxFound = max(c.mxFound, m)
			} else {
				fmt.Println("r", m - 1)
				c.binr = m - 1
			}
			if c.binl > c.binr {
				m = c.mxFound
				fmt.Println("SOLUTION:", m)
				if c.settings&(1<<LogOut) != 0 {
					fmt.Fprintf(c.log, "SOLUTION: %d\n", m)
		                        fmt.Fprintf(c.log, "%fs\n", tm)
				}
				break
			}
			c.jobs = 0
                        c.done = 0
                        c.sols = 0
			c.drawing <- lck
		} else {
			break
		}
	}

	// Exit
	c.log.Close()
	c.sum.Close()

}

// Take output
func (cmdr *commander) takeOutput(line string) returnInfo {
	ri := returnInfo{[]int{}, 0, "", 0, 0, 0}
	foo1 := func(n *int, s string) {
		nn, _ := strconv.ParseInt(s[1:], 10, 32)
		*n = int(nn)
	}
	foo2 := func(v *string, s string) { *v = s[1:] }
	foo3 := func(f *float64, s string) { *f = parsefloat(s[1:]) }
	funi := map[rune]func(*int, string){'r': foo1, 's': foo1, 'i': foo1}
	argi := map[rune]*int{'r': &ri.run, 's': &ri.sol, 'i': &ri.uid}
	funs := map[rune]func(*string, string){'e': foo2}
	args := map[rune]*string{'e': &ri.err}
	funf := map[rune]func(*float64, string){'t': foo3}
	argf := map[rune]*float64{'t': &ri.time}
	cmdr.readInfo(line, funi, argi, funs, args, funf, argf, 'a', &ri.assignment)
	return ri
}

// Take request
func (cmdr *commander) takeRequest(line string) commander {
	c := defaultCom()
	c.netCode = Client
	c.takeInput(line)
	return c
}

// Take input
func (cmdr *commander) takeInput(line string) {
	foo1 := func(n *int, s string) {
		nn, _ := strconv.ParseInt(s[1:], 10, 32)
		*n = int(nn)
	}
	foo2 := func(n *int, s string) {
		var v int
		if s[0] == 'L' { v = LogOut } else { if s[0] == 'S' { v = SummaryOut } else { if s[0] == 'R' { v = RestartOut } else { if s[0] == 'B' { v = BarOut } else { if s[0] == 'J' { v = ClingoCheck } else { v = Repeat } } } } }
		if s[1:] == "0" {
			*n &= ^(int(1) << uint(v))
		} else {
			*n |= (int(1) << uint(v))
		}
	}
	foo3 := func(v *string, s string) { *v = s[1:] }
	funi := map[rune]func(*int, string){'n': foo1, 'm': foo1, 'l':foo1, 't': foo1, 'd': foo1, 's': foo1, 'D': foo1, 'T': foo1, 'L': foo2, 'S': foo2, 'R': foo2, 'B': foo2, 'N': foo1, 'i': foo1, 'p': foo1, 'g':foo1, 'P':foo2, 'b':foo1, 'f':foo1, 'J':foo2}
	argi := map[rune]*int{'n': &cmdr.nwrk, 'm': &cmdr.prng, 'l':&cmdr.plen, 't': &cmdr.wtime, 'd': &cmdr.depth, 'L': &cmdr.settings, 's': &cmdr.mxSol, 'D': &cmdr.ndfs, 'T': &cmdr.ctime, 'S': &cmdr.settings, 'R': &cmdr.settings, 'B': &cmdr.settings, 'N': &cmdr.netCode, 'i': &cmdr.wid, 'p': &cmdr.opcap, 'g':&cmdr.gauge, 'P':&cmdr.settings, 'b':&cmdr.binr, 'f':&cmdr.binl, 'J':&cmdr.settings}
	funs := map[rune]func(*string, string){'C': foo3, 'W': foo3, 'I': foo3, 'F':foo3}
	args := map[rune]*string{'C': &cmdr.comp, 'W': &cmdr.prog, 'I': &cmdr.ipString, 'F':&cmdr.namesuf}
	cmdr.head = []int{}
	cmdr.readInfo(line, funi, argi, funs, args, map[rune]func(*float64, string){}, map[rune]*float64{}, 'H', &cmdr.head)
}

// General info reader
func (cmdr *commander) readInfo(line string, funi map[rune]func(*int, string), argi map[rune]*int, funs map[rune]func(*string, string), args map[rune]*string, funf map[rune]func(*float64, string), argf map[rune]*float64, lch rune, arr *[]int) {

	com := []rune{}
	set := strings.Fields(line)

	for _, s := range set {
		if s[0] == '-' && s[1] != '1' {
			for _, c := range s[1:] {
				com = append(com, c)
			}
		} else if len(com) > 0 {
			c := com[0]
			com = com[1:]
			if _, ok := argi[c]; ok {
				funi[c](argi[c], string(c)+s)
			} else if _, ok := args[c]; ok {
				funs[c](args[c], string(c)+s)
			} else if _, ok := argf[c]; ok {
				funf[c](argf[c], string(c)+s)
			} else if c == 'r' {
				cmdr.settings |= (1 << RestartIn)
				cmdr.resinstr = s
			} else if c == lch {
				hd := strings.FieldsFunc(s, func(r rune) bool { return r == ':' || r == '-' || r == '_' })
				for _, e := range hd {
					i, _ := strconv.ParseInt(e, 10, 32)
					*arr = append(*arr, int(i))
				}
			} else {
				fmt.Printf("Command not recognized: -%s\n", c)
			}
		}
	}

	if cmdr.prng < 1 && cmdr.netCode != Client && cmdr.binr < 0 {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print(string(27) + "[1A:\r\033[2KM: ")
		mln, _ := reader.ReadString('\n')
		mm, _ := strconv.ParseInt(mln[:len(mln)-1], 10, 32)
		cmdr.plen = int(mm)
	}

}

// Take input and launch commander unit
func main() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Special settings: ")
	line, _ := reader.ReadString('\n')
	c := defaultCom()
	c.takeInput(line)
	c.log, _ = os.Create("log." + strconv.Itoa(c.netCode) + c.namesuf + ".txt")
	c.sum, _ = os.Create("summary." + strconv.Itoa(c.netCode) + c.namesuf + ".txt")
	fmt.Print(string(27) + "[1A:\r\033[2K")
	c.launch()
	c.res.Close()
}
