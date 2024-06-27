package main

import (
	"./packages/concurrent"
	"./packages/server"
	"./packages/check"
	"./packages/util"
	"sync/atomic"
	"io/ioutil"
	"os/exec"
	"strconv"
	"strings"
	"unsafe"
	"errors"
	"bufio"
	"math"
	"time"
	"fmt"
	"net"
	"os"
	"io"
)

/*** CONSTANTS ***/

/*** PROGRAM LOCATIONS ***/
// NAME is the name of the commander version.
const NAME = "commander.3.0.0"
// Params is the set of additional parameters passed to clingo
const Params = "-q "
// Postprocessor string
const PostProcessor = "perl perl/post.clingo.short.pl"
// WorkerCompiler is the address of the clingo compiler.
const WorkerCompiler = "clingo"
// WorkerProg is the address of the clingo worker unit.
const WorkerProg = "clingo/costas.clingo"
// ProbName is the name of the problem being solved
const ProbName = "costas"
// LogDir is the log file directory.
const LogDir = "logs/"

/*** SETTINGS CONSTANTS ***/
// Verbose is the bit representing whether a commander is verbose.
const Verbose = 0
// SelectivelyVerbose is the bit representing whether a commander is verbose for select messages (internet, errors).
const SelectivelyVerbose = 1
// LogOut is the bit representing whether a commander prints to the log file.
const LogOut = 2
// SummaryOut is the bit representing whether a commander prints to the summary file.
const SummaryOut = 3
// RestartOut is the bit representing whether a commander prints to the restart file.
const RestartOut = 4
// Appending is the bit representing whether a commander appends to the log and restart files.
const Appending = 5
// AllowingRepeats is the bit representing whether repeats are allowed in permutations.
const AllowingRepeats = 6
// DrawingBar is the bit representing whether the progress bar is to be drawn.
const DrawingBar = 7
// BarToFile is the bit representing whether the progress bar is copied to bar.out
const BarToFile = 8
// DefSet is the defaulOBt settings integer.
const DefSet = (1<<9)-2
// NoCheck is the check code of a program that does not employ a consistency check in the DFS.
const NoCheck = 0
// ClingoCheck is the check code of a program that employs a 1-second clingo consistency check.
const ClingoCheck = 1
// GoCheck is the check code of a program that employs a custom golang consistency check from package ./packages/check.
const GoCheck = 2

/*** RETURN CODES ***/
// Killed is the code returned by workers that did not finish their tasks.
const Killed = NAME + "-worker-killed"
// Success is the code returned by internet workers.
const Success = NAME + "-TCP/IP-worker-done"
// Init is the code of an initial assignment.
const Init = NAME + "-collector-DFS"
// ComKey is the prefix a communication must possess to be accepted by the commander server.
const ComKey = NAME + "-TCP/IP-connect-request"
// LimHit is the code returned by a worker if the solution limit has been reached.
const LimHit = NAME + "-solution-limit-reached"
// MaxDeep is the code returned by a collector if it has received a full-length consistent assignment.
const MaxDeep = NAME + "-maximum-depth-reached"
// Inconsistent is the return code of inconsistent clingo worker.
const Inconsistent = "exit status 20"
// RequestCode is the code sent by a subcommander to request an assignment.
const RequestCode = "0"
// ReturnCode is the code sent by a subcommander to return an assignment.
const ReturnCode = "1"
// MapSet is the value of a section of the restart tree still needing completion.
const MapSet = 1

/*** DEFAULT SETTINGS ***/
// OpCap is the initial estimated worker frequency.
const OpCap = 10000000
// Pause is the amount of time for which threads pause when necessary.
const Pause = 250
// LPause is the amount of time for which threads take a long pause when waiting for triggers.
const LPause = 1000
// BarLen is the default length of the progress bars.
const BarLen = 50
// HiMark is the high water mark in terms of number of workers.
const HiMark = 10
// LoMark is the low water mark in terms of number of workers.
const LoMark = 2
// Sol is the default solution cap (0 indicates finding all solutions).
const Sol = 0
// Gauge is the default size of a gauge case.
const Gauge = 10
// T is the default time limit for threads (0 indicates no limit).
const T = 0
// D is the default minimum depth of recursion.
const D = 0
// N is the default number of worker threads.
const N = 0
// S is the default number of startup DFS threads
const S = 1

/*** COMMANDER STRUCTURE ***/

// Commander structure encapsulates all of the information parallel process need to communicate for distribution of work and collection of data.
type Commander struct {

	// ipString stores the target IP address as a string.
	ipString	string
	// targetIP stores a pointer to the TCPAddr once it is acquired.
	targetIP	*net.TCPAddr
	// tcpLis stores a pointer the TCPListener of a Server commander.
	tcpLis	*net.TCPListener
	// netCode indicates the type of internet commander: offline, subcommander, or server.
	netCode	int
	// sub_id stores the id of a subcommander.
	sub_id	int

	// safetyHash maps each assignment to the IDs of subcommanders to which it has been assigned.
	safetyHash	*concurrent.MultiMap
	// dispatchStack is a ConcurrentStack of dispatched assignments.
	dispatchStack	concurrent.ConcurrentCollection
	// reportStack is a ConcurrentStack of initial assignments and worker reports for DFS traversal.
	reportStack	concurrent.ConcurrentCollection
	// restartTable	is a map used in the restart process.
	restartTable	map[string]int
	// recall is a bool used to terminate all threads before restart.
	recall	bool
	// begin is a bool used to issue a command for units to begin work once restarting is complete.
	begin	bool

	// resinstr is the name of the restart file from which the commander will restart.
	resinstr	string
	// namesuf is the suffix with which to end file names.
	namesuf		string
	// resin is the file from which the commander reads restart data.
	resin	*os.File
	// summary is a human-readable summary file of solution counts in each explored leaf of the search tree.
	summary	*os.File
	// log is a human-readable log file of a step-by-step documentation of operations performed by the program.
	log	*os.File
	// ilog is a human-readable log file of a step-by-step documentation of internet-related operations performed by the program.
	ilog	*os.File
	// elog is a human-readable log file of errors encountered by the program
	elog	*os.File
	// res is a restart file that the commander can use to restart an incomplete search.
	res *os.File
	// logger is a util.Logger wrapper for the log file
	logger	*util.Logger
	// elogger is a util.Logger wrapper for the errlog file
	elogger	*util.Logger
	// ilogger is a util.Logger wrapper for the log file to log internet communication
	ilogger *util.Logger
	// rlogger is a util.Logger wrapper for the restart file
	rlogger *util.Logger
	// slogger is a util.Logger wrapper for the summary file
	slogger *util.Logger
	
	// comp is the location of the worker compiler.
	comp	string
	// prog is the location of the worker program.
	prog	string
	//  is the common prefix that defines the root of the tree.
	head	[]int
	// hiwater is an upper watermark for lazy queueing.
	hiwater	int
	// lowater is a low watermark for lazy queueing.
	lowater int
	// wtime is the time limit on the workers.
	wtime	int
	// ctime is the time limit on the commander itself.
	ctime	int
	// opcap is the estimated worker frequency.
	opcap	int
	// depth is the minimim depth of the tree to which a search must descend.
	depth	int
	// tdepth is the minimum depth of the tree at which the projected difficulty of assignments is not greater that wtime.
	tdepth	int
	// check is the checking code of the commander: 0 for no checking, 1 for clingo checking, and 2 for packaged checking.
	check	int
	// binr is the upper bound of a binary search.
	binr	int
	// binl is the lower bound of a binary dearch.
	binl	int
	// plen is the permutation length for the problem.
	plen	int
	// prng is the highest value in the range that is in the domain of the variables.
	prng	int
	// nwk is the number of cpu threads utilized.
	nwrk	int
	// ndfs is the number of startup threads utilized.
	ndfs	int
	// settings is an integer, the digits of which represent boolean setting flags.
	settings	int
	// mxsol is an integer, the number of solutions to be found. The commander terminates after finding mxsol solutions.
	mxsol	int
	// fac is a slice of cached factorials or exponents for guessing difficulty
	fac	[]float64
	// alldone is a flag that stores whether the restart file input described a complete search tree.
	alldone	bool
	
	// wjobs is an integer, the total number of jobs queued for workers. Use the atomic util.Inc to increment.
	wjobs	uint64
	// wdone is an integer, the total number of jobs queued for workers. Use the atomic util.Inc to increment.
	wdone	uint64
	// sols is an integer, the number of solutions found so far. Use the atomic util.Inc to increment.
	sols	uint64
	// jobs is an integer, the total number of jobs queued. Use the atomic util.Inc to increment.
	jobs	uint64
	// done is an integer, the total number of queued jobs completed. Use the atomic util.Inc to increment.
	done	uint64
	// dropped is an integer, the number of threads that have terminated due to the recall message. Use the atomic util.Inc to increment.
	dropped	uint64
	// started is an integer, the number of threads that have begun work due to the start message. Use the atomic util.Inc to increment.
	started	uint64
	// nsub is an integer, the number of subcommander connections. Use the atomic util.Inc to increment.
	nsub	uint64
	
	// run is an integer, the run id of the commander.
	run	uint64
	
	// timer is used to determine the clocktime since start for logging and time limit purposes.
	timer	time.Time
	// mxFound represents the maximum solvable case of a binary search.
	mxFound	int
	// gauge is the size of the inital worker run that gauges processor frequency.
	gauge	int

}

/*** ASSIGNMENT INFO STRUCTURE ***/
type AssignmentInfo struct {
	// assignment is an []int slice that stores the prefix defining the sub-tree.
	assignment []int
	// time is the time elapsed during completion in seconds
	time       float64
	// err is the error returned by worker
	err        string
	// wid is the id of the worker
	wid        int
	// sol is the number of solutions found
	sol        int
	// run is the id of the commander run associated with this assignment
	run        uint64
}

/*** SETTINGS FLAGS ***/

// Is returns true if and only if the desired settings bit is set to 1.
func (c *Commander) Is(set int) bool { return (*c).settings&(1<<uint(set)) != 0; }

/*** SATISFY COMMUNICATOR ***/

// Override Log()
func (c *Commander) ILog() *util.Logger { return (*c).ilogger; }
func (c *Commander) ELog() *util.Logger { return (*c).elogger; }
func (c *Commander) Res() *util.Logger { return (*c).rlogger; }
func (c *Commander) Sum() *util.Logger { return (*c).slogger; }
func (c *Commander) Log() *util.Logger { return (*c).logger; }

// Override Communicator methods
func (c *Commander) TargetIP() **net.TCPAddr { return &c.targetIP }
func (c *Commander) IPStr() *string { return &c.ipString }
func (c *Commander) NetCode() int { return c.netCode }
func (c *Commander) Key() string { return ComKey }

// Override Server methods
func (c *Commander) Lis() **net.TCPListener { return &c.tcpLis }

// GetWork sends a request for work to the server commander.
func (c *Commander) GetWork() {
	con := server.Connect(c)
	s := ""
	s += " -i " + strconv.Itoa(c.sub_id)
	server.Write(c, con, RequestCode + s)
	sup, _ := server.Read(c, con)
	c.TakeInput(sup)
	con.Close()
}

// Report reports a completed assignment to the server commander.
func (c *Commander) Report(tm float64, done bool) {
	con := server.Connect(c)
	s := "1 -a "
	for _, n := range c.head {
		s += strconv.Itoa(n) + "-"
	}
	s = s[:len(s)-1]
	s += " -t " + strconv.FormatFloat(tm, 'f', -1, 64)
	s += " -s " + strconv.Itoa(int(c.sols))
	s += " -i " + strconv.Itoa(c.sub_id)
	s += " -e "
	if done {
		s += Success
	} else {
		s += Killed
	}
	server.Write(c, con, s)
	con.Close()
}

// HandleRequest handles a single connection request from a subcommander.
func (c *Commander) HandleRequest(con *net.TCPConn) {
	s, err := server.Read(c, con)
	if s == "" || err != nil {
		con.Close()
		return
	}
	set := strings.Fields(s)
	if set[0] == ReturnCode {
		defer func() {
			r := recover()
			if r != nil {
				util.Write(c.ELog(), "Request handler encountered error while processing work return, recovering...\n")
				con.Close()
			}
		}()
		func() {
			w := c.TakeOutput(s[2:])
			concurrent.Push(c.reportStack, *w)
			util.Inc(&c.wdone)
		}()
	} else {
		defer func() {
			r := recover()
			if r != nil {
				util.Write(c.ELog(), "Request handler encountered error while processing work return, recovering...\n")
				con.Close()
			}
		}()
		func() {
			req := c.takeRequest(s[2:])
			util.Write(c.ILog(), "Preparing dispatch...\n")
			var a AssignmentInfo
			ok := false
			val := concurrent.Pop(c.dispatchStack)
			for !ok {
				ok = true
				if val == nil {
					ok = false
				} else {
					a = val.(AssignmentInfo)
				}
			}
			s := "-ml " + strconv.Itoa(c.prng) + " " + strconv.Itoa(c.plen) + " -T " + strconv.Itoa(c.wtime)
			if req.sub_id == -1 {
				s += " -i " + strconv.Itoa(c.nwrk)
				req.sub_id = int(c.nsub)
				util.Inc(&c.nsub)
			}
			c.safetyHash.Insert(util.ToString(a.assignment), req.sub_id)
			s += " -H "
			for _, n := range a.assignment {
				s += strconv.Itoa(n) + "-"
			}
			server.Write(c, con, s[:len(s)-1])
		}()
	}
}

/*** SAFE RESTART UTILITIES ***/

// WaitToBegin sleeps in intervals of LPause ms unit c.begin is true.
// WaitToBegin prints s and increments c.started before it terminates.
func (c *Commander) WaitToBegin(s string) {
	for !c.begin {
		time.Sleep(LPause * time.Millisecond)
	}
	util.Write(c.Log(), s)
	util.Inc(&c.started)
}

// CheckForRecall returns true if c.recall is true.
// CheckForRecall prints s and increments c.dropped if c.recall is true.
// Immediately terminate thread if CheckForRecall returns true!
func (c *Commander) CheckForRecall(s string) bool {
	if c.recall {
		util.Write(c.Log(), s)
		util.Inc(&c.dropped)
		return true
	}
	return false
}

/*** VALIDITY CHECK ***/

// IsValid returns false is the argument contradicts the constraints. It returns true otherwise.
func (c *Commander) IsValid(prefix []int) bool {
	if c.check == ClingoCheck {
		_, err := c.RunWorker(prefix, -1, 1)
		return err.Error() != Inconsistent
	} else if c.check == GoCheck {
		return check.IsConsistent(prefix, ProbName)
	}
	return true
}

// Diff returns an integer value corresponding to the estimated number of seconds required to complete the given assignment.
func (c *Commander) Diff (prefix []int) int {
	if c.fac[c.plen - len(prefix)] > float64(c.opcap * c.wtime) {
		return 0
	}
	return int(c.fac[c.plen - len(prefix)])
}

/*** WORKER UNIT ***/

// RunWorker runs the worker program for the argument permutation.
func (c *Commander) RunWorker(prefix []int, wid, time int) ([]byte, error) {
	// Open assignment file
	assignment := "assignments/" + NAME + ".assignment." + strconv.Itoa(wid) + ".data"
	file, ferr := os.Create(assignment)
	if ferr != nil {
		go util.Write(c.ELog(), "WID %d:\tError occurred while opening data assignment\n", wid)
	}
	// Write assignment
	for i, j := range prefix {
		fmt.Fprintf(file, "setting(%d, %d).\n", i+1, j)
	}
	file.Close()
	// Execute costas.clingo
	if c.Is(LogOut) {
		go util.Write(c.Log(), "Engaging\t%v\t\tWID %d\n", prefix, wid)
	}
	comstr := c.comp
	if time != 0 {
		comstr += " --time-limit=" + strconv.Itoa(time)
	}
	nsol := util.Max(c.mxsol - int(c.sols), 0)
	if nsol == 0 && c.mxsol != 0 {
		return nil, errors.New(LimHit)
	}
	comstr += " " + Params + "-n " + strconv.Itoa(nsol) + " -c m=" + strconv.Itoa(c.plen) + " -c k=" + strconv.Itoa(c.prng) + " " + assignment + " " + c.prog
	return exec.Command("bash", "-c", comstr).Output()
}

// Worker is a routine that performs the tasks of a single worker thread.
// Worker reads assignments from the dispatchStack, runs those assignments, postprocesses them, and queues the output to reportQueue.
func (c *Commander) Worker(wid int) {

	/// Wait for begin message
	c.WaitToBegin(fmt.Sprintf("Starting\tWID %d\n", wid))

	/// Work loop
	for {

		/// Check for recall
		if c.CheckForRecall(fmt.Sprintf("Stopping\tWID %d\n", wid)) {
			return
		}
		
		/// Receive assignment from dispatch queue
		var nxt AssignmentInfo
		ok := true
		defer func() {
			if r := recover(); r != nil {
				util.Write(c.ELog(), "WID %d:\tError accessing dispatch queue... recovering\n", wid)
				ok = false
			}
		}()
		func() {
			val := concurrent.Pop(c.dispatchStack)
			if val == nil {
				ok = false
			} else {
				nxt = val.(AssignmentInfo)
			}
		}()
		if !ok {
			time.Sleep(Pause * time.Millisecond)
			continue
		}

		/// Process assignment
		defer func() {
			if r := recover(); r != nil {
				util.Write(c.ELog(), "WID %d:\tError processing assignment... reporting failure and recovering\n", wid)
				concurrent.Push(c.reportStack, AssignmentInfo{nxt.assignment, 0, Killed, wid, 0, nxt.run})
			}
		}()
		func() {
			/// Log reception of assignment
			util.Write(c.Log(), "Assigned\t%v\t\tWID %d\n", nxt.assignment, wid)
			/// Check run
			if nxt.run != c.run {
				util.Write(c.Log(), "Rejected\t%v\t\tWID %d\t(old run: %d)\n", nxt.assignment, wid, nxt.run)
				return
			}
			/// Compute and write to post-process file
			out, err := c.RunWorker(nxt.assignment, wid, c.wtime)
			if err.Error() == LimHit {
				util.Write(c.Log(), "Rejected\t%v\t\tWID %d\t(solution limit reached: %d/%d)\n", nxt.assignment, wid, c.sols, c.mxsol)
			}
			util.Write(c.Log(), "Computed\t%v\t\tWID %d\t%q\n", nxt.assignment, wid, err)
			postAssignment := "assignments/costas.post-processor_" + strconv.Itoa(wid) + ".in"
			postFile, pferr := os.Create(postAssignment)
			if pferr != nil {
				util.Write(c.ELog(), "WID %d:\tError occurred while opening post-process assignment\n", wid)
			}
			fmt.Fprintf(postFile, string(out[:len(out)]))
			postFile.Close()
			/// Run post-processor
			util.Write(c.Log(), "Prcssing\t%v\t\tWID %d\n", nxt.assignment, wid)
			pst := exec.Command("bash", "-c", PostProcessor)
			stdin, _ := pst.StdinPipe()
			io.WriteString(stdin, postAssignment)
			stdin.Close()
			pstout, psterr := pst.Output()
			util.Write(c.Log(), "PstPrcss\t%v\t\tWID %d\n", nxt.assignment, wid)
			/// Parse post-processor ourput
			var sol int
			var tm float64
			if psterr != nil {
				util.Write(c.ELog(), "WID %d:\tError occurred in post-processing\n", wid)
				sol = 0
			} else {
				pstr := strings.Fields(string(pstout[:len(pstout)-1]))
				sol = util.ParseInt(pstr[0])
				tm = util.ParseFloat(pstr[1])
			}
			/// Return results
			util.Write(c.Log(), "Finished\t%v\t\tWID %d\n", nxt.assignment, wid)
			concurrent.Push(c.reportStack, AssignmentInfo{nxt.assignment, tm, err.Error(), wid, sol, nxt.run})
			util.Inc(&c.wdone)
		}()

	}

}

/*** COLLECTOR UNIT ***/

// GoodExit returns true if the argument is an exit status that indicates a completed assignment. It returns false otherwise.
func (c *Commander) GoodExit(ex string) bool {
	if ex == "exit status 10" || ex == "exit status 30" || ex == "exit status 20" || ex == Success || ex == MaxDeep {
		return true
	}
	return false
}

// FinishedExit returns true if the argument is an exit status that indicates an assignment that does not need to be requequed. It returns false otherwise.
func (c *Commander) FinishedExit(ex string) bool {
	if c.GoodExit(ex) || ex == LimHit {
		return true
	}
	return false
}

// CleanExit returns true if the argument is an exit status that indicates an assignment that does not need to be expanded. It returns false otherwise.
func (c *Commander) CleanExit(ex string) bool {
	if c.FinishedExit(ex) || ex == Init {
		return true
	}
	return false
}

// Collector is a routine that processes initial assignments and the information returned by workers.
// Collector pushes assignments for workers to complete to the dispatchStack.
func (c *Commander) Collector(cid int) {

	/// Wait for begin message
	c.WaitToBegin(fmt.Sprintf("Starting\tCID %d\n", cid))

	/// Collection loop
	for {

		/// Check for recall
		if c.CheckForRecall(fmt.Sprintf("Stopping\tCID %d\n", cid)) {
			return
		}

		/// Receive information from report stack
		var nxt AssignmentInfo
		ok := true
		/*defer func() {
			if r := recover(); r != nil {
				util.Write(c.ELog(), "CID %d:\tError accessing report stack... recovering\n", cid)
				ok = false
			}
		}()*/
		func() {
			val := concurrent.Pop(c.reportStack)
			if val == nil {
				ok = false
			} else {
				nxt = val.(AssignmentInfo)
			}
		}()
		if !ok {
			time.Sleep(Pause * time.Millisecond)
			continue
		}

		/// Log reception of information
		util.Write(c.Log(), "Received\t%v\t\tCID %d\t%q\n", nxt.assignment, cid, nxt.err)

		/// A consistent full-length assignment is a solution.
		if nxt.err == Init && len(nxt.assignment) == c.plen && c.check != NoCheck {
			concurrent.Push(c.reportStack, AssignmentInfo{ append([]int(nil), nxt.assignment...), 0, MaxDeep, -1, 1, nxt.run })
			continue
		}

		/// Check watermarks
		if nxt.err == Init && c.wjobs - c.wdone > uint64(c.hiwater) {
			for c.wjobs - c.wdone > uint64(c.lowater) {
				time.Sleep(Pause * time.Millisecond)
				/// Check for recall
				if c.CheckForRecall(fmt.Sprintf("Stopping\tCID %d\n", cid)) {
					return
				}
			}
		}

		/// Process report
		/*defer func() {
			if r := recover(); r != nil {
				util.Write(c.ELog(), "CID %d:\tError processing worker report... re-pushing and recovering\n", cid)
				concurrent.Push(c.reportStack, nxt)
			}
		}()*/
		func() {
			/// Check validity of run
			if uint64(nxt.run) != c.run {
				return
			}
			/// Check validity of worker id
			if c.NetCode() == server.ServerCode {
				if !c.safetyHash.Contains(util.ToString(nxt.assignment), nxt.wid, false, true) && nxt.err != Init {
					c.safetyHash.MapLock.Unlock()
					return
				}
				c.safetyHash.Erase(util.ToString(nxt.assignment), true)
				fmt.Println("GOOD")
			}
			/// Add information to commander totals
			sum := "%v\t\tWID %d\t%q\t%d solution(s)\t%fs\t"
			if c.GoodExit(nxt.err) {
				atomic.AddUint64(&c.sols, uint64(nxt.sol))
			} else {
				nxt.sol = -1
				sum += "(killed)"
			}
			util.Write(c.Log(), "Updating\t%v\t\tCID %d\t%d(+%d)\n", nxt.assignment, cid, c.sols, util.Max(nxt.sol, 0))
			/// Make restart and summary file entries
			res := strconv.Itoa(len(nxt.assignment))
			for _, n := range(nxt.assignment) {
				res += " " + strconv.Itoa(n)
			}
			res += " " + strconv.Itoa(nxt.sol) + fmt.Sprintf(" %f\n", nxt.time)
			/// Push children to work queue or report stack
			if len(nxt.assignment) < c.plen && (!c.CleanExit(nxt.err) || (c.wtime != 0 && c.Diff(nxt.assignment) == 0) || c.depth > len(nxt.assignment)) {
				util.Write(c.Res(), res)
				util.Write(c.Sum(), sum + "\n", nxt.assignment, nxt.wid, nxt.err, nxt.sol, nxt.time)
				for i := 1; i <= c.prng; i++ {
					if !(!c.Is(AllowingRepeats) && util.Contains(nxt.assignment, i)) && c.IsValid(append(append([]int(nil), nxt.assignment...), i)) {
						concurrent.Push(c.reportStack, AssignmentInfo{ append(append([]int(nil), nxt.assignment...), i), 0, Init, -1, -1, nxt.run })
						util.Inc(&c.jobs)
					}
				}
				util.Inc(&c.done)
			} else if !c.FinishedExit(nxt.err) {
				concurrent.Push(c.dispatchStack, AssignmentInfo{ append([]int(nil), nxt.assignment...), -1, "-1", -1, -1, c.run })
				util.Write(c.Log(), "Dispatch\t%v\t\tCID %d\t%d\n", nxt.assignment, cid, c.jobs)
				util.Inc(&c.wjobs)
			} else {
				util.Write(c.Res(), res)
				util.Write(c.Sum(), sum + "\n", nxt.assignment, nxt.wid, nxt.err, nxt.sol, nxt.time)
				util.Inc(&c.done)
			}
		}()

	}

}

/*** BAR DRAWING UNIT ***/

// BarDraw is a routine that draws and updates a progress bar.
func (c *Commander) BarDraw() {

	/// Wait for begin message
	c.WaitToBegin(fmt.Sprintf("Starting\tBar Drawing Unit\n"))

	/// Bar length values
	wd := -1
	wj := -1
	d := -1
	j := -1
	if c.Is(DrawingBar) { fmt.Println("\n") }

	/// Drawing loop
	for {
		/// Check settings
		for !c.Is(DrawingBar) {
			time.Sleep(LPause * time.Millisecond)
			/// Check for recall
			if c.CheckForRecall(fmt.Sprintf("Stopping\tBar Drawing Unit\n")) {
				return
			}
		}
		/// Only draw if there has been a change
		for d == int(c.done) && j == int(c.jobs) && wd == int(c.wdone) && wj == int(c.wjobs) {
			time.Sleep(Pause * time.Millisecond)
			if c.recall {
				break
			}
		}
		/// Draw bar
		d = int(c.done); j = int(c.jobs); wd = int(c.wdone); wj = int(c.wjobs)
		g := math.Max(float64(j), c.fac[c.plen] / c.fac[c.plen - util.Max(c.depth - 1, c.tdepth)])
		q := math.Min(math.Max(float64(d) / g, 0), 1)
		if d == 0 {
			q = 0
		}
		util.Write(c.Log(), "BarStart\t%d/%d\n", d, j)
		out := strings.Repeat(string(27) + "[1A:\r\033[2K", 3) + "║" + strings.Repeat("▒", int(BarLen * q)) + strings.Repeat("░", BarLen - int(BarLen * q)) + "║\t"
		out += strconv.Itoa(d) + "/" + fmt.Sprintf("%.0f", g) + "?\n"
		q = math.Min(math.Max(float64(d) / float64(j), 0), 1)
		if d == 0 && j == 0 {
			q = 0
		}
		out += "║" + strings.Repeat("▒", int(BarLen * q)) + strings.Repeat("░", BarLen - int(BarLen * q)) + "║\t"
		out += strconv.Itoa(d) + "/" + strconv.Itoa(j) + "\n"
		q = math.Min(math.Max(float64(wj - wd) / float64(c.hiwater + c.lowater), 0), 1)
		tout := "║" + strings.Repeat("▒", int(BarLen * q)) + strings.Repeat("░", BarLen - int(BarLen * q)) + "║\t"
		tout = util.Replace(tout, '▓', BarLen * c.lowater / (c.hiwater + c.lowater) + 1)
		tout = util.Replace(tout, '▓', BarLen * c.hiwater / (c.hiwater + c.lowater) + 1)
		out += tout + strconv.Itoa(c.lowater) + "|" + strconv.Itoa(wj - wd) + "\t|" + strconv.Itoa(c.hiwater) + "\n"
		fmt.Print(out)
		if c.Is(BarToFile) {
			ioutil.WriteFile("bar.out", []byte(out), 0644)
		}
		util.Write(c.Log(), "BarEnded\t%d/%d\n", d, j)
		/// Check for recall
		if c.CheckForRecall(fmt.Sprintf("Stopping\tBar Drawing Unit\n")) {
			return
		}
	}

}

/*** RESTART FROM FILE ***/

// Restart reads a file produced by another execution of the program to restart a terminated search without loss of work.
func (c *Commander) Restart() bool {
	scanner := bufio.NewScanner(c.resin)
	scanner.Split(bufio.ScanWords)
	for scanner.Scan() {
		// Scan line
		n := util.ParseInt(scanner.Text())
		p := []int{}
		for i := 0; i < int(n); i++ {
			scanner.Scan()
			p = append(p, util.ParseInt(scanner.Text()))
		}
		scanner.Scan()
		sol := util.ParseInt(scanner.Text())
		scanner.Scan()
		kill := util.ParseFloat(scanner.Text())
		// Log
		util.Write(c.Sum(), "%v   \t   \t%q\t%d solution(s)", p, "loaded", sol)
		if kill < 0.0 {
			util.Write(c.Sum(), " (killed)")
		}
		util.Write(c.Sum(), "\n")
		util.Write(c.Log(), "Reloaded\t%v\t\t%f\t%d\n\t\t", p, kill, sol)
		// Delete from hash table
		delete(c.restartTable, util.ToString(p))
		// Enqueue children or add result
		if kill >= 0.0 && sol >= 0 {
			c.sols += uint64(sol)
		} else {
			for i := 1; i <= c.prng; i++ {
				if !c.IsValid(append(append([]int(nil), p...), i)) || (!c.Is(AllowingRepeats) && util.Contains(p, i)) {
					continue
				}
				if len(p) == int(n) {
					p = append(p, 0)
				}
				p[n] = i
				if c.restartTable[util.ToString(p)] == 0 {
					c.restartTable[util.ToString(p)] = MapSet
				}
			}
		}
	}
	return len(c.restartTable) == 0
}

/*** INITIALIZATION ***/

// Construct constructs a default commander using zero values or the constant defaults.
func (c *Commander) Construct() {
	c.dispatchStack = (concurrent.ConcurrentCollection)(concurrent.NewStack())
	c.reportStack = (concurrent.ConcurrentCollection)(concurrent.NewStack())
	c.safetyHash = concurrent.NewMap()
	c.head = make([]int, 0)
	c.comp = WorkerCompiler
	c.prog = WorkerProg
	c.settings = DefSet
	c.hiwater = HiMark
	c.lowater = LoMark
	c.gauge = Gauge
	c.opcap = OpCap
	c.mxsol = Sol
	c.sub_id = -1
	c.binr = -1
	c.depth = D
	c.wtime = T
	c.nwrk = N
	c.ndfs = S
	c.binl = 0
}

// TakeInput takes an input string of settings flags and changes the commander's settings to those of the string.
func (c *Commander) TakeInput(s string) {
	fooint := func(n unsafe.Pointer, _ rune, s string) { *(*int)(n) = util.ParseInt(s) }
	foobool := func(n unsafe.Pointer, r rune, s string) {
		mp := map[rune]int{'L' : LogOut, 'V' : Verbose, 'B' : DrawingBar, 'S' : SummaryOut, 'v' : SelectivelyVerbose, 'P' : AllowingRepeats, 'A' : Appending}
		v, _ := mp[r]
		if s == "0" {
			*(*int)(n) &= ^(int(1) << uint(v))
		} else {
			*(*int)(n) |= (int(1) << uint(v))
		}
	}
	c.head = make([]int, 0)
	foostr := func(n unsafe.Pointer, _ rune, s string) { *(*string)(n) = s }
	foohead := func(n unsafe.Pointer, _ rune, s string) {
		hd := strings.FieldsFunc(s, func(r rune) bool { return r == ':' || r == '-' || r == '_' })
		c := (*Commander)(n)
		for _, e := range hd {
			i := util.ParseInt(e)
			c.head = append(c.head, i)
		}
	}
	funm := map[rune]func(unsafe.Pointer, rune, string) {
		'n' : fooint, 'm' : fooint, 'l' : fooint, 't' : fooint, 'T' : fooint, 'd' : fooint, 'D' : fooint, 'A' : foobool,
		'L' : foobool, 'S' : foobool, 'R' : foobool, 'B' : foobool, 'P' : foobool, 'V' : foobool, 'v' : foobool, 'H' : foohead,
		'p' : fooint, 'g' : fooint, 'b' : fooint, 'u' : fooint, 'J' : fooint, 'c' : fooint, 'r' : foostr,
		'N' : fooint, 'i' : fooint, 'I' : foostr,
		'C' : foostr, 'W' : foostr, 'F' : foostr }
	argm := map[rune]unsafe.Pointer {
		'n' : unsafe.Pointer(&c.nwrk), 'm' : unsafe.Pointer(&c.prng), 'l' : unsafe.Pointer(&c.plen), 't' : unsafe.Pointer(&c.wtime),
		'T' : unsafe.Pointer(&c.ctime), 'd' : unsafe.Pointer(&c.depth), 'D' : unsafe.Pointer(&c.ndfs), 'H' : unsafe.Pointer(c),
		'L' : unsafe.Pointer(&c.settings), 'S' : unsafe.Pointer(&c.settings), 'R' : unsafe.Pointer(&c.settings), 'A' : unsafe.Pointer(&c.settings),
		'B' : unsafe.Pointer(&c.settings), 'P' : unsafe.Pointer(&c.settings), 'V' : unsafe.Pointer(&c.settings), 'v' : unsafe.Pointer(&c.settings),
		'p' : unsafe.Pointer(&c.opcap), 'g' : unsafe.Pointer(&c.gauge), 'b' : unsafe.Pointer(&c.binl), 'u' : unsafe.Pointer(&c.binr),
		'J' : unsafe.Pointer(&c.check), 'c' : unsafe.Pointer(&c.mxsol), 'r' : unsafe.Pointer(&c.resinstr),
		'N' : unsafe.Pointer(&c.netCode), 'i' : unsafe.Pointer(&c.sub_id), 'I' : unsafe.Pointer(&c.ipString),
		'C' : unsafe.Pointer(&c.comp), 'W' : unsafe.Pointer(&c.prog), 'F' : unsafe.Pointer(&c.namesuf) }
	util.ReadInfo(s, argm, funm)
}

// TakeRequest encapsulates the information of a subcommander work request in a commander object and returns that commander.
func (c *Commander) takeRequest(s string) *Commander {
	com := new(Commander)
	com.Construct()
	com.netCode = server.ClientCode
	com.TakeInput(s)
	return com
}

// TakeOutput encapsulates the information of a subcommander answer in an AssignmentInfo object and returns that object.
func (c *Commander) TakeOutput(s string) *AssignmentInfo {
	ai := new(AssignmentInfo)
	fooint := func(n unsafe.Pointer, _ rune, s string) { *(*int)(n) = util.ParseInt(s) }
	foostr := func(n unsafe.Pointer, _ rune, s string) { *(*string)(n) = s }
	fooflt := func(n unsafe.Pointer, _ rune, s string) { *(*float64)(n) = util.ParseFloat(s) }
	fooa := func(n unsafe.Pointer, _ rune, s string) {
		hd := strings.FieldsFunc(s, func(r rune) bool { return r == ':' || r == '-' || r == '_' })
		a := (*AssignmentInfo)(n)
		for _, e := range hd {
			i := util.ParseInt(e)
			a.assignment = append(a.assignment, i)
		}
	}
	funm := map[rune]func(unsafe.Pointer, rune, string) {
		'r' : fooint, 's' : fooint, 'i' : fooint,
		'e' : foostr, 't' : fooflt, 'a' : fooa }
	argm := map[rune]unsafe.Pointer {
		'r' : unsafe.Pointer(&ai.run), 's' : unsafe.Pointer(&ai.sol), 'i' : unsafe.Pointer(&ai.wid),
		'e' : unsafe.Pointer(&ai.err), 't' : unsafe.Pointer(&ai.time), 'a' : unsafe.Pointer(ai) }
	util.ReadInfo(s, argm, funm)
	return ai
}

/*** RUN SEARCH ***/

// SetUp performs one-time setup operations for the commander.
func (c *Commander) SetUp() {
	/// Make files and log startup
	c.MakeFiles()
	server.SetUpNetwork(c)
	util.Write(c.Log(), "Beginning setup...\n\t")
	c.timer = time.Now()
	if c.plen == 0 {
		c.plen = c.prng
	}
	c.hiwater *= util.Max(c.nwrk, 1)
	c.lowater *= util.Max(c.nwrk, 1)
	/// Cache factorials or exponents
	util.Write(c.Log(), "Caching difficulties...\t")
	c.fac = append(c.fac, 1)
	for i := 1; i <= util.Max(c.plen, c.binr); i++ {
		x := i
		if c.Is(AllowingRepeats) {
			x = c.prng
		}
		c.fac = append(c.fac, float64(x) * c.fac[i - 1])
		c.tdepth = c.plen + 1
		if c.fac[i] > float64(c.opcap * c.wtime) {
			c.tdepth = util.Min(c.tdepth, c.plen - i + 1)
		}
		if c.tdepth == c.plen + 1 {
			c.tdepth = 0
		}
	}
	util.Write(c.Log(), "done\n\t")
	/// Reset
	c.Reset(true)
	/// Log completion
	util.Write(c.Log(), "Finished setting up.\n")
}

// Reset performs setup operations before each run of the commander.
func (c *Commander) Reset(first bool) {
	/// Make files and log reset
	if !first {
		c.MakeFiles()
		util.Write(c.Log(), "Resetting...\n\tIncrementing run...\t")
		util.Inc(&c.run)
		util.Write(c.Log(), "done\n\t")
		/// Empty channels
		util.Write(c.Log(), "Emptying channels...\t")
		c.dispatchStack = (concurrent.ConcurrentCollection)(concurrent.NewStack())
		c.reportStack = (concurrent.ConcurrentCollection)(concurrent.NewStack())
		c.safetyHash = concurrent.NewMap()
		util.Write(c.Log(), "done\n\t")
	}
	/// Acquire assignment
	if c.NetCode() == server.ClientCode {
		c.GetWork()
	}
	/// Binary search step
	if c.binr >= 0 {
		util.Write(c.Log(), "Binary search step...\t")
		c.plen = (c.binr + c.binl) / 2
		c.timer = time.Now()
		util.Write(c.Log(), "done (l=%d)\n\t", c.plen)
	}
	/// Reset counters
	util.Write(c.Log(), "Resetting counters...\t")
	c.recall = false
	c.begin = false
	c.started = 0
	c.dropped = 0
	c.wjobs = 0
	c.wdone = 0
	c.sols = 0
	c.jobs = 0
	c.done = 0
	util.Write(c.Log(), "done\n\t")
	/// Launch workers and collectors
	util.Write(c.Log(), "Launching collectors...\t")
	for i := 1; i <= c.ndfs; i++ {
		go c.Collector(i)
	}
	util.Write(c.Log(), "done\n\tLaunching workers...\t")
	for i := 1; i <= c.nwrk; i++ {
		go c.Worker(i)
	}
	util.Write(c.Log(), "done\n\tLaunching bar...\t")
	go c.BarDraw()
	util.Write(c.Log(), "done\n\t");
	/// Hash base cases
	util.Write(c.Log(), "Hashing base cases...\n\t\t")
	c.restartTable = make(map[string]int)
	for i := 1; i <= c.prng; i++ {
		if c.Is(AllowingRepeats) || !(util.Contains(c.head, i)) {
			c.restartTable[util.ToString(append(append([]int{}, c.head...), i))] = MapSet
			util.Write(c.Log(), "Reloaded\t%v\n\t\t", append(append([]int{}, c.head...), i))
		}
	}
	util.Write(c.Log(), "done\n\t")
	/// Hash loaded cases
	c.alldone = false
	if c.resinstr != "" {
		util.Write(c.Log(), "Restarting from %s...\n\t\t", c.resinstr)
		c.alldone = c.Restart()
		util.Write(c.Log(), "done\n\t")
	}
	/// Queue hash
	util.Write(c.Log(), "Queueing hash...\tstarted\n")
	go func() {
		for p := range c.restartTable {
			r := c.run
			ok := false
			for !ok {
				ok = true
				defer func() {
					if r := recover(); r != nil {
						util.Write(c.ELog(), "Init:\tError queueing initial assignment... recovering\n")
						ok = false
					}
				}()
				func() {
					concurrent.Push(c.reportStack, AssignmentInfo{util.ToSlice(p), -1, Init, -1, -1, r})
					if ok { util.Inc(&c.jobs) }
				}()
			}
		}
		c.restartTable = nil
	}()
	/// Log completion
	if !first {
		util.Write(c.Log(), "Finished reseting.\n")
	}
}

// MakeFiles creates the files that the commander uses.
func (c *Commander) MakeFiles() {
	/// Create files
	c.log = util.FOpen(LogDir + "log" + c.namesuf + "." + strconv.Itoa(c.netCode) + "." + strconv.Itoa(int(c.run)) + ".txt", c.Is(Appending))
	c.summary = util.FOpen(LogDir + "summary" + c.namesuf + "." + strconv.Itoa(c.netCode) + "." + strconv.Itoa(int(c.run)) + ".txt", c.Is(Appending))
	c.ilog = util.FOpen(LogDir + "log.i" + c.namesuf + "." + strconv.Itoa(c.netCode) + "." + strconv.Itoa(int(c.run)) + ".txt", c.Is(Appending))
	c.elog = util.FOpen(LogDir + "errlog" + c.namesuf + "." + strconv.Itoa(c.netCode) + "." + strconv.Itoa(int(c.run)) + ".txt", c.Is(Appending))
	if c.resinstr != "" {
		c.resin, _ = os.Open(c.resinstr)
		c.res = util.FOpen(c.resinstr, true)
	} else {
		c.res = util.FOpen(LogDir + "restart" + c.namesuf + "." + strconv.Itoa(c.netCode) + "." + strconv.Itoa(int(c.run)) + ".txt", false)
	}
	/// Create loggers
	c.logger = &(util.Logger{util.MakeLog(c.log, c.Is(LogOut)), c.Is(Verbose)})
	c.rlogger = &(util.Logger{util.MakeLog(c.res, c.Is(RestartOut)), false})
	c.slogger = &(util.Logger{util.MakeLog(c.summary, c.Is(SummaryOut)), false})
	c.ilogger = &(util.Logger{util.MakeLog(c.ilog, c.Is(LogOut)), c.Is(SelectivelyVerbose)})
	c.elogger = &(util.Logger{util.MakeLog(c.elog, c.Is(LogOut)), c.Is(Verbose) || c.Is(SelectivelyVerbose)})
}

// Search performs a single search. It returns true if another search is necessary and false otherwise.
func (c *Commander) Search() bool {
	/// If already done, do not start the search
	if !c.alldone {
		/// Send begin message
		c.begin = true
		/// Wait for end conditions
		for {
			util.Write(c.Log(), "Mainloop\t(Checking for Recall)\n")
			if c.ctime != 0 && time.Now().Sub(c.timer).Seconds() > float64(c.ctime) {
				util.Write(c.Log(), "Recalled\t(Time Limit Exceeded)\n")
				break
			} else if c.jobs != uint64(0) && c.done == c.jobs {
				util.Write(c.Log(), "Recalled\t(Finished)\n")
				break
			} else if c.mxsol > 0 && int(c.sols) >= c.mxsol {
				util.Write(c.Log(), "Recalled\t(Solution Limit Exceeded)\n")
				break
			} else {
				time.Sleep(LPause * time.Millisecond)
			}
		}
		/// Sound recall
		c.recall = true
		for c.NetCode() != server.ServerCode && c.nwrk + c.ndfs + 1 != int(c.dropped) {
			time.Sleep(LPause * time.Millisecond)
		}
	}
	tm := time.Now().Sub(c.timer).Seconds()
	/// Subcommander
	if c.NetCode() == server.ClientCode {
		c.Report(tm, (c.jobs != uint64(0) && c.done == c.jobs) || (c.mxsol > 0 && int(c.sols) >= c.mxsol))
		return true;
	}
	/// Binary search step
	if c.binr >= 0 {
		m := (c.binl + c.binr) / 2
		if c.sols != 0 {
			util.Write(c.Log(), "Binary L\t%d\n", m + 1)
			c.mxFound = util.Max(c.mxFound, m)
			c.binl = m + 1
		} else {
			util.Write(c.Log(), "Binary R\t%d\n", m - 1)
			c.binr = m - 1
		}
		if c.binl > c.binr {
			m = c.mxFound
			util.Write(c.Log(), "SOLUTION\t%d\n%fs\n", m, tm)
			if !c.Is(Verbose) { fmt.Printf("SOLUTION\t%d\n%fs\n", m, tm) }
			return false
		}
		return true
	}
	/// End of run
	util.Write(c.Log(), "%d SOLUTIONS\n%fs\n", c.sols, tm)
	if !c.Is(Verbose) { fmt.Printf("%d SOLUTIONS\n%fs\n", c.sols, tm) }
	return false
}

// Main takes input and runs searches
func main() {

	/// Take input
	var c Commander
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Special settings: ")
	line, _ := reader.ReadString('\n')
	c.Construct()
	c.TakeInput(line)

	/// Run searches
	c.SetUp()
	for c.Search() {
		c.Reset(false)
	}

}
