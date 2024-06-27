package util

import (
	"sync/atomic"
	"strconv"
	"strings"
	"unsafe"
	"sync"
	"fmt"
	"os"
)

/*** FILE UTILITIES ***/

// LogFile is a wrapper for files that guarantees writing is atomic.
// LogFile objects can be 'turned off' so that write requests are ignored.
type LogFile struct {
	// LogLock is the LogFile's mutex for writing atomically.
	LogLock	*sync.Mutex
	// Log is the LogFile's wrapped File object.
	Log		*os.File
	// On is the boolean flag indicating whether the LogFile will accept write requests.
	On		bool
}

// FExists returns true if the file in the given location exists. It returns false otherwise.
func FExists(f string) bool {
	_, err := os.Stat(f)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

// FOpen returns a pointer to an os.File in the given location.
// If the file exists and a is true, then the file will be in append mode.
func FOpen(f string, a bool) *os.File {
	if a && FExists(f) {
		t, _ := os.OpenFile(f, os.O_APPEND|os.O_WRONLY, 0600)
		return t
	}
	t, _ := os.Create(f)
	return t
}

// MakeLog constructs a LogFile structure using the given file and setting.
func MakeLog(f *os.File, o bool) *LogFile {
	return &LogFile{&sync.Mutex{}, f, o}
}

// Write writes the input string to the Log File object if On is true.
// Any following parameters are inserted, formatted as with Printf.
func (log *LogFile) Write(v bool, format string, a ...interface{}) {
	out := fmt.Sprintf(format, a...)
	if log.On {
		log.LogLock.Lock()
		fmt.Fprint(log.Log, out)
		log.LogLock.Unlock()
	}
	if v {
		fmt.Print(out)
	}
}

/*** INPUT UTILITIES ***/

// ReadInfo is a general function for taking command line input with a system of one-character flags.
// For each flag, the following whitespace separated block of text is passed as an argument to the corresponding function found in the input maps.
func ReadInfo(line string, argm map[rune]unsafe.Pointer, funm map[rune]func(unsafe.Pointer, rune, string)) {
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
			if _, ok := argm[c]; ok {
				funm[c](argm[c], c, s)
			} else {
				fmt.Printf("Command not recognized: -%s\n", string(c))
			}
		}
	}
}

/*** BASIC UTILITIES ***/

// Logger is an interface that provides utility functions with a way to access the commander's log files.
type Logger struct {
	Log	*LogFile
	Verb	bool
}

// Write writes to a logger's file.
func Write(lgr *Logger, format string, a ...interface{}) { (*(*lgr).Log).Write((*lgr).Verb, format, a...) };

// ParseInt takes as an argument a string s representing an integer.
// ParseInt parses and returns an int32 from s.
func ParseInt(s string) int {
	t, _ := strconv.ParseInt(s, 10, 32)
	return int(t)
}

// ParseInt takes as an argument a string s representing a decimal value.
// ParseFloat parses and returns a float64 from s.
func ParseFloat(s string) float64 {
	t, _ := strconv.ParseFloat(s, 64)
	return float64(t)
}

// Max takes as arguments two integers a and b.
// Max returns the greater of a and b.
func Max(i, j int) int {
	if i > j {
		return i
	}
	return j
}

// Min takes as arguments two integers a and b.
// Min returns the smaller of a and b.
func Min(i, j int) int {
	if i > j {
		return j
	}
	return i
}

// ToString takes as an argument a slice of integers a.
// ToString converts a to a space-separated string and returns that string.
func ToString(a []int) string {
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

// ToSlice takes as an argument a string s containing only space-separated integers.
// ToSlice converts s into a slice of int32 and returns that slice.
func ToSlice(s string) []int {
	f := strings.Fields(s)
	a := []int{}
	for _, n := range f {
		a = append(a, ParseInt(n))
	}
	return a
}

// Unwrap takes as an argument an interface{} t representing an int32 slice.
// Unwrap returns the int32 slice represented by t.
func Unwrap(t interface{}) []int {
	a := []int{}
	nt, _ := t.([]int)
	for _, n := range nt {
		a = append(a, int(n))
	}
	return a
}

// Equals takes as arguments two int32 slices a and b.
// Equals returns true if a and b contain the same elements in the same order and false otherwise.
func Equals(a []int, b []int) bool {
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

// Contains takes as arguments an int slice s and an integer e.
// Contains returns true if s contains e and false otherwise.
func Contains(s []int, e int) bool {
	for _, n := range s {
		if n == e {
			return true
		}
	}
	return false
}

// Hand handles an error input err.
// If err is nil, Hand returns true. Otherwise, it returns false.
// Hand submits a write request to the logger, containing s1 if err is nil and false otherise.
func Hand(lgr *Logger, err error, s1 string, s2 string) bool {
	if err != nil {
		if s1 != "" {
			go Write(lgr, s1 + "\n")
		}
		return false
	}
	if s2 != "" {
		go Write(lgr, s2 + "\n")
	}
	return true
}

// Inc increments the value of x.
// Inc is atomic.
func Inc(x *uint64) {
	atomic.AddUint64(x, 1)
}

// Replace takes as arguments string s, rune r, and integer i.
// Replace returns a new string identical to s, except character i is replaced with r.
func Replace(s string, r rune, i int) string {
	ss := []rune(s)
	ss[i] = r
	return string(ss)
}