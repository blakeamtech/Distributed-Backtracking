package server

import (
	"strconv"
	"../util"
	"errors"
	"bufio"
	"fmt"
	"net"
	"os"
)

/*** CONSTANTS ***/

// MessageLengthBytes is the length of the message prefix that communicates the length of the rest of the message, 4.
const MessageLengthBytes = 4
// MaxByte is the number of values a byte can hold, 256.
const MaxByte = 1 << Byte
// Net is the internet protocol used, TCP.
const Net = "tcp"
// Byte is the number of bits in a byte, 8.
const Byte = 8
// OfflineCode is the NetCode of a local commander.
const OfflineCode = 0
// ClientCode is the NetCode of a subcommander.
const ClientCode = 1
// ServerCode is the NetCode of a server commander.
const ServerCode = 2
// ReadWait is the time a communicator will wait on an open connection before closing it.
const ReadWait = 10
// KeyFail is the error returned when a port denies access or gives an incorrect key
const KeyFail = "access denied"

/*** TYPE DEFINITIONS ***/

// Communicator is an interface that interfaces Server and Client implement.
// Communicator requires that structures possess a target IP address, a code defining whether they are a server or a client, and a key that begins all messages.
type Communicator interface {
	TargetIP()	**net.TCPAddr
	ILog()		*util.Logger
	IPStr()		*string
	Key()		string
	NetCode()	int
}

// Server is an interface for which general request catching is implemented in this package.
// Server requires that structures define a method for handling individual requests.
type Server interface {
	Lis()	**net.TCPListener
	HandleRequest(*net.TCPConn)
	Communicator
}

// Client is another name for Communicator.
type Client interface {
	Communicator
}

/*** METHOD IMPLEMENTATION ***/

// RetryIP prompts for a new IP address via stdout/stdin.
func RetryIP(com Communicator) {
	fmt.Print("Enter new target TCP/IP address: ")
	reader := bufio.NewReader(os.Stdin)
	ln, _ := reader.ReadString('\n')
	*com.IPStr() = ln[:len(ln)-1]
}

// Acquire sets the target address of the communicator com to that defined by its IP string.
func Acquire(com Communicator) bool {
	var err error
	*com.TargetIP(), err = net.ResolveTCPAddr(Net, *com.IPStr())
	return util.Hand(com.ILog(), err, "Error acquiring target TCP/IP address: "+*com.IPStr(), "Target TCP/IP address acquired: "+*com.IPStr())
}

//11:40
// Reacquire repeatedly attempts to acquire a target IP address.
// Reacquire prompts for a new IP address from stdin upon failure.
func Reacquire(com Communicator) {
	for !Acquire(com) {
		RetryIP(com)
	}
}

// Listen creates a listener for interface Server.
// Listen returns true if the TCPListener is successfully created and false otherwise.
func Listen(ser Server) bool {
	var err error
	*ser.Lis(), err = net.ListenTCP(Net, *ser.TargetIP())
	return util.Hand(ser.ILog(), err, "Error creating TCPListener on port", "Listening to port")
}

// Connect connects a Client interface to the target Server.
// Connect returns a pointer to the connection.
func Connect(cli Client) *net.TCPConn {
	sock, err := net.DialTCP(Net, nil, *cli.TargetIP())
	util.Hand(cli.ILog(), err, "Error opening connection to target", "Opened connection to target")
	return sock
}

// Accept accepts a connection to a Server.
// Accept returns a pointer to the connection.
func Accept(ser Server) *net.TCPConn {
	con, err := (*ser.Lis()).AcceptTCP()
	util.Hand(ser.ILog(), err, "Error attempting connection", "Connected to "+con.RemoteAddr().String())
	return con
}

// ReadBytes reads n bytes from a connection to a Communicator.
// Readbytes resturns the string read from the connection.
func ReadBytes(com Communicator, con *net.TCPConn, n int) string {
	go util.Write(com.ILog(), "Reading "+strconv.Itoa(n)+" bytes\n")
	r := make([]byte, n)
	_, err := con.Read(r)
	util.Hand(com.ILog(), err, "Error reading from connected port at "+con.RemoteAddr().String(), "Message ("+string(r)+") received from port at "+con.RemoteAddr().String())
	return string(r)
}

// Read reads the input from a connection to a Communicator.
// Read first reads MessageLengthBytes bytes to determine how many more bytes to read.
// Read returns the read string.
func Read(com Communicator, con *net.TCPConn) (string, error) {
	key := ReadBytes(com, con, len(com.Key()))
	if key != com.Key() {
		return "", errors.New(KeyFail)
	}
	ns := []byte(ReadBytes(com, con, MessageLengthBytes))
	n := 0
	for _, ch := range ns {
		n = n<<Byte + int(ch)
	}
	return ReadBytes(com, con, n), nil
}

// WriteBytes writes a string to a connection of a Communicator.
// WriteBytes returns true if the string is successfully transmitted and false otherwise.
func WriteBytes(com Communicator, con *net.TCPConn, m string) bool {
	_, err := con.Write([]byte(m))
	return util.Hand(com.ILog(), err, "Error transmitting message ("+m+") to port at "+con.RemoteAddr().String(), "Transmitted message ("+m+") to port at "+con.RemoteAddr().String())
}

// Write writes a string to a connection, prefixed by MessageLengthBytes bytes representing the length of the string.
// Write returns true if the string is successfully transmitted and false otherwise.
func Write(com Communicator, con *net.TCPConn, m string) bool {
	n := len(m)
	s := ""
	for i := 0; i < MessageLengthBytes; i++ {
		//fmt.Println(n % MaxByte, byte(n % MaxByte))
		s = string(byte(n%MaxByte)) + s
		n /= MaxByte
	}
	return WriteBytes(com, con, com.Key()+s+m)
}

// SetUpNetwork acquires the target IP address of a Communicator com.
// If com is a Server, it also creates a listener on the target port.
func SetUpNetwork(com Communicator) {
	if com.NetCode() == OfflineCode {
		return
	}
	Reacquire(com)
	if com.NetCode() == ServerCode {
		ser, _ := com.(Server)
		if !Listen(ser) {
			RetryIP(com)
			SetUpNetwork(com)
		} else {
			go RunService(ser)
		}
	}
}

// RunService catches all connection requests to a Server.
// RunService calls HandleRequest for each.
func RunService(ser Server) {
	for {
		con := Accept(ser)
		go ser.HandleRequest(con)
	}
}