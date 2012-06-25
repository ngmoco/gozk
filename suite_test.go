package gozk_test

import (
	. "launchpad.net/gocheck"
	"testing"
	"io/ioutil"
	"path"
	"fmt"
	"os"
	"gozk"
	"time"
)

func TestAll(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&S{})

type S struct {
	zkRoot      string
	zkTestRoot  string
	zkTestPort  int
	zkServerSh  string
	zkServerOut *os.File
	zkAddr      string

	handles     []*gozk.ZooKeeper
	events      []*gozk.Event
	liveWatches int
	deadWatches chan bool
}

var logLevel = 0 //gozk.LOG_ERROR


var testZooCfg = ("dataDir=%s\n" +
	"clientPort=%d\n" +
	"tickTime=2000\n" +
	"initLimit=10\n" +
	"syncLimit=5\n" +
	"")

var testLog4jPrp = ("log4j.rootLogger=INFO,CONSOLE\n" +
	"log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
	"log4j.appender.CONSOLE.Threshold=DEBUG\n" +
	"log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
	"log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} [myid:%X{myid}] - %-5p [%t:%C{1}@%L] - %m%n" +
	"")

func (s *S) init(c *C) (*gozk.ZooKeeper, chan gozk.Event) {
	zk, watch, err := gozk.Init(s.zkAddr, 5e9)
	c.Assert(err, IsNil)

	s.handles = append(s.handles, zk)

	event := <-watch

	c.Assert(event.Type, Equals, gozk.EVENT_SESSION)
	c.Assert(event.State, Equals, gozk.STATE_CONNECTED)

	bufferedWatch := make(chan gozk.Event, 256)
	bufferedWatch <- event

	s.liveWatches += 1
	go func() {
	loop:
		for {
			select {
			case event, ok := <-watch:
				if !ok {
					close(bufferedWatch)
					break loop
				}
				select {
				case bufferedWatch <- event:
				default:
					panic("Too many events in buffered watch!")
				}
			}
		}
		s.deadWatches <- true
	}()

	return zk, bufferedWatch
}

func (s *S) SetUpTest(c *C) {
	c.Assert(gozk.CountPendingWatches(), Equals, 0,
		Bug("Test got a dirty watch state before running!"))
	gozk.SetLogLevel(logLevel)
}

func (s *S) TearDownTest(c *C) {
	// Close all handles opened in s.init().
	for _, handle := range s.handles {
		handle.Close()
	}

	// Wait for all the goroutines created in s.init() to terminate.
	for s.liveWatches > 0 {
		select {
		case <-s.deadWatches:
			s.liveWatches -= 1
		case <-time.After(5e9):
			panic("There's a locked watch goroutine :-(")
		}
	}

	// Reset the list of handles.
	s.handles = make([]*gozk.ZooKeeper, 0)

	c.Assert(gozk.CountPendingWatches(), Equals, 0,
		Bug("Test left live watches behind!"))
}

// We use the suite set up and tear down to manage a custom zookeeper
//
func (s *S) SetUpSuite(c *C) {

	s.deadWatches = make(chan bool)

	var err os.Error

	s.zkRoot = os.Getenv("ZKROOT")
	if s.zkRoot == "" {
		panic("You must define $ZKROOT ($ZKROOT/bin/zkServer.sh is needed)")
	}

	s.zkTestRoot = c.MkDir()
	s.zkTestPort = 21812

	println("ZooKeeper test server directory:", s.zkTestRoot)
	println("ZooKeeper test server port:", s.zkTestPort)

	s.zkAddr = fmt.Sprintf("localhost:%d", s.zkTestPort)

	s.zkServerSh = path.Join(s.zkRoot, "bin/zkServer.sh")
	s.zkServerOut, err = os.OpenFile(path.Join(s.zkTestRoot, "stdout.txt"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic("Can't open stdout.txt file for server: " + err.String())
	}

	dataDir := path.Join(s.zkTestRoot, "data")
	confDir := path.Join(s.zkTestRoot, "conf")

	os.Mkdir(dataDir, 0755)
	os.Mkdir(confDir, 0755)

	err = os.Setenv("ZOOCFGDIR", confDir)
	if err != nil {
		panic("Can't set $ZOOCFGDIR: " + err.String())
	}

	zooCfg := []byte(fmt.Sprintf(testZooCfg, dataDir, s.zkTestPort))
	err = ioutil.WriteFile(path.Join(confDir, "zoo.cfg"), zooCfg, 0644)
	if err != nil {
		panic("Can't write zoo.cfg: " + err.String())
	}

	log4jPrp := []byte(testLog4jPrp)
	err = ioutil.WriteFile(path.Join(confDir, "log4j.properties"), log4jPrp, 0644)
	if err != nil {
		panic("Can't write log4j.properties: " + err.String())
	}

	s.StartZK()
}

func (s *S) TearDownSuite(c *C) {
	s.StopZK()
	s.zkServerOut.Close()
}

func (s *S) StartZK() {
	attr := os.ProcAttr{Files: []*os.File{os.Stdin, s.zkServerOut, os.Stderr}}
	proc, err := os.StartProcess(s.zkServerSh, []string{s.zkServerSh, "start"}, &attr)
	if err != nil {
		panic("Problem executing zkServer.sh start: " + err.String())
	}

	result, err := proc.Wait(0)
	if err != nil {
		panic(err.String())
	} else if result.ExitStatus() != 0 {
		panic("'zkServer.sh start' exited with non-zero status")
	}
}

func (s *S) StopZK() {
	attr := os.ProcAttr{Files: []*os.File{os.Stdin, s.zkServerOut, os.Stderr}}
	proc, err := os.StartProcess(s.zkServerSh, []string{s.zkServerSh, "stop"}, &attr)
	if err != nil {
		panic("Problem executing zkServer.sh stop: " + err.String() +
			" (look for runaway java processes!)")
	}
	result, err := proc.Wait(0)
	if err != nil {
		panic(err.String())
	} else if result.ExitStatus() != 0 {
		panic("'zkServer.sh stop' exited with non-zero status " +
			"(look for runaway java processes!)")
	}
}
