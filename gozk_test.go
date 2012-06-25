package gozk_test

import (
	. "launchpad.net/gocheck"
	"gozk"
	"time"
)

// This error will be delivered via C errno, since ZK unfortunately
// only provides the handler back from zookeeper_init().
func (s *S) TestInitErrorThroughErrno(c *C) {
	zk, watch, err := gozk.Init("bad-domain-without-port", 5e9)
	if zk != nil {
		zk.Close()
	}
	if watch != nil {
		go func() {
			for {
				_, ok := <-watch
				if !ok {
					break
				}
			}
		}()
	}
	c.Assert(zk, IsNil)
	c.Assert(watch, IsNil)
	c.Assert(err, Matches, "invalid argument")
}

func (s *S) TestRecvTimeoutInitParameter(c *C) {
	zk, watch, err := gozk.Init(s.zkAddr, 0)
	c.Assert(err, IsNil)
	defer zk.Close()

	select {
	case <-watch:
		c.Fatal("Watch fired")
	default:
	}

	for i := 0; i != 1000; i++ {
		_, _, err := zk.Get("/zookeeper")
		if err != nil {
			c.Assert(err, Matches, "operation timeout")
			c.SucceedNow()
		}
	}

	c.Fatal("Operation didn't timeout")
}

func (s *S) TestSessionWatches(c *C) {
	c.Assert(gozk.CountPendingWatches(), Equals, 0)

	zk1, watch1 := s.init(c)
	zk2, watch2 := s.init(c)
	zk3, watch3 := s.init(c)

	c.Assert(gozk.CountPendingWatches(), Equals, 3)

	event1 := <-watch1
	c.Assert(event1.Type, Equals, gozk.EVENT_SESSION)
	c.Assert(event1.State, Equals, gozk.STATE_CONNECTED)

	c.Assert(gozk.CountPendingWatches(), Equals, 3)

	event2 := <-watch2
	c.Assert(event2.Type, Equals, gozk.EVENT_SESSION)
	c.Assert(event2.State, Equals, gozk.STATE_CONNECTED)

	c.Assert(gozk.CountPendingWatches(), Equals, 3)

	event3 := <-watch3
	c.Assert(event3.Type, Equals, gozk.EVENT_SESSION)
	c.Assert(event3.State, Equals, gozk.STATE_CONNECTED)

	c.Assert(gozk.CountPendingWatches(), Equals, 3)

	zk1.Close()
	c.Assert(gozk.CountPendingWatches(), Equals, 2)
	zk2.Close()
	c.Assert(gozk.CountPendingWatches(), Equals, 1)
	zk3.Close()
	c.Assert(gozk.CountPendingWatches(), Equals, 0)
}

// Gozk injects a STATE_CLOSED event when zk.Close() is called, right
// before the channel is closed.  Closing the channel injects a nil
// pointer, as usual for Go, so the STATE_CLOSED gives a chance to
// know that a nil pointer is coming, and to stop the procedure.
// Hopefully this procedure will avoid some nil-pointer references by
// mistake.
func (s *S) TestClosingStateInSessionWatch(c *C) {
	zk, watch := s.init(c)

	event := <-watch
	c.Assert(event.Type, Equals, gozk.EVENT_SESSION)
	c.Assert(event.State, Equals, gozk.STATE_CONNECTED)

	zk.Close()
	event, ok := <-watch
	c.Assert(ok, Equals, false)
	c.Assert(event.Type, Equals, gozk.EVENT_CLOSED)
	c.Assert(event.State, Equals, gozk.STATE_CLOSED)
}

func (s *S) TestEventString(c *C) {
	var event gozk.Event
	event = gozk.Event{gozk.EVENT_SESSION, "/path", gozk.STATE_CONNECTED}
	c.Assert(event, Matches, "ZooKeeper connected")
	event = gozk.Event{gozk.EVENT_CREATED, "/path", gozk.STATE_CONNECTED}
	c.Assert(event, Matches, "ZooKeeper connected; path created: /path")
	event = gozk.Event{-1, "/path", gozk.STATE_CLOSED}
	c.Assert(event, Matches, "ZooKeeper connection closed")
}

var okTests = []struct{gozk.Event; Ok bool}{
	{gozk.Event{gozk.EVENT_SESSION, "", gozk.STATE_CONNECTED}, true},
	{gozk.Event{gozk.EVENT_CREATED, "", gozk.STATE_CONNECTED}, true},
	{gozk.Event{0, "", gozk.STATE_CLOSED}, false},
	{gozk.Event{0, "", gozk.STATE_EXPIRED_SESSION}, false},
	{gozk.Event{0, "", gozk.STATE_AUTH_FAILED}, false},
}

func (s *S) TestEventOk(c *C) {
	for _, t := range okTests {
		c.Assert(t.Event.Ok(), Equals, t.Ok)
	}
}

func (s *S) TestGetAndStat(c *C) {
	zk, _ := s.init(c)

	data, stat, err := zk.Get("/zookeeper")
	c.Assert(err, IsNil)
	c.Assert(data, Equals, "")
	c.Assert(stat.Czxid(), Equals, int64(0))
	c.Assert(stat.Mzxid(), Equals, int64(0))
	c.Assert(stat.CTime(), Equals, int64(0))
	c.Assert(stat.MTime(), Equals, int64(0))
	c.Assert(stat.Version(), Equals, int32(0))
	c.Assert(stat.CVersion(), Equals, int32(0))
	c.Assert(stat.AVersion(), Equals, int32(0))
	c.Assert(stat.EphemeralOwner(), Equals, int64(0))
	c.Assert(stat.DataLength(), Equals, int32(0))
	c.Assert(stat.NumChildren(), Equals, int32(1))
	c.Assert(stat.Pzxid(), Equals, int64(0))
}

func (s *S) TestGetAndError(c *C) {
	zk, _ := s.init(c)

	data, stat, err := zk.Get("/non-existent")

	c.Assert(data, Equals, "")
	c.Assert(stat, IsNil)
	c.Assert(err, Matches, "no node")
	c.Assert(err.Code(), Equals, gozk.ZNONODE)
}

func (s *S) TestCreateAndGet(c *C) {
	zk, _ := s.init(c)

	path, err := zk.Create("/test-", "bababum", gozk.SEQUENCE|gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL))
	c.Assert(err, IsNil)
	c.Assert(path, Matches, "/test-[0-9]+")

	// Check the error condition from Create().
	_, err = zk.Create(path, "", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL))
	c.Assert(err, Matches, "node exists")

	data, _, err := zk.Get(path)
	c.Assert(err, IsNil)
	c.Assert(data, Equals, "bababum")
}

func (s *S) TestCreateSetAndGet(c *C) {
	zk, _ := s.init(c)

	_, err := zk.Create("/test", "", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL))
	c.Assert(err, IsNil)

	stat, err := zk.Set("/test", "bababum", -1) // Any version.
	c.Assert(err, IsNil)
	c.Assert(stat.Version(), Equals, int32(1))

	data, _, err := zk.Get("/test")
	c.Assert(err, IsNil)
	c.Assert(data, Equals, "bababum")
}

func (s *S) TestGetAndWatch(c *C) {
	c.Check(gozk.CountPendingWatches(), Equals, 0)

	zk, _ := s.init(c)

	c.Check(gozk.CountPendingWatches(), Equals, 1)

	_, err := zk.Create("/test", "one", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL))
	c.Assert(err, IsNil)

	data, stat, watch, err := zk.GetW("/test")
	c.Assert(err, IsNil)
	c.Assert(data, Equals, "one")
	c.Assert(stat.Version(), Equals, int32(0))

	select {
	case <-watch:
		c.Fatal("Watch fired")
	default:
	}

	c.Check(gozk.CountPendingWatches(), Equals, 2)

	_, err = zk.Set("/test", "two", -1)
	c.Assert(err, IsNil)

	event := <-watch
	c.Assert(event.Type, Equals, gozk.EVENT_CHANGED)

	c.Check(gozk.CountPendingWatches(), Equals, 1)

	data, _, watch, err = zk.GetW("/test")
	c.Assert(err, IsNil)
	c.Assert(data, Equals, "two")

	select {
	case <-watch:
		c.Fatal("Watch fired")
	default:
	}

	c.Check(gozk.CountPendingWatches(), Equals, 2)

	_, err = zk.Set("/test", "three", -1)
	c.Assert(err, IsNil)

	event = <-watch
	c.Assert(event.Type, Equals, gozk.EVENT_CHANGED)

	c.Check(gozk.CountPendingWatches(), Equals, 1)
}

func (s *S) TestGetAndWatchWithError(c *C) {
	c.Check(gozk.CountPendingWatches(), Equals, 0)

	zk, _ := s.init(c)

	c.Check(gozk.CountPendingWatches(), Equals, 1)

	_, _, watch, err := zk.GetW("/test")
	c.Assert(err, NotNil)
	c.Assert(err.Code(), Equals, gozk.ZNONODE)
	c.Assert(watch, IsNil)

	c.Check(gozk.CountPendingWatches(), Equals, 1)
}

func (s *S) TestCloseReleasesWatches(c *C) {
	c.Check(gozk.CountPendingWatches(), Equals, 0)

	zk, _ := s.init(c)

	c.Check(gozk.CountPendingWatches(), Equals, 1)

	_, err := zk.Create("/test", "one", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL))
	c.Assert(err, IsNil)

	_, _, _, err = zk.GetW("/test")
	c.Assert(err, IsNil)

	c.Assert(gozk.CountPendingWatches(), Equals, 2)

	zk.Close()

	c.Assert(gozk.CountPendingWatches(), Equals, 0)
}

// By default, the ZooKeeper C client will hang indefinitely if a
// handler is closed twice.  We get in the way and prevent it.
func (s *S) TestClosingTwiceDoesntHang(c *C) {
	zk, _ := s.init(c)
	err := zk.Close()
	c.Assert(err, IsNil)
	err = zk.Close()
	c.Assert(err, NotNil)
	c.Assert(err.Code(), Equals, gozk.ZCLOSING)
}

func (s *S) TestChildren(c *C) {
	zk, _ := s.init(c)

	children, stat, err := zk.Children("/")
	c.Assert(err, IsNil)
	c.Assert(children, Equals, []string{"zookeeper"})
	c.Assert(stat.NumChildren(), Equals, int32(1))

	children, stat, err = zk.Children("/non-existent")
	c.Assert(err, NotNil)
	c.Assert(err.Code(), Equals, gozk.ZNONODE)
	c.Assert(children, Equals, []string{})
	c.Assert(stat, Equals, nil)
}

func (s *S) TestChildrenAndWatch(c *C) {
	c.Check(gozk.CountPendingWatches(), Equals, 0)

	zk, _ := s.init(c)

	c.Check(gozk.CountPendingWatches(), Equals, 1)

	children, stat, watch, err := zk.ChildrenW("/")
	c.Assert(err, IsNil)
	c.Assert(children, Equals, []string{"zookeeper"})
	c.Assert(stat.NumChildren(), Equals, int32(1))

	select {
	case <-watch:
		c.Fatal("Watch fired")
	default:
	}

	c.Check(gozk.CountPendingWatches(), Equals, 2)

	_, err = zk.Create("/test1", "", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL))
	c.Assert(err, IsNil)

	event := <-watch
	c.Assert(event.Type, Equals, gozk.EVENT_CHILD)
	c.Assert(event.Path, Equals, "/")

	c.Check(gozk.CountPendingWatches(), Equals, 1)

	children, stat, watch, err = zk.ChildrenW("/")
	c.Assert(err, IsNil)
	c.Assert(stat.NumChildren(), Equals, int32(2))

	// The ordering is most likely unstable, so this test must be fixed.
	c.Assert(children, Equals, []string{"test1", "zookeeper"})

	select {
	case <-watch:
		c.Fatal("Watch fired")
	default:
	}

	c.Check(gozk.CountPendingWatches(), Equals, 2)

	_, err = zk.Create("/test2", "", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL))
	c.Assert(err, IsNil)

	event = <-watch
	c.Assert(event.Type, Equals, gozk.EVENT_CHILD)

	c.Check(gozk.CountPendingWatches(), Equals, 1)
}

func (s *S) TestChildrenAndWatchWithError(c *C) {
	c.Check(gozk.CountPendingWatches(), Equals, 0)

	zk, _ := s.init(c)

	c.Check(gozk.CountPendingWatches(), Equals, 1)

	_, stat, watch, err := zk.ChildrenW("/test")
	c.Assert(err, NotNil)
	c.Assert(err.Code(), Equals, gozk.ZNONODE)
	c.Assert(watch, IsNil)
	c.Assert(stat, IsNil)

	c.Check(gozk.CountPendingWatches(), Equals, 1)
}

func (s *S) TestExists(c *C) {
	zk, _ := s.init(c)

	stat, err := zk.Exists("/zookeeper")
	c.Assert(err, IsNil)
	c.Assert(stat.NumChildren(), Equals, int32(1))

	stat, err = zk.Exists("/non-existent")
	c.Assert(err, IsNil)
	c.Assert(stat, IsNil)
}

func (s *S) TestExistsAndWatch(c *C) {
	c.Check(gozk.CountPendingWatches(), Equals, 0)

	zk, _ := s.init(c)

	c.Check(gozk.CountPendingWatches(), Equals, 1)

	stat, watch, err := zk.ExistsW("/test")
	c.Assert(err, IsNil)
	c.Assert(stat, IsNil)

	c.Check(gozk.CountPendingWatches(), Equals, 2)

	select {
	case <-watch:
		c.Fatal("Watch fired")
	default:
	}

	_, err = zk.Create("/test", "", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL))
	c.Assert(err, IsNil)

	event := <-watch
	c.Assert(event.Type, Equals, gozk.EVENT_CREATED)
	c.Assert(event.Path, Equals, "/test")

	c.Check(gozk.CountPendingWatches(), Equals, 1)

	stat, watch, err = zk.ExistsW("/test")
	c.Assert(err, IsNil)
	c.Assert(stat, NotNil)
	c.Assert(stat.NumChildren(), Equals, int32(0))

	c.Check(gozk.CountPendingWatches(), Equals, 2)
}

func (s *S) TestExistsAndWatchWithError(c *C) {
	c.Check(gozk.CountPendingWatches(), Equals, 0)

	zk, _ := s.init(c)

	c.Check(gozk.CountPendingWatches(), Equals, 1)

	stat, watch, err := zk.ExistsW("///")
	c.Assert(err, NotNil)
	c.Assert(err.Code(), Equals, gozk.ZBADARGUMENTS)
	c.Assert(stat, IsNil)
	c.Assert(watch, IsNil)

	c.Check(gozk.CountPendingWatches(), Equals, 1)
}

func (s *S) TestDelete(c *C) {
	zk, _ := s.init(c)

	_, err := zk.Create("/test", "", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL))
	c.Assert(err, IsNil)

	err = zk.Delete("/test", 5)
	c.Assert(err, NotNil)
	c.Assert(err.Code(), Equals, gozk.ZBADVERSION)

	err = zk.Delete("/test", -1)
	c.Assert(err, IsNil)

	err = zk.Delete("/test", -1)
	c.Assert(err, NotNil)
	c.Assert(err.Code(), Equals, gozk.ZNONODE)
}

func (s *S) TestClientIdAndReInit(c *C) {
	zk1, _ := s.init(c)
	clientId1 := zk1.ClientId()

	zk2, _, err := gozk.ReInit(s.zkAddr, 5e9, clientId1)
	c.Assert(err, IsNil)
	defer zk2.Close()
	clientId2 := zk2.ClientId()

	c.Assert(clientId1, Equals, clientId2)
}

// Surprisingly for some (including myself, initially), the watch
// returned by the exists method actually fires on data changes too.
func (s *S) TestExistsWatchOnDataChange(c *C) {
	zk, _ := s.init(c)

	_, err := zk.Create("/test", "", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL))
	c.Assert(err, IsNil)

	_, watch, err := zk.ExistsW("/test")
	c.Assert(err, IsNil)

	_, err = zk.Set("/test", "new", -1)
	c.Assert(err, IsNil)

	event := <-watch

	c.Assert(event.Path, Equals, "/test")
	c.Assert(event.Type, Equals, gozk.EVENT_CHANGED)
}

func (s *S) TestACL(c *C) {
	zk, _ := s.init(c)

	_, err := zk.Create("/test", "", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL))
	c.Assert(err, IsNil)

	acl, stat, err := zk.ACL("/test")
	c.Assert(err, IsNil)
	c.Assert(acl, Equals, gozk.WorldACL(gozk.PERM_ALL))
	c.Assert(stat, NotNil)
	c.Assert(stat.Version(), Equals, int32(0))

	acl, stat, err = zk.ACL("/non-existent")
	c.Assert(err, NotNil)
	c.Assert(err.Code(), Equals, gozk.ZNONODE)
	c.Assert(acl, IsNil)
	c.Assert(stat, IsNil)
}

func (s *S) TestSetACL(c *C) {
	zk, _ := s.init(c)

	_, err := zk.Create("/test", "", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL))
	c.Assert(err, IsNil)

	err = zk.SetACL("/test", gozk.WorldACL(gozk.PERM_ALL), 5)
	c.Assert(err, NotNil)
	c.Assert(err.Code(), Equals, gozk.ZBADVERSION)

	err = zk.SetACL("/test", gozk.WorldACL(gozk.PERM_READ), -1)
	c.Assert(err, IsNil)

	acl, _, err := zk.ACL("/test")
	c.Assert(err, IsNil)
	c.Assert(acl, Equals, gozk.WorldACL(gozk.PERM_READ))
}

func (s *S) TestAddAuth(c *C) {
	zk, _ := s.init(c)

	acl := []gozk.ACL{{gozk.PERM_READ, "digest", "joe:enQcM3mIEHQx7IrPNStYBc0qfs8="}}

	_, err := zk.Create("/test", "", gozk.EPHEMERAL, acl)
	c.Assert(err, IsNil)

	_, _, err = zk.Get("/test")
	c.Assert(err, NotNil)
	c.Assert(err.Code(), Equals, gozk.ZNOAUTH)

	err = zk.AddAuth("digest", "joe:passwd")
	c.Assert(err, IsNil)

	_, _, err = zk.Get("/test")
	c.Assert(err, IsNil)
}

func (s *S) TestWatchOnReconnection(c *C) {
	c.Check(gozk.CountPendingWatches(), Equals, 0)

	zk, session := s.init(c)

	event := <-session
	c.Assert(event.Type, Equals, gozk.EVENT_SESSION)
	c.Assert(event.State, Equals, gozk.STATE_CONNECTED)

	c.Check(gozk.CountPendingWatches(), Equals, 1)

	stat, watch, err := zk.ExistsW("/test")
	c.Assert(err, IsNil)
	c.Assert(stat, IsNil)

	c.Check(gozk.CountPendingWatches(), Equals, 2)

	s.StopZK()
	time.Sleep(2e9)
	s.StartZK()

	// The session channel should receive the reconnection notification,
	select {
	case event := <-session:
		c.Assert(event.State, Equals, gozk.STATE_CONNECTING)
	case <-time.After(3e9):
		c.Fatal("Session watch didn't fire")
	}
	select {
	case event := <-session:
		c.Assert(event.State, Equals, gozk.STATE_CONNECTED)
	case <-time.After(3e9):
		c.Fatal("Session watch didn't fire")
	}

	// The watch channel should not, since it's not affected.
	select {
	case event := <-watch:
		c.Fatalf("Exists watch fired: %s", event)
	default:
	}

	// And it should still work.
	_, err = zk.Create("/test", "", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL))
	c.Assert(err, IsNil)

	event = <-watch
	c.Assert(event.Type, Equals, gozk.EVENT_CREATED)
	c.Assert(event.Path, Equals, "/test")

	c.Check(gozk.CountPendingWatches(), Equals, 1)
}

func (s *S) TestWatchOnSessionExpiration(c *C) {
	c.Check(gozk.CountPendingWatches(), Equals, 0)

	zk, session := s.init(c)

	event := <-session
	c.Assert(event.Type, Equals, gozk.EVENT_SESSION)
	c.Assert(event.State, Equals, gozk.STATE_CONNECTED)

	c.Check(gozk.CountPendingWatches(), Equals, 1)

	stat, watch, err := zk.ExistsW("/test")
	c.Assert(err, IsNil)
	c.Assert(stat, IsNil)

	c.Check(gozk.CountPendingWatches(), Equals, 2)

	// Use expiration trick described in the FAQ.
	clientId := zk.ClientId()
	zk2, session2, err := gozk.ReInit(s.zkAddr, 5e9, clientId)

	for event := range session2 {
		c.Log("Event from overlapping session: ", event)
		if event.State == gozk.STATE_CONNECTED {
			// Wait for zk to process the connection.
			// Not reliable without this. :-(
			time.Sleep(1e9)
			zk2.Close()
		}
	}
	for event := range session {
		c.Log("Event from primary session: ", event)
		if event.State == gozk.STATE_EXPIRED_SESSION {
			break
		}
	}

	select {
	case event := <-watch:
		c.Assert(event.State, Equals, gozk.STATE_EXPIRED_SESSION)
	case <-time.After(3e9):
		c.Fatal("Watch event didn't fire")
	}

	event = <-watch
	c.Assert(event.Type, Equals, gozk.EVENT_CLOSED)
	c.Assert(event.State, Equals, gozk.STATE_CLOSED)

	c.Check(gozk.CountPendingWatches(), Equals, 1)
}
