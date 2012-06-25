package gozk_test

import (
	. "launchpad.net/gocheck"
	"gozk"
	"os"
)

func (s *S) TestRetryChangeCreating(c *C) {
	zk, _ := s.init(c)

	err := zk.RetryChange("/test", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL),
		func(data string, stat gozk.Stat) (string, os.Error) {
			c.Assert(data, Equals, "")
			c.Assert(stat, IsNil)
			return "new", nil
		})
	c.Assert(err, IsNil)

	data, stat, err := zk.Get("/test")
	c.Assert(err, IsNil)
	c.Assert(stat, NotNil)
	c.Assert(stat.Version(), Equals, int32(0))
	c.Assert(data, Equals, "new")

	acl, _, err := zk.ACL("/test")
	c.Assert(err, IsNil)
	c.Assert(acl, Equals, gozk.WorldACL(gozk.PERM_ALL))
}

func (s *S) TestRetryChangeSetting(c *C) {
	zk, _ := s.init(c)

	_, err := zk.Create("/test", "old", gozk.EPHEMERAL,
		gozk.WorldACL(gozk.PERM_ALL))
	c.Assert(err, IsNil)

	err = zk.RetryChange("/test", gozk.EPHEMERAL, []gozk.ACL{},
		func(data string, stat gozk.Stat) (string, os.Error) {
			c.Assert(data, Equals, "old")
			c.Assert(stat, NotNil)
			c.Assert(stat.Version(), Equals, int32(0))
			return "brand new", nil
		})
	c.Assert(err, IsNil)

	data, stat, err := zk.Get("/test")
	c.Assert(err, IsNil)
	c.Assert(stat, NotNil)
	c.Assert(stat.Version(), Equals, int32(1))
	c.Assert(data, Equals, "brand new")

	// ACL was unchanged by RetryChange().
	acl, _, err := zk.ACL("/test")
	c.Assert(err, IsNil)
	c.Assert(acl, Equals, gozk.WorldACL(gozk.PERM_ALL))
}

func (s *S) TestRetryChangeUnchangedValueDoesNothing(c *C) {
	zk, _ := s.init(c)

	_, err := zk.Create("/test", "old", gozk.EPHEMERAL,
		gozk.WorldACL(gozk.PERM_ALL))
	c.Assert(err, IsNil)

	err = zk.RetryChange("/test", gozk.EPHEMERAL, []gozk.ACL{},
		func(data string, stat gozk.Stat) (string, os.Error) {
			c.Assert(data, Equals, "old")
			c.Assert(stat, NotNil)
			c.Assert(stat.Version(), Equals, int32(0))
			return "old", nil
		})
	c.Assert(err, IsNil)

	data, stat, err := zk.Get("/test")
	c.Assert(err, IsNil)
	c.Assert(stat, NotNil)
	c.Assert(stat.Version(), Equals, int32(0)) // Unchanged!
	c.Assert(data, Equals, "old")
}

func (s *S) TestRetryChangeConflictOnCreate(c *C) {
	zk, _ := s.init(c)

	changeFunc := func(data string, stat gozk.Stat) (string, os.Error) {
		switch data {
		case "":
			c.Assert(stat, IsNil)
			_, err := zk.Create("/test", "conflict", gozk.EPHEMERAL,
				gozk.WorldACL(gozk.PERM_ALL))
			c.Assert(err, IsNil)
			return "<none> => conflict", nil
		case "conflict":
			c.Assert(stat, NotNil)
			c.Assert(stat.Version(), Equals, int32(0))
			return "conflict => new", nil
		default:
			c.Fatal("Unexpected node data: " + data)
		}
		return "can't happen", nil
	}

	err := zk.RetryChange("/test", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL),
		changeFunc)
	c.Assert(err, IsNil)

	data, stat, err := zk.Get("/test")
	c.Assert(err, IsNil)
	c.Assert(data, Equals, "conflict => new")
	c.Assert(stat, NotNil)
	c.Assert(stat.Version(), Equals, int32(1))
}

func (s *S) TestRetryChangeConflictOnSetDueToChange(c *C) {
	zk, _ := s.init(c)

	_, err := zk.Create("/test", "old", gozk.EPHEMERAL,
		gozk.WorldACL(gozk.PERM_ALL))
	c.Assert(err, IsNil)

	changeFunc := func(data string, stat gozk.Stat) (string, os.Error) {
		switch data {
		case "old":
			c.Assert(stat, NotNil)
			c.Assert(stat.Version(), Equals, int32(0))
			_, err := zk.Set("/test", "conflict", 0)
			c.Assert(err, IsNil)
			return "old => new", nil
		case "conflict":
			c.Assert(stat, NotNil)
			c.Assert(stat.Version(), Equals, int32(1))
			return "conflict => new", nil
		default:
			c.Fatal("Unexpected node data: " + data)
		}
		return "can't happen", nil
	}

	err = zk.RetryChange("/test", gozk.EPHEMERAL, []gozk.ACL{}, changeFunc)
	c.Assert(err, IsNil)

	data, stat, err := zk.Get("/test")
	c.Assert(err, IsNil)
	c.Assert(data, Equals, "conflict => new")
	c.Assert(stat, NotNil)
	c.Assert(stat.Version(), Equals, int32(2))
}

func (s *S) TestRetryChangeConflictOnSetDueToDelete(c *C) {
	zk, _ := s.init(c)

	_, err := zk.Create("/test", "old", gozk.EPHEMERAL,
		gozk.WorldACL(gozk.PERM_ALL))
	c.Assert(err, IsNil)

	changeFunc := func(data string, stat gozk.Stat) (string, os.Error) {
		switch data {
		case "old":
			c.Assert(stat, NotNil)
			c.Assert(stat.Version(), Equals, int32(0))
			err := zk.Delete("/test", 0)
			c.Assert(err, IsNil)
			return "old => <deleted>", nil
		case "":
			c.Assert(stat, IsNil)
			return "<deleted> => new", nil
		default:
			c.Fatal("Unexpected node data: " + data)
		}
		return "can't happen", nil
	}

	err = zk.RetryChange("/test", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_READ),
		changeFunc)
	c.Assert(err, IsNil)

	data, stat, err := zk.Get("/test")
	c.Assert(err, IsNil)
	c.Assert(data, Equals, "<deleted> => new")
	c.Assert(stat, NotNil)
	c.Assert(stat.Version(), Equals, int32(0))

	// Should be the new ACL.
	acl, _, err := zk.ACL("/test")
	c.Assert(err, IsNil)
	c.Assert(acl, Equals, gozk.WorldACL(gozk.PERM_READ))
}

func (s *S) TestRetryChangeErrorInCallback(c *C) {
	zk, _ := s.init(c)

	err := zk.RetryChange("/test", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL),
		func(data string, stat gozk.Stat) (string, os.Error) {
			return "don't use this", os.NewError("BOOM!")
		})
	c.Assert(err, NotNil)
	c.Assert(err.Code(), Equals, gozk.ZSYSTEMERROR)
	c.Assert(err.String(), Equals, "BOOM!")

	stat, err := zk.Exists("/test")
	c.Assert(err, IsNil)
	c.Assert(stat, IsNil)
}

func (s *S) TestRetryChangeFailsReading(c *C) {
	zk, _ := s.init(c)

	_, err := zk.Create("/test", "old", gozk.EPHEMERAL,
		gozk.WorldACL(gozk.PERM_WRITE)) // Write only!
	c.Assert(err, IsNil)

	var called bool
	err = zk.RetryChange("/test", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL),
		func(data string, stat gozk.Stat) (string, os.Error) {
			called = true
			return "", nil
		})
	c.Assert(err, NotNil)
	c.Assert(err.Code(), Equals, gozk.ZNOAUTH)

	stat, err := zk.Exists("/test")
	c.Assert(err, IsNil)
	c.Assert(stat, NotNil)
	c.Assert(stat.Version(), Equals, int32(0))

	c.Assert(called, Equals, false)
}

func (s *S) TestRetryChangeFailsSetting(c *C) {
	zk, _ := s.init(c)

	_, err := zk.Create("/test", "old", gozk.EPHEMERAL,
		gozk.WorldACL(gozk.PERM_READ)) // Read only!
	c.Assert(err, IsNil)

	var called bool
	err = zk.RetryChange("/test", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL),
		func(data string, stat gozk.Stat) (string, os.Error) {
			called = true
			return "", nil
		})
	c.Assert(err, NotNil)
	c.Assert(err.Code(), Equals, gozk.ZNOAUTH)

	stat, err := zk.Exists("/test")
	c.Assert(err, IsNil)
	c.Assert(stat, NotNil)
	c.Assert(stat.Version(), Equals, int32(0))

	c.Assert(called, Equals, true)
}

func (s *S) TestRetryChangeFailsCreating(c *C) {
	zk, _ := s.init(c)

	_, err := zk.Create("/test", "old", gozk.EPHEMERAL,
		gozk.WorldACL(gozk.PERM_READ)) // Read only!
	c.Assert(err, IsNil)

	var called bool
	err = zk.RetryChange("/test/sub", gozk.EPHEMERAL,
		gozk.WorldACL(gozk.PERM_ALL),
		func(data string, stat gozk.Stat) (string, os.Error) {
			called = true
			return "", nil
		})
	c.Assert(err, NotNil)
	c.Assert(err.Code(), Equals, gozk.ZNOAUTH)

	stat, err := zk.Exists("/test/sub")
	c.Assert(err, IsNil)
	c.Assert(stat, IsNil)

	c.Assert(called, Equals, true)
}
