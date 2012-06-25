// gozk - Zookeeper support for the Go language
//
//   https://wiki.ubuntu.com/gozk
//
// Copyright (c) 2010-2011 Canonical Ltd.
//
// Written by Gustavo Niemeyer <gustavo.niemeyer@canonical.com>
//
package gozk

/*
#cgo CFLAGS: -I/usr/include/c-client-src
#cgo LDFLAGS: -lzookeeper_mt

#include <zookeeper.h>
#include "helpers.h"
*/
import "C"

import (
	"fmt"
	"unsafe"
	"sync"
	"os"
)

// -----------------------------------------------------------------------
// Main constants and data types.

// The main ZooKeeper object, created through the Init function.
// Encapsulates all communication with ZooKeeper.
type ZooKeeper struct {
	watchChannels  map[uintptr]chan Event
	sessionWatchId uintptr
	handle         *C.zhandle_t
	mutex          sync.Mutex
}

// ClientId represents the established session in ZooKeeper.  This is only
// useful to be passed back into the ReInit function.
type ClientId struct {
	cId C.clientid_t
}

// ACL represents one access control list element, providing the permissions
// (one of PERM_*), the scheme ("digest", etc), and the id (scheme-dependent)
// for the access control mechanism in ZooKeeper.
type ACL struct {
	Perms  uint32
	Scheme string
	Id     string
}

// Event channels are used to provide notifications of changes in the
// ZooKeeper connection state and in specific node aspects.
// 
// There are two sources of events: the session channel obtained during
// initialization with Init, and any watch channels obtained
// through one of the W-suffixed functions (GetW, ExistsW, etc).
// 
// The session channel will only receive session-level events notifying
// about critical and transient changes in the ZooKeeper connection
// state (STATE_CONNECTED, STATE_EXPIRED_SESSION, etc).  On long
// running applications the session channel must *necessarily* be
// observed since certain events like session expirations require an
// explicit reconnection and reestablishment of state (or bailing out).
// Because of that, the buffer used on the session channel has a limited
// size, and a panic will occur if too many events are not collected.
//
// Watch channels enable monitoring state for nodes, and the
// moment they're fired depends on which function was called to
// create them.  Note that, unlike in other ZooKeeper interfaces,
// gozk will NOT dispatch unimportant session events such as
// STATE_ASSOCIATING, STATE_CONNECTING and STATE_CONNECTED to
// watch Event channels, since they are transient and disruptive
// to the workflow.  Critical state changes such as expirations
// are still delivered to all event channels, though, and the
// transient events may be obsererved in the session channel.
//
// Since every watch channel may receive critical session events, events
// received must not be handled blindly as if the watch requested has
// been fired.  To facilitate such tests, Events offer the Ok method,
// and they also have a good String method so they may be used as an
// os.Error value if wanted. E.g.:
//
//     event := <-watch
//     if !event.Ok() {
//         err = event
//         return
//     }
//
// Note that closed channels will deliver zeroed Event, which means
// event.Type is set to EVENT_CLOSED and event.State is set to STATE_CLOSED,
// to facilitate handling.
type Event struct {
	Type  int
	Path  string
	State int
}

// Error codes that may be used to verify the result of the
// Code method from Error.
const (
	ZOK                      = C.ZOK
	ZSYSTEMERROR             = C.ZSYSTEMERROR
	ZRUNTIMEINCONSISTENCY    = C.ZRUNTIMEINCONSISTENCY
	ZDATAINCONSISTENCY       = C.ZDATAINCONSISTENCY
	ZCONNECTIONLOSS          = C.ZCONNECTIONLOSS
	ZMARSHALLINGERROR        = C.ZMARSHALLINGERROR
	ZUNIMPLEMENTED           = C.ZUNIMPLEMENTED
	ZOPERATIONTIMEOUT        = C.ZOPERATIONTIMEOUT
	ZBADARGUMENTS            = C.ZBADARGUMENTS
	ZINVALIDSTATE            = C.ZINVALIDSTATE
	ZAPIERROR                = C.ZAPIERROR
	ZNONODE                  = C.ZNONODE
	ZNOAUTH                  = C.ZNOAUTH
	ZBADVERSION              = C.ZBADVERSION
	ZNOCHILDRENFOREPHEMERALS = C.ZNOCHILDRENFOREPHEMERALS
	ZNODEEXISTS              = C.ZNODEEXISTS
	ZNOTEMPTY                = C.ZNOTEMPTY
	ZSESSIONEXPIRED          = C.ZSESSIONEXPIRED
	ZINVALIDCALLBACK         = C.ZINVALIDCALLBACK
	ZINVALIDACL              = C.ZINVALIDACL
	ZAUTHFAILED              = C.ZAUTHFAILED
	ZCLOSING                 = C.ZCLOSING
	ZNOTHING                 = C.ZNOTHING
	ZSESSIONMOVED            = C.ZSESSIONMOVED
)

// Constants for SetLogLevel.
const (
	LOG_ERROR = C.ZOO_LOG_LEVEL_ERROR
	LOG_WARN  = C.ZOO_LOG_LEVEL_WARN
	LOG_INFO  = C.ZOO_LOG_LEVEL_INFO
	LOG_DEBUG = C.ZOO_LOG_LEVEL_DEBUG
)

// These are defined as extern.  To avoid having to declare them as
// variables here they are inlined, and correctness is ensured on
// init().

// Constants for Create's flags parameter.
const (
	EPHEMERAL = 1 << iota
	SEQUENCE
)

// Constants for ACL Perms.
const (
	PERM_READ = 1 << iota
	PERM_WRITE
	PERM_CREATE
	PERM_DELETE
	PERM_ADMIN
	PERM_ALL = 0x1f
)

// Constants for Event Type.
const (
	EVENT_CREATED = iota + 1
	EVENT_DELETED
	EVENT_CHANGED
	EVENT_CHILD
	EVENT_SESSION     = -1
	EVENT_NOTWATCHING = -2

	// Doesn't really exist in zk, but handy for use in zeroed Event
	// values (e.g. closed channels).
	EVENT_CLOSED = 0
)

// Constants for Event State.
const (
	STATE_EXPIRED_SESSION = -112
	STATE_AUTH_FAILED     = -113
	STATE_CONNECTING      = 1
	STATE_ASSOCIATING     = 2
	STATE_CONNECTED       = 3

	// Doesn't really exist in zk, but handy for use in zeroed Event
	// values (e.g. closed channels).
	STATE_CLOSED = 0
)

func init() {
	if EPHEMERAL != C.ZOO_EPHEMERAL ||
		SEQUENCE != C.ZOO_SEQUENCE ||
		PERM_READ != C.ZOO_PERM_READ ||
		PERM_WRITE != C.ZOO_PERM_WRITE ||
		PERM_CREATE != C.ZOO_PERM_CREATE ||
		PERM_DELETE != C.ZOO_PERM_DELETE ||
		PERM_ADMIN != C.ZOO_PERM_ADMIN ||
		PERM_ALL != C.ZOO_PERM_ALL ||
		EVENT_CREATED != C.ZOO_CREATED_EVENT ||
		EVENT_DELETED != C.ZOO_DELETED_EVENT ||
		EVENT_CHANGED != C.ZOO_CHANGED_EVENT ||
		EVENT_CHILD != C.ZOO_CHILD_EVENT ||
		EVENT_SESSION != C.ZOO_SESSION_EVENT ||
		EVENT_NOTWATCHING != C.ZOO_NOTWATCHING_EVENT ||
		STATE_EXPIRED_SESSION != C.ZOO_EXPIRED_SESSION_STATE ||
		STATE_AUTH_FAILED != C.ZOO_AUTH_FAILED_STATE ||
		STATE_CONNECTING != C.ZOO_CONNECTING_STATE ||
		STATE_ASSOCIATING != C.ZOO_ASSOCIATING_STATE ||
		STATE_CONNECTED != C.ZOO_CONNECTED_STATE {

		panic("OOPS: Constants don't match C counterparts")
	}
}

// AuthACL produces an ACL list containing a single ACL which uses
// the provided permissions, with the scheme "auth", and ID "", which
// is used by ZooKeeper to represent any authenticated user.
func AuthACL(perms uint32) []ACL {
	return []ACL{{perms, "auth", ""}}
}

// WorldACL produces an ACL list containing a single ACL which uses
// the provided permissions, with the scheme "world", and ID "anyone",
// which is used by ZooKeeper to represent any user at all.
func WorldACL(perms uint32) []ACL {
	return []ACL{{perms, "world", "anyone"}}
}

// -----------------------------------------------------------------------
// Event methods.

// Ok returns true in case the event reports zk as being in a usable state.
func (e Event) Ok() bool {
	// That's really it for now. Anything else seems to mean zk
	// can't be used at the moment.
	return e.State == STATE_CONNECTED
}

func (e Event) String() (s string) {
	switch e.State {
	case STATE_EXPIRED_SESSION:
		s = "ZooKeeper session expired"
	case STATE_AUTH_FAILED:
		s = "ZooKeeper authentication failed"
	case STATE_CONNECTING:
		s = "ZooKeeper connecting"
	case STATE_ASSOCIATING:
		s = "ZooKeeper still associating"
	case STATE_CONNECTED:
		s = "ZooKeeper connected"
	case STATE_CLOSED:
		s = "ZooKeeper connection closed"
	default:
		s = fmt.Sprintf("unknown ZooKeeper state %d", e.State)
	}
	if e.Type == -1 || e.Type == EVENT_SESSION {
		return
	}
	if s != "" {
		s += "; "
	}
	switch e.Type {
	case EVENT_CREATED:
		s += "path created: "
	case EVENT_DELETED:
		s += "path deleted: "
	case EVENT_CHANGED:
		s += "path changed: "
	case EVENT_CHILD:
		s += "path children changed: "
	case EVENT_NOTWATCHING:
		s += "not watching: " // !?
	case EVENT_SESSION:
		// nothing
	}
	s += e.Path
	return
}

// -----------------------------------------------------------------------
// Error interface which maps onto the ZooKeeper error codes.

type Error interface {
	String() string
	Code() int
}

type errorType struct {
	zkrc C.int
	err  os.Error
}

func newError(zkrc C.int, err os.Error) Error {
	return &errorType{zkrc, err}
}

func (error *errorType) String() (result string) {
	if error.zkrc == ZSYSTEMERROR && error.err != nil {
		result = error.err.String()
	} else {
		result = C.GoString(C.zerror(error.zkrc)) // Static, no need to free it.
	}
	return
}

// Code returns the error code that may be compared against one of
// the gozk.Z* constants.
func (error *errorType) Code() int {
	return int(error.zkrc)
}

// -----------------------------------------------------------------------
// Stat interface which maps onto the ZooKeeper Stat struct.

// We declare this as an interface rather than an actual struct because
// this way we don't have to copy data around between the real C struct
// and the Go one on every call.  Most uses will only touch a few elements,
// or even ignore the stat entirely, so that's a win.

// Stat contains detailed information about a node.
type Stat interface {
	Czxid() int64
	Mzxid() int64
	CTime() int64
	MTime() int64
	Version() int32
	CVersion() int32
	AVersion() int32
	EphemeralOwner() int64
	DataLength() int32
	NumChildren() int32
	Pzxid() int64
}

type resultStat C.struct_Stat

func (stat *resultStat) Czxid() int64 {
	return int64(stat.czxid)
}

func (stat *resultStat) Mzxid() int64 {
	return int64(stat.mzxid)
}

func (stat *resultStat) CTime() int64 {
	return int64(stat.ctime)
}

func (stat *resultStat) MTime() int64 {
	return int64(stat.mtime)
}

func (stat *resultStat) Version() int32 {
	return int32(stat.version)
}

func (stat *resultStat) CVersion() int32 {
	return int32(stat.cversion)
}

func (stat *resultStat) AVersion() int32 {
	return int32(stat.aversion)
}

func (stat *resultStat) EphemeralOwner() int64 {
	return int64(stat.ephemeralOwner)
}

func (stat *resultStat) DataLength() int32 {
	return int32(stat.dataLength)
}

func (stat *resultStat) NumChildren() int32 {
	return int32(stat.numChildren)
}

func (stat *resultStat) Pzxid() int64 {
	return int64(stat.pzxid)
}

// -----------------------------------------------------------------------
// Functions and methods related to ZooKeeper itself.

const bufferSize = 1024 * 1024

// SetLogLevel changes the minimum level of logging output generated
// to adjust the amount of information provided.
func SetLogLevel(level int) {
	C.zoo_set_debug_level(C.ZooLogLevel(level))
}

// Init initializes the communication with a ZooKeeper cluster. The provided
// servers parameter may include multiple server addresses, separated
// by commas, so that the client will automatically attempt to connect
// to another server if one of them stops working for whatever reason.
//
// The recvTimeout parameter, given in nanoseconds, allows controlling
// the amount of time the connection can stay unresponsive before the
// server will be considered problematic.
//
// Session establishment is asynchronous, meaning that this function
// will return before the communication with ZooKeeper is fully established.
// The watch channel receives events of type SESSION_EVENT when any change
// to the state of the established connection happens.  See the documentation
// for the Event type for more details.
func Init(servers string, recvTimeoutNS int64) (zk *ZooKeeper, watch chan Event, err Error) {
	zk, watch, err = internalInit(servers, recvTimeoutNS, nil)
	return
}

// Equivalent to Init, but attempt to reestablish an existing session
// identified via the clientId parameter.
func ReInit(servers string, recvTimeoutNS int64, clientId *ClientId) (zk *ZooKeeper, watch chan Event, err Error) {
	zk, watch, err = internalInit(servers, recvTimeoutNS, clientId)
	return
}

func internalInit(servers string, recvTimeoutNS int64, clientId *ClientId) (*ZooKeeper, chan Event, Error) {

	zk := &ZooKeeper{}
	zk.watchChannels = make(map[uintptr]chan Event)

	var cId *C.clientid_t
	if clientId != nil {
		cId = &clientId.cId
	}

	watchId, watchChannel := zk.createWatch(true)
	zk.sessionWatchId = watchId

	cservers := C.CString(servers)
	handle, cerr := C.zookeeper_init(cservers, C.watch_handler, C.int(recvTimeoutNS / 1e6), cId, unsafe.Pointer(watchId), 0)
	C.free(unsafe.Pointer(cservers))
	if handle == nil {
		zk.closeAllWatches()
		return nil, nil, newError(ZSYSTEMERROR, cerr)
	}
	zk.handle = handle
	runWatchLoop()
	return zk, watchChannel, nil
}

// ClientId returns the client ID for the existing session with ZooKeeper.
// This is useful to reestablish an existing session via ReInit.
func (zk *ZooKeeper) ClientId() *ClientId {
	return &ClientId{*C.zoo_client_id(zk.handle)}
}

// Close terminates the ZooKeeper interaction.
func (zk *ZooKeeper) Close() Error {

	// Protect from concurrency around zk.handle change.
	zk.mutex.Lock()
	defer zk.mutex.Unlock()

	if zk.handle == nil {
		// ZooKeeper may hang indefinitely if a handler is closed twice,
		// so we get in the way and prevent it from happening.
		return newError(ZCLOSING, nil)
	}
	rc, cerr := C.zookeeper_close(zk.handle)

	zk.closeAllWatches()
	stopWatchLoop()

	// At this point, nothing else should need zk.handle.
	zk.handle = nil

	if rc != C.ZOK {
		return newError(rc, cerr)
	}
	return nil
}

// Get returns the data and status from an existing node.  err will be nil,
// unless an error is found. Attempting to retrieve data from a non-existing
// node is an error.
func (zk *ZooKeeper) Get(path string) (data string, stat Stat, err Error) {

	cpath := C.CString(path)
	cbuffer := (*C.char)(C.malloc(bufferSize))
	cbufferLen := C.int(bufferSize)
	defer C.free(unsafe.Pointer(cpath))
	defer C.free(unsafe.Pointer(cbuffer))

	cstat := C.struct_Stat{}
	rc, cerr := C.zoo_wget(zk.handle, cpath, nil, nil,
		cbuffer, &cbufferLen, &cstat)
	if rc != C.ZOK {
		return "", nil, newError(rc, cerr)
	}
	result := C.GoStringN(cbuffer, cbufferLen)

	return result, (*resultStat)(&cstat), nil
}

// GetW works like Get but also returns a channel that will receive
// a single Event value when the data or existence of the given ZooKeeper
// node changes or when critical session events happen.  See the
// documentation of the Event type for more details.
func (zk *ZooKeeper) GetW(path string) (data string, stat Stat, watch chan Event, err Error) {

	cpath := C.CString(path)
	cbuffer := (*C.char)(C.malloc(bufferSize))
	cbufferLen := C.int(bufferSize)
	defer C.free(unsafe.Pointer(cpath))
	defer C.free(unsafe.Pointer(cbuffer))

	watchId, watchChannel := zk.createWatch(true)

	cstat := C.struct_Stat{}
	rc, cerr := C.zoo_wget(zk.handle, cpath,
		C.watch_handler, unsafe.Pointer(watchId),
		cbuffer, &cbufferLen, &cstat)
	if rc != C.ZOK {
		zk.forgetWatch(watchId)
		return "", nil, nil, newError(rc, cerr)
	}

	result := C.GoStringN(cbuffer, cbufferLen)
	return result, (*resultStat)(&cstat), watchChannel, nil
}

// Children returns the children list and status from an existing node.
// err will be nil, unless an error is found. Attempting to retrieve the
// children list from a non-existent node is an error.
func (zk *ZooKeeper) Children(path string) (children []string, stat Stat, err Error) {

	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	cvector := C.struct_String_vector{}
	cstat := C.struct_Stat{}
	rc, cerr := C.zoo_wget_children2(zk.handle, cpath, nil, nil,
		&cvector, &cstat)

	// Can't happen if rc != 0, but avoid potential memory leaks in the future.
	if cvector.count != 0 {
		children = parseStringVector(&cvector)
	}
	if rc != C.ZOK {
		err = newError(rc, cerr)
	} else {
		stat = (*resultStat)(&cstat)
	}
	return
}

// ChildrenW works like Children but also returns a channel that will
// receive a single Event value when a node is added or removed under the
// provided path or when critical session events happen.  See the documentation
// of the Event type for more details.
func (zk *ZooKeeper) ChildrenW(path string) (children []string, stat Stat, watch chan Event, err Error) {

	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	watchId, watchChannel := zk.createWatch(true)

	cvector := C.struct_String_vector{}
	cstat := C.struct_Stat{}
	rc, cerr := C.zoo_wget_children2(zk.handle, cpath,
		C.watch_handler, unsafe.Pointer(watchId),
		&cvector, &cstat)

	// Can't happen if rc != 0, but avoid potential memory leaks in the future.
	if cvector.count != 0 {
		children = parseStringVector(&cvector)
	}
	if rc != C.ZOK {
		zk.forgetWatch(watchId)
		err = newError(rc, cerr)
	} else {
		stat = (*resultStat)(&cstat)
		watch = watchChannel
	}
	return
}

func parseStringVector(cvector *C.struct_String_vector) []string {
	vector := make([]string, cvector.count)
	dataStart := uintptr(unsafe.Pointer(cvector.data))
	uintptrSize := unsafe.Sizeof(dataStart)
	for i := 0; i != len(vector); i++ {
		cpathPos := dataStart + uintptr(i)*uintptrSize
		cpath := *(**C.char)(unsafe.Pointer(cpathPos))
		vector[i] = C.GoString(cpath)
	}
	C.deallocate_String_vector(cvector)
	return vector
}

// Exists checks if a node exists at the given path.  If it does,
// stat will contain meta information on the existing node, otherwise
// it will be nil.
func (zk *ZooKeeper) Exists(path string) (stat Stat, err Error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	cstat := C.struct_Stat{}
	rc, cerr := C.zoo_wexists(zk.handle, cpath, nil, nil, &cstat)

	// We diverge a bit from the usual here: a ZNONODE is not an error
	// for an exists call, otherwise every Exists call would have to check
	// for err != nil and err.Code() != ZNONODE.
	if rc == C.ZOK {
		stat = (*resultStat)(&cstat)
	} else if rc != C.ZNONODE {
		err = newError(rc, cerr)
	}
	return
}

// ExistsW works like Exists but also returns a channel that will
// receive an Event value when a node is created in case the returned
// stat is nil and the node didn't exist, or when the existing node
// is removed. It will also receive critical session events. See the
// documentation of the Event type for more details.
func (zk *ZooKeeper) ExistsW(path string) (stat Stat, watch chan Event, err Error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	watchId, watchChannel := zk.createWatch(true)

	cstat := C.struct_Stat{}
	rc, cerr := C.zoo_wexists(zk.handle, cpath,
		C.watch_handler, unsafe.Pointer(watchId), &cstat)

	// We diverge a bit from the usual here: a ZNONODE is not an error
	// for an exists call, otherwise every Exists call would have to check
	// for err != nil and err.Code() != ZNONODE.
	switch rc {
	case ZOK:
		stat = (*resultStat)(&cstat)
		watch = watchChannel
	case ZNONODE:
		watch = watchChannel
	default:
		zk.forgetWatch(watchId)
		err = newError(rc, cerr)
	}
	return
}

// Create creates a node at the given path with the given data. The
// provided flags may determine features such as whether the node is
// ephemeral or not, or whether it should have a sequence number
// attached to it, and the provided ACLs will determine who can access
// the node and under which circumstances.
//
// The returned path is useful in cases where the created path may differ
// from the requested one, such as when a sequence number is appended
// to it due to the use of the gozk.SEQUENCE flag.
func (zk *ZooKeeper) Create(path, value string, flags int, aclv []ACL) (pathCreated string, err Error) {
	cpath := C.CString(path)
	cvalue := C.CString(value)
	defer C.free(unsafe.Pointer(cpath))
	defer C.free(unsafe.Pointer(cvalue))

	caclv := buildACLVector(aclv)
	defer C.deallocate_ACL_vector(caclv)

	// Allocate additional space for the sequence (10 bytes should be enough).
	cpathLen := C.size_t(len(path) + 32)
	cpathCreated := (*C.char)(C.malloc(cpathLen))
	defer C.free(unsafe.Pointer(cpathCreated))

	rc, cerr := C.zoo_create(zk.handle, cpath, cvalue, C.int(len(value)),
		caclv, C.int(flags), cpathCreated, C.int(cpathLen))
	if rc != C.ZOK {
		return "", newError(rc, cerr)
	}

	return C.GoString(cpathCreated), nil
}

// Set modifies the data for the existing node at the given path, replacing it
// by the provided value. If version is not -1, the operation will only
// succeed if the node is still at the given version when the replacement
// happens as an atomic operation. The returned Stat value will contain
// data for the resulting node, after the operation is performed.
//
// It is an error to attempt to set the data of a non-existing node with
// this function. In these cases, use Create instead.
func (zk *ZooKeeper) Set(path, value string, version int32) (stat Stat, err Error) {

	cpath := C.CString(path)
	cvalue := C.CString(value)
	defer C.free(unsafe.Pointer(cpath))
	defer C.free(unsafe.Pointer(cvalue))

	cstat := C.struct_Stat{}

	rc, cerr := C.zoo_set2(zk.handle, cpath, cvalue, C.int(len(value)),
		C.int(version), &cstat)
	if rc != C.ZOK {
		return nil, newError(rc, cerr)
	}

	return (*resultStat)(&cstat), nil
}

// Delete removes the node at path. If version is not -1, the operation
// will only succeed if the node is still at this version when the
// node is deleted as an atomic operation.
func (zk *ZooKeeper) Delete(path string, version int32) (err Error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	rc, cerr := C.zoo_delete(zk.handle, cpath, C.int(version))
	if rc != C.ZOK {
		return newError(rc, cerr)
	}
	return nil
}

// AddAuth adds a new authentication certificate to the ZooKeeper
// interaction. The scheme parameter will specify how to handle the
// authentication information, while the cert parameter provides the
// identity data itself. For instance, the "digest" scheme requires
// a pair like "username:password" to be provided as the certificate.
func (zk *ZooKeeper) AddAuth(scheme, cert string) Error {
	cscheme := C.CString(scheme)
	ccert := C.CString(cert)
	defer C.free(unsafe.Pointer(cscheme))
	defer C.free(unsafe.Pointer(ccert))

	data := C.create_completion_data()
	if data == nil {
		panic("Failed to create completion data")
	}
	defer C.destroy_completion_data(data)

	rc, cerr := C.zoo_add_auth(zk.handle, cscheme, ccert, C.int(len(cert)),
		C.handle_void_completion, unsafe.Pointer(data))
	if rc != C.ZOK {
		return newError(rc, cerr)
	}

	C.wait_for_completion(data)

	rc = C.int(uintptr(data.data))
	if rc != C.ZOK {
		return newError(rc, nil)
	}

	return nil
}

// ACL returns the access control list for path.
func (zk *ZooKeeper) ACL(path string) ([]ACL, Stat, Error) {

	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	caclv := C.struct_ACL_vector{}
	cstat := C.struct_Stat{}

	rc, cerr := C.zoo_get_acl(zk.handle, cpath, &caclv, &cstat)
	if rc != C.ZOK {
		return nil, nil, newError(rc, cerr)
	}

	aclv := parseACLVector(&caclv)

	return aclv, (*resultStat)(&cstat), nil
}

// SetACL changes the access control list for path.
func (zk *ZooKeeper) SetACL(path string, aclv []ACL, version int32) Error {

	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	caclv := buildACLVector(aclv)
	defer C.deallocate_ACL_vector(caclv)

	rc, cerr := C.zoo_set_acl(zk.handle, cpath, C.int(version), caclv)
	if rc != C.ZOK {
		return newError(rc, cerr)
	}

	return nil
}

func parseACLVector(caclv *C.struct_ACL_vector) []ACL {
	structACLSize := unsafe.Sizeof(C.struct_ACL{})
	aclv := make([]ACL, caclv.count)
	dataStart := uintptr(unsafe.Pointer(caclv.data))
	for i := 0; i != int(caclv.count); i++ {
		caclPos := dataStart + uintptr(i)*structACLSize
		cacl := (*C.struct_ACL)(unsafe.Pointer(caclPos))

		acl := &aclv[i]
		acl.Perms = uint32(cacl.perms)
		acl.Scheme = C.GoString(cacl.id.scheme)
		acl.Id = C.GoString(cacl.id.id)
	}
	C.deallocate_ACL_vector(caclv)

	return aclv
}

func buildACLVector(aclv []ACL) *C.struct_ACL_vector {
	structACLSize := unsafe.Sizeof(C.struct_ACL{})
	data := C.calloc(C.size_t(len(aclv)), C.size_t(structACLSize))
	if data == nil {
		panic("ACL data allocation failed")
	}

	caclv := &C.struct_ACL_vector{}
	caclv.data = (*C.struct_ACL)(data)
	caclv.count = C.int32_t(len(aclv))

	dataStart := uintptr(unsafe.Pointer(caclv.data))
	for i, acl := range aclv {
		caclPos := dataStart + uintptr(i)*structACLSize
		cacl := (*C.struct_ACL)(unsafe.Pointer(caclPos))
		cacl.perms = C.int32_t(acl.Perms)
		// C.deallocate_ACL_vector() will also handle deallocation of these.
		cacl.id.scheme = C.CString(acl.Scheme)
		cacl.id.id = C.CString(acl.Id)
	}

	return caclv
}

// -----------------------------------------------------------------------
// RetryChange utility method.

type ChangeFunc func(oldValue string, oldStat Stat) (newValue string, err os.Error)

// RetryChange runs changeFunc to attempt to atomically change path
// in a lock free manner, and retries in case there was another
// concurrent change between reading and writing the node.
//
// changeFunc must work correctly if called multiple times in case
// the modification fails due to concurrent changes, and it may return
// an error that will cause the the RetryChange function to stop and
// return an Error with code ZSYSTEMERROR and the same .String() result
// as the provided error.
//
// This mechanism is not suitable for a node that is frequently modified
// concurrently. For those cases, consider using a pessimistic locking
// mechanism.
//
// This is the detailed operation flow for RetryChange:
//
// 1. Attempt to read the node. In case the node exists, but reading it
// fails, stop and return the error found.
//
// 2. Call the changeFunc with the current node value and stat,
// or with an empty string and nil stat, if the node doesn't yet exist.
// If the changeFunc returns an error, stop and return an Error with
// ZSYSTEMERROR Code() and the same String() as the changeFunc error.
//
// 3. If the changeFunc returns no errors, use the string returned as
// the new candidate value for the node, and attempt to either create
// the node, if it didn't exist, or to change its contents at the specified
// version.  If this procedure fails due to conflicts (concurrent changes
// in the same node), repeat from step 1.  If this procedure fails with any
// other error, stop and return the error found.
//
func (zk *ZooKeeper) RetryChange(path string, flags int, acl []ACL, changeFunc ChangeFunc) (err Error) {
	for {
		oldValue, oldStat, getErr := zk.Get(path)
		if getErr != nil && getErr.Code() != ZNONODE {
			err = getErr
			break
		}
		newValue, osErr := changeFunc(oldValue, oldStat)
		if osErr != nil {
			return newError(ZSYSTEMERROR, osErr)
		} else if oldStat == nil {
			_, err = zk.Create(path, newValue, flags, acl)
			if err == nil || err.Code() != ZNODEEXISTS {
				break
			}
		} else if newValue == oldValue {
			return nil // Nothing to do.
		} else {
			_, err = zk.Set(path, newValue, oldStat.Version())
			if err == nil {
				break
			} else {
				code := err.Code()
				if code != ZBADVERSION && code != ZNONODE {
					break
				}
			}
		}
	}
	return err
}

// -----------------------------------------------------------------------
// Watching mechanism.

// The bridging of watches into Go is slightly tricky because Cgo doesn't
// yet provide a nice way to callback from C into a Go routine, so we do
// this by hand.  That bridging works the following way:
//
// Whenever a *W method is called, it will return a channel which
// outputs Event values.  Internally, a map is used to maintain references
// between an unique integer key (the watchId), and the event channel. The
// watchId is then handed to the C zookeeper library as the watch context,
// so that we get it back when events happen.  Using an integer key as the
// watch context rather than a pointer is needed because there's no guarantee
// that in the future the GC will not move objects around, and also because
// a strong reference is needed on the Go side so that the channel is not
// garbage-collected.
//
// So, this is what's done to establish the watch.  The interesting part
// lies in the other side of this logic, when events actually happen.
//
// Since Cgo doesn't allow calling back into Go, we actually fire a new
// goroutine the very first time Init is called, and allow it to block
// in a pthread condition variable within a C function. This condition
// will only be notified once a zookeeper watch callback appends new
// entries to the event list.  When this happens, the C function returns
// and we get back into Go land with the pointer to the watch data,
// including the watchId and other event details such as type and path.

var watchMutex sync.Mutex
var watchZooKeepers = make(map[uintptr]*ZooKeeper)
var watchCounter uintptr
var watchLoopCounter int

// CountPendingWatches returns the number of pending watches which have
// not been fired yet, across all ZooKeeper instances.  This is useful
// mostly as a debugging and testing aid.
func CountPendingWatches() int {
	watchMutex.Lock()
	count := len(watchZooKeepers)
	watchMutex.Unlock()
	return count
}

// createWatch creates and registers a watch, returning the watch id
// and channel.
func (zk *ZooKeeper) createWatch(session bool) (watchId uintptr, watchChannel chan Event) {
	buf := 1 // session/watch event
	if session {
		buf = 32
	}
	watchChannel = make(chan Event, buf)
	watchMutex.Lock()
	defer watchMutex.Unlock()
	watchId = watchCounter
	watchCounter += 1
	zk.watchChannels[watchId] = watchChannel
	watchZooKeepers[watchId] = zk
	return
}

// forgetWatch cleans resources used by watchId and prevents it
// from ever getting delivered. It shouldn't be used if there's any
// chance the watch channel is still visible and not closed, since
// it might mean a goroutine would be blocked forever.
func (zk *ZooKeeper) forgetWatch(watchId uintptr) {
	watchMutex.Lock()
	defer watchMutex.Unlock()
	zk.watchChannels[watchId] = nil, false
	watchZooKeepers[watchId] = nil, false
}

// closeAllWatches closes all watch channels for zk.
func (zk *ZooKeeper) closeAllWatches() {
	watchMutex.Lock()
	defer watchMutex.Unlock()
	for watchId, ch := range zk.watchChannels {
		close(ch)
		zk.watchChannels[watchId] = nil, false
		watchZooKeepers[watchId] = nil, false
	}
}

// sendEvent delivers the event to the watchId event channel.  If the
// event channel is a watch event channel, the event is delivered,
// the channel is closed, and resources are freed.
func sendEvent(watchId uintptr, event Event) {
	if event.State == STATE_CLOSED {
		panic("Attempted to send a CLOSED event")
	}
	watchMutex.Lock()
	defer watchMutex.Unlock()
	zk, ok := watchZooKeepers[watchId]
	if !ok {
		return
	}
	if event.Type == EVENT_SESSION && watchId != zk.sessionWatchId {
		switch event.State {
		case STATE_EXPIRED_SESSION, STATE_AUTH_FAILED:
		default:
			// WTF? Feels like TCP saying "dropped a dup packet, ok?"
			return
		}
	}
	ch := zk.watchChannels[watchId]
	if ch == nil {
		return
	}
	select {
	case ch <- event:
	default:
		// Channel not available for sending, which means session
		// events are necessarily involved (trivial events go
		// straight to the buffer), and the application isn't paying
		// attention for long enough to have the buffer filled up.
		// Break down now rather than leaking forever.
		if watchId == zk.sessionWatchId {
			panic("Session event channel buffer is full")
		} else {
			panic("Watch event channel buffer is full")
		}
	}
	if watchId != zk.sessionWatchId {
		zk.watchChannels[watchId] = nil, false
		watchZooKeepers[watchId] = nil, false
		close(ch)
	}
}

// runWatchLoop start the event loop to collect events from the C
// library and dispatch them into Go land.  Calling this function
// multiple times will only increase a counter, rather than
// getting multiple watch loops running.
func runWatchLoop() {
	watchMutex.Lock()
	if watchLoopCounter == 0 {
		go _watchLoop()
	}
	watchLoopCounter += 1
	watchMutex.Unlock()
}

// stopWatchLoop decrements the event loop counter. For the moment,
// the event loop doesn't actually stop, but some day we can easily
// implement termination of the loop if necessary.
func stopWatchLoop() {
	watchMutex.Lock()
	watchLoopCounter -= 1
	if watchLoopCounter == 0 {
		// Not really stopping right now, so let's just
		// avoid it from running again.
		watchLoopCounter += 1
	}
	watchMutex.Unlock()
}

// Loop and block in a C call waiting for a watch to be fired.  When
// it fires, handle the watch by dispatching it to the correct event
// channel, and go back onto waiting mode.
func _watchLoop() {
	for {
		// This will block until there's a watch event is available.
		data := C.wait_for_watch()
		event := Event{
			Type:  int(data.event_type),
			Path:  C.GoString(data.event_path),
			State: int(data.connection_state),
		}
		watchId := uintptr(data.watch_context)
		C.destroy_watch_data(data)
		sendEvent(watchId, event)
	}
}
