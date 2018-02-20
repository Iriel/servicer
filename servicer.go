// Package servicer provides a service monitor/control/handle mechanism.
//
// A service is a loosely monitored and managed goroutine with a consistent
// interface for lifecycle management, status query and optional command
// execution.
//
// State
//
// A service instance goes through the following state transition once. Some
// states may be skipped.
//
//   Created -> Starting -> Running -> Stopping -> Stopped
//
// The service may optionally provide additional details along with the
// state.
//
// Created indicates that the service has not yet been launched,
// which also means that the service cannot execute commands (so calls to
// Handle.Do will block).
//
// Starting indicates that the service launcher has executed the service
// goroutine.
//
// Running indicates that the service has acknowledged that it is active.
//
// Stopping indicates that the service has acknowledged that it must stop.
//
// Stopped indicates that the service method has exited (or was never
// started) and the service has shut down.
//
// Monitoring
//
// The Servicer interface provides methods to get the current status of the
// service (at least as seen by the monitor) as well as to be notified
// when the state reaches at least a particular state milestone. In addition
// there are special milestone statuses:
//
//   StatusQuery       - Get the current state
//   StatusNext        - Wait until the status is updated (even if unchanged)
//   StatusStateChange - Wait until the next state change
//
// All such methods return immediately with the current state if the service
// is Stopped.
//
// In addition a service user may Subscribe for status events, which will
// be delivered to a specified channel as they occur. The channel will be
// closed when the service terminates (or if the channel was not available
// if a blocking subscription is not requested).
//
// Usage
//
// For simple services the Servicer or Handle interface can be used directly.
// More complex services may wish to expose only the Servicer part directly
// and use the Handle instance internally so as to implement more specific
// command methods.
package servicer

import (
	"errors"
	"fmt"
	"time"
)

// State indicates the basic state of a service in the monitor state machine.
// The main states are ordered such that the state may only increase.
type State int

const (
	// Created services are not yet running
	Created State = iota
	// Starting services have been launched
	Starting
	// Running services have confirmed they are running
	Running
	// Stopping services have confirmed they are to stop
	Stopping
	// Stopped services have ended
	Stopped

	// Unknown state indicates the state monitor was not available
	Unknown State = 0x6000

	// StatusQuery is a condtion to return the current state immediately
	StatusQuery State = 0x8000
	// StatusNext is a condtion to return on the next status update
	StatusNext State = 0x8001
	// StatusStateChange is a condtion to return only when the state changes
	StatusStateChange State = 0x8002
)

//go:generate stringer -type=State

const (
	// statusSubscribe is used internally to create subscriptions
	statusSubscribe State = 0xC000
)

// Status captures the reported status of a service at a point in time.
type Status struct {
	// The service state.
	State State

	// Service-specific status detail.
	Detail interface{}

	// The time of this status change. Note that the update time is not
	// constrained by the monitor.
	Update time.Time
}

func (s Status) String() string {
	return fmt.Sprintf("%s (%v) @%s", s.State, s.Detail, s.Update)
}

// Servicer provides an external representation of a service instance.
// The behavior of operations against a closed servicer are implementation
// defined.
type Servicer interface {
	// Status returns the current status of the service. The returned
	// state may be Unknown if the service is no longer monitored.
	Status() Status

	// WaitFor blocks until the specified condition is met or exceeded.
	// The status at that time is returned.
	// The returned state may be Unknown if the service is no longer
	// monitored.
	WaitFor(condition State) Status

	// Subscribe requests that the current status and all subsequent
	// statuses be written to a channel. The channel will be closed
	// after the Stopped status is written. The service monitor will
	// also close the channel if blocking is false and the channel
	// did not accept a status.
	Subscribe(c chan<- Status, blocking bool)

	// Start the service if not already started. This may be called
	// multiple times, including after Stop().
	Start()

	// Stop the service if not already stopped. This may be called
	// multiple times, and may be called before Start().
	Stop()

	// Close the service, this may only be called once and may prevent
	// future operations against the service from completing.
	Close()
}

// Handle extends Servicer by adding a command channel to the service.
type Handle interface {
	// All of the methods of the Servicer interface are included.
	Servicer

	// Servicer returns the more limited Servicer object for the handle.
	Servicer() Servicer

	// IsStarted returns whether the service has given a start or stop
	// request. This is based on whether the request was made, not
	// on the service status.
	WasStarted() bool

	// IsStopping returns whether the service has given a stop request.
	// This is based on whether the request was made, not on the service
	// status.
	WasStopped() bool

	// Do executes a command within the service, it returns the results
	// of the command execution from the service.
	Do(interface{}) (interface{}, error)

	// DoImmediate executes a command within the service only if the
	// service is not performing any other action. The second return
	// value is true iff the command was submitted.
	DoImmediate(interface{}) (interface{}, bool, error)

	// Closes the command channel used by Do, this must be called
	// only once.
	CloseDo()
}

// Commanderdprovides in-service command handling.
type Commander interface {
	// Do the command, the parameter and returns map directly to Handle.Do.
	Do(request interface{}) (response interface{}, err error)
}

// CommanderFunc allows a function to be used directly as a Commander.
type CommanderFunc func(request interface{}) (response interface{}, err error)

func (f CommanderFunc) Do(request interface{}) (response interface{}, err error) {
	return f(request)
}

// Response captures the result of a service command.
type Response struct {
	// Result is the actual result object
	Result interface{}

	// Error is an error if the command failed, or nil
	Error error
}

// Command encapsulates a requested service command.
type Command struct {
	// Reply is the queue onto which the command response is to be written.
	Reply chan<- Response

	// Request represents the command to perform.
	Request interface{}
}

// Control is the service end of the service monitor connection.
type Control struct {
	// Stop is a close-only channel to signal shutdown.
	Stop chan int

	// Command delivers Do commands to the service.
	Command <-chan Command

	// Status delivers status updates to the monitor.
	status chan<- Status
}

// Status sets the current service status. Attempting to post
// a Stopped status will cause a panic.
// Note that the status monitor will ignore invalid status changes.
func (c *Control) Status(status Status) {
	if status.State == Stopped {
		panic("Services cannot force Stopped state")
	}
	c.status <- status
}

// State sets the status with a particular state and detail and the current
// time. Using a state of Stopped will cause a panic.
// This is a convenience wrapper for Control.Status
func (c *Control) State(state State, detail interface{}) {
	if state == Stopped {
		panic("Services cannot force Stopped state")
	}
	c.Status(Status{state, detail, time.Now()})
}

// Running set the state to Running with nil detail.
// This is a convenience wrapper for Control.State(Running, nil)
func (c *Control) Running() {
	c.State(Running, nil)
}

// RunningDetail sets the state to Running with a specific detail.
// This is a convenience wrapper for Control.State(Running, detail)
func (c *Control) RunningDetail(detail interface{}) {
	c.State(Running, detail)
}

// Stopping sets the state to Stopping with nil detail.
// This is a convenience wrapper for Control.State(Stopping, nil)
func (c *Control) Stopping() {
	c.State(Stopping, nil)
}

// Perform handles execution from the Command queue, including discarding
// the Command channel if it is closed. The commander will be
// invoked and the result sent back to the Command's reply, including
// if the handler causes a panic().
func (c *Control) Perform(command Command, ok bool, commander Commander) {
	if !ok {
		c.Command = nil
		return
	}
	defer close(command.Reply)
	defer func(c chan<- Response) {
		err := recover()
		if err != nil {
			select {
			case c <- Response{nil, fmt.Errorf("Command panic: %s", err)}:
			default:
			}
		}
	}(command.Reply)
	result, err := commander.Do(command.Request)
	command.Reply <- Response{result, err}
}

// closeWithoutPanic closes a signal channel (if open) without a panic.
// It returns true only if the close was executed and successful.
func closeWithoutPanic(c chan int) (ok bool) {
	select {
	case _ = <-c:
		// Already closed
		return false
	default:
	}
	defer func() { _ = recover() }()
	close(c)
	return true
}

// Close invalidates the Control, and indicates that the service has Stopped.
// Close causes an implicit Status update to Stopped with no detail, but a
// Service may explicitly set a Stopped status beforehand if a detail is
// required.
func (c *Control) close() {
	// Make it clear that the control channel closed
	closeWithoutPanic(c.Stop)
	defer func() { _ = recover() }()
	close(c.status)
}

// IsStopped returns whether the service has received a stop notification.
func (c *Control) IsStopped() bool {
	select {
	case _ = <-c.Stop:
		return true

	default:
		return false
	}
}

// statusRequest carries a request for status notification from the monitor.
type statusRequest struct {
	// reply is used to return a response to the requestor.
	reply chan<- Status
	// condition represents the necessary condition for a reply.
	condition State
	// blocking indicates if submissions to the reply queue can block.
	blocking bool
	// TODO add a deadline (requires timer in monitor)
}

// canApplyStatus gets whether a new status is permitted to overwrite the
// current status.
func canApplyStatus(current Status, new Status) bool {
	if current.State > new.State {
		// Cannot go backwards
		return false
	}
	switch current.State {
	case Stopped:
		// Can only stop once
		return false

	case Created, Starting, Running, Stopping:
		return true

	default:
		// Cannot transition to unknown state
		return false
	}
}

// Serviceunner functions launch an actual service when it is started. It
// be called at most once for any service.  The service itself is expected
// to adhere to the following basic protocol to ensure that it detects and
// reacts to control events. The launcher will ensure that a Stopped
// status update is delivered when the service exits, using the
// result of the function as the detail.
//
//   func (s *someService) Run(control service.Control) interface{} {
//     control.Running()
//     commander := CommanderFunc(func(i interface{}) (interface{}, error) {
//       ... // Commander logic
//     })
//   Loop:
//     for {
//       select {
//       case <- control.Stop:
//         // Stop request received
//         control.Stopping()
//         break Loop
//       case cmd, ok := control.Command:
//         // Command received
//         control.Perform(cmd, ok, commander);
//       // Actual service logic here
//       }
//     }
//     return "Exited"
//   }
type Service interface {
	// Execute the service using the specified control handle. The
	// return value is used as detail for the Stopped event.
	Run(control Control) (detail interface{})
}

// ServiceFunc allows an appropriate function to be used as a Service.
//
//   handle := service.New(service.ServiceFunc(someFunction))
type ServiceFunc func(control Control) (detail interface{})

func (f ServiceFunc) Run(control Control) (detail interface{}) {
	return f(control)
}

// recoveryCloser closes a Control if it recovers from a panic.
func recoveryCloser(control Control) {
	r := recover()
	if r != nil {
		control.close()
	}
}

// recoveryStopper delivers a Stopped state if it recovers from a panic.
func recoveryStopper(control Control) {
	r := recover()
	if r != nil {
		err := fmt.Sprintf("Caught panic: %s", r)
		control.status <- Status{Stopped, err, time.Now()}
	}
}

// runService is executed in the coroutine for the service and takes
// care of state updates for Starting and Stopped, and closing the
// Control.
func runService(control Control, service Service) {
	defer control.close()
	defer recoveryStopper(control)
	control.status <- Status{Starting, nil, time.Now()}
	detail := service.Run(control)
	control.status <- Status{Stopped, detail, time.Now()}
}

// launchService executes a Service within a goroutine. The
// Control will be closed if a panic occurs while starting the service.
func launchService(control Control, service Service) {
	defer recoveryCloser(control)
	go runService(control, service)
}

// closeStatusRequest closes the reply queue for a status request,
// swallowing any panic that occurs.
func closeStatusRequest(req *statusRequest) {
	defer func() { _ = recover() }()
	if req.reply != nil {
		reply := req.reply
		req.reply = nil
		close(reply)
	}
}

// closeStatusRequestOnPanic closes a status request if a panic is recovered.
func closeStatusRequestOnPanic(req *statusRequest) {
	x := recover()
	if x != nil {
		closeStatusRequest(req)
	}
}

// doStatusResponse handles delivery of a status response and subsequent
// potential close of the request. The return value indicates if the
// response is still open afterwards.
func doStatusResponse(req *statusRequest, status Status, keepOpen bool) (stillOpen bool) {
	defer closeStatusRequestOnPanic(req)
	if req.blocking {
		req.reply <- status
	} else {
		select {
		case req.reply <- status:
		default:
			keepOpen = false
		}
	}
	if !keepOpen {
		closeStatusRequest(req)
	}
	return keepOpen
}

// statusMonitor manages the service state machine, including starting
// the service and responding to status requests.
func statusMonitor(sin <-chan Status, rin <-chan statusRequest, status Status,
	start <-chan int, control Control, service Service) {
	current := status
	pending := make([]statusRequest, 0, 10)

	cstop := control.Stop

	for sin != nil || rin != nil {
	Select:
		select {
		case <-start:
			// Saw start signal, must launch service
			launchService(control, service)
			start, cstop, control, service = nil, nil, Control{}, nil

		case <-cstop:
			// Saw stop signal for unstarted service
			control.close()
			start, cstop, control, service = nil, nil, Control{}, nil

		case req, ok := <-rin:
			if !ok {
				rin = nil
				break
			}

			if current.State == Stopped || req.condition <= current.State {
				req.condition = StatusQuery
			}

			switch req.condition {
			case statusSubscribe:
				if !doStatusResponse(&req, current, true) {
					break Select
				}

			default:
				fallthrough

			case StatusQuery:
				doStatusResponse(&req, current, false)
				break Select

			case Created, Starting, Running, Stopping, Stopped:
				// Pend for future known state
			case StatusStateChange, StatusNext:
				// Pend for future unknown state
			}
			// Must pend for now
			pending = append(pending, req)

		case upd, ok := <-sin:
			if !ok {
				sin = nil
				if current.State == Stopped {
					break
				}
				upd = Status{Stopped, nil, time.Now()}
			}
			if !canApplyStatus(current, upd) {
				break
			}

			stateChange := current.State != upd.State
			current = upd
			newState := upd.State

		PendLoop:
			for i, pl := 0, len(pending); i < pl; i++ {
				pr := pending[i]
				c := pr.condition

				if newState == Stopped {
					c = StatusQuery
				}

				switch c {
				case StatusStateChange:
					if !stateChange {
						continue PendLoop
					}

				case StatusNext, statusSubscribe, StatusQuery:
					// Always send update

				default:
					if c > newState {
						continue PendLoop
					}
				}

				stillOpen := doStatusResponse(&pr, current, c == statusSubscribe)
				if !stillOpen {
					pl--
					if i < pl {
						pending[i] = pending[pl]
					}
					i--
					pending[pl] = statusRequest{}
					pending = pending[0:pl]
				}
			}
		}
	}

	for _, pr := range pending {
		closeStatusRequest(&pr)
	}
}

// servicer is the core implementation of the Servicer interface.
type servicer struct {
	// start is the close-only channel for start notification
	start chan int

	// start is the close-only channel for stop notification
	stop chan int

	// status delivers status queries to the monitor
	status chan<- statusRequest
}

func (s *servicer) Start() {
	closeWithoutPanic(s.start)
}

func (s *servicer) Stop() {
	closeWithoutPanic(s.stop)
}

func (s *servicer) Close() {
	s.Stop()
	s.Start()
	close(s.status)
}

func (s *servicer) Status() Status {
	return s.WaitFor(StatusQuery)
}

func (s *servicer) WaitFor(condition State) Status {
	switch condition {
	case Created, Starting, Running, Stopping, Stopped:
		// Normal statuses
	case StatusStateChange, StatusNext, StatusQuery:
		// Query statuses
	default:
		// Don't permit unknown/unhelpful conditions
		condition = StatusQuery
	}

	// Buffer size 1 to not block reply
	reply := make(chan Status, 1)
	req := statusRequest{reply, condition, false}
	s.status <- req
	// Now wait for the reply

	resp, ok := <-reply
	if !ok {
		return Status{Unknown, "Status unavailable", time.Now()}
	}
	return resp
}

func (s *servicer) Subscribe(reply chan<- Status, blocking bool) {
	req := statusRequest{reply, statusSubscribe, blocking}
	s.status <- req
}

// handle extends the servicer to include the command channel
type handle struct {
	*servicer
	command chan<- Command
}

func (h *handle) WasStarted() bool {
	select {
	case <-h.start:
		return true

	case <-h.stop:
		return true

	default:
		return false
	}
}

func (h *handle) WasStopped() bool {
	select {
	case <-h.stop:
		return true

	default:
		return false
	}
}

func (h *handle) Do(request interface{}) (response interface{}, err error) {
	// One size buffer to avoid blocking
	reply := make(chan Response, 1)
	cmd := Command{reply, request}
	select {
	case <-h.stop:
		return nil, errors.New("Service is stopped")

	case h.command <- cmd:
	}
	resp, ok := <-reply
	if !ok {
		return nil, errors.New("No reply recieved from service")
	}
	return resp.Result, resp.Error
}

func (h *handle) DoImmediate(request interface{}) (response interface{}, submitted bool, err error) {
	// One size buffer to avoid blocking
	reply := make(chan Response, 1)
	cmd := Command{reply, request}
	select {
	case <-h.stop:
		return nil, false, errors.New("Service is stopped")

	case h.command <- cmd:

	default:
		return nil, false, nil
	}
	resp, ok := <-reply
	if !ok {
		return nil, true, errors.New("No reply recieved from service")
	}
	return resp.Result, true, resp.Error
}

func (h *handle) CloseDo() {
	close(h.command)
}

func (h *handle) Servicer() Servicer {
	return h.servicer
}

// New creates a new service and returns a Handle to manage the service.
// The Handle/Servicer must be Close()d when in order to stop its monitor
// goroutine.
func New(service Service) Handle {
	stop := make(chan int)
	start := make(chan int)
	command := make(chan Command)
	status := make(chan Status, 3)
	statusRequest := make(chan statusRequest)

	servicer := servicer{start, stop, statusRequest}
	handle := handle{&servicer, command}
	control := Control{stop, command, status}
	initialStatus := Status{Created, nil, time.Now()}

	go statusMonitor(status, statusRequest, initialStatus,
		start, control, service)

	return &handle
}
