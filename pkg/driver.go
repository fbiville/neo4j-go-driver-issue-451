package driver

import (
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"runtime/debug"
	"strings"
	"sync"
)

var accessLock sync.RWMutex
var recoveryLock sync.Mutex

type Driver struct {
	driver                neo4j.DriverWithContext
	dbURI, user, password string
}

// Settings holds the driver settings
type Settings struct {
	ConnectionString, User, Password string
}

func executeHook(onResults ResultsHookFn, result neo4j.ResultWithContext) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("[neo4j onResults] recovered from panic: %v\n\n%s", r, string(debug.Stack()))
		}
	}()
	err = onResults(result)
	if err != nil {
		return err
	}
	return nil
}

func NewDriver(settings Settings) (*Driver, error) {
	driver, err := neo4j.NewDriverWithContext(settings.ConnectionString, neo4j.BasicAuth(settings.User, settings.Password, ""))

	if err != nil {
		return nil, err
	}

	return &Driver{driver: driver, dbURI: settings.ConnectionString, user: settings.User, password: settings.Password}, nil
}

// ResultsHookFn allows the caller to parse the query results safely
type ResultsHookFn func(result neo4j.ResultWithContext) error

// ExecuteQuery runs a query an ensured connected driver via Bolt. it it used with a hook of the original neo4j.Result object for a convenient usage
func (d *Driver) ExecuteQuery(ctx context.Context, query string, params map[string]interface{}, onResults ResultsHookFn) (err error) {
	accessLock.RLock()
	defer accessLock.RUnlock()
	return d.nonblockExecuteQuery(ctx, query, params, onResults)

}

// nonblockExecuteQuery makes sure that a recursive retry to execute a query doesn't create a more mutexes and thus a deadlock
// example is when a query executed, Rlock acquired, than Close function called, trying to aquire Lock, blocked, and then
// the function calls itself again for retry, trying to acquire Rlock, but is blocked by Lock that is blocked by previous Rlock
func (d *Driver) nonblockExecuteQuery(ctx context.Context, query string, params map[string]interface{}, onResults ResultsHookFn) (err error) {

	session, err := d.NewSession(ctx)
	if err != nil {
		return err
	}
	defer d.CloseSession(ctx, session)

	result, err := session.Run(ctx, query, params)
	if err != nil {
		if err.Error() == "Trying to create session on closed driver" || strings.HasPrefix(err.Error(), "ConnectivityError") {
			err = d.reconnect(ctx)
			if err != nil {
				return err
			}
			return d.nonblockExecuteQuery(ctx, query, params, onResults)
		}
		return err
	}
	err = executeHook(onResults, result) //<-- reporting metrics inside
	if err != nil {
		return err
	}
	return nil
}

// NewSession returns a new *connected* session only after ensuring the underlying connection is alive.
// it ensures liveliness by re-creating a new driver in case of connectivity issues.
// it returns an error in case any connectivity issue could not be resolved even after re-creating the driver.
func (d *Driver) NewSession(ctx context.Context) (neo4j.SessionWithContext, error) {
	return d.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite}), nil
}

// CloseSession closes any open resources and marks this session as unusable.
// it wraps the original neo4j.Session.Close() func with af metrics and logs
func (d *Driver) CloseSession(ctx context.Context, session neo4j.SessionWithContext) {
	session.Close(ctx)
}

// reconnect will create a new driver if current one is not connected
// it uses double verification, as two queries might both get an error and try to reconnect, one will fix the connection
// the other doesn't need to reconnect
func (d *Driver) reconnect(ctx context.Context) error {
	recoveryLock.Lock()
	defer recoveryLock.Unlock()
	if err := d.driver.VerifyConnectivity(ctx); err == nil {
		return nil

	}

	driver, err := NewDriver(Settings{d.dbURI, d.user, d.password})
	if err != nil {
		return err
	}
	d.nonblockClose(ctx) //close old driver
	d.driver = driver.driver
	return nil
}

func (d *Driver) nonblockClose(ctx context.Context) {
	if d.driver == nil {
		return
	}
	d.driver.Close(ctx)
}

// Close safely closes the underlying open connections to the DB.
func (d *Driver) Close(ctx context.Context) {
	accessLock.Lock()
	defer accessLock.Unlock()
	d.nonblockClose(ctx)
}
