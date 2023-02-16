package driver_test

import (
	"context"
	"errors"
	. "github.com/fbiville/neo4j-go-driver-issue-451/pkg"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/suite"
	"sync"
	"testing"
)

func TestIssue451(t *testing.T) {
	suite.Run(t, new(DriverTestSuite))
}

var connectionSettings = Settings{
	ConnectionString: "neo4j://localhost",
	User:             "neo4j",
	Password:         "letmein!",
}

type DriverTestSuite struct {
	suite.Suite
	ctx    context.Context
	driver *Driver
}

func (s *DriverTestSuite) SetupSuite() {
	s.ctx = context.Background()
}

func (s *DriverTestSuite) TearDownSuite() {
	s.ctx.Done()
}

func (s *DriverTestSuite) connectToNeo() {
	require := s.Require()
	driver, err := NewDriver(connectionSettings)
	require.NoError(err)
	s.driver = driver
}

func (s *DriverTestSuite) SetupTest() {
	s.connectToNeo()
}

func (s *DriverTestSuite) TestMultithreadedQueryRequestsWithConnectionRecovery() {
	s.driver.Close(s.ctx)
	count := 1000
	wg := &sync.WaitGroup{}
	wg.Add(count)

	for i := 0; i < count; i++ {

		go func(wg *sync.WaitGroup, i int, s *DriverTestSuite) {
			defer wg.Done()
			s.driver.Close(s.ctx)
			err := s.executeSimpleQuery()
			s.Require().NoError(err)

		}(wg, i, s)
	}
	wg.Wait()

}

func executeSimpleQuery(ctx context.Context, driver *Driver) error {
	return driver.ExecuteQuery(ctx, "CREATE (test:Test) return true", map[string]interface{}{}, func(result neo4j.ResultWithContext) error {
		var record *neo4j.Record
		result.NextRecord(ctx, &record)
		if record == nil || len(record.Values) == 0 {
			return errors.New("no records")
		}
		_, ok := record.Values[0].(bool)
		if !ok {
			return errors.New("expected value to be bool")
		}
		return nil
	})
}

func (s *DriverTestSuite) executeSimpleQuery() error {
	return executeSimpleQuery(s.ctx, s.driver)
}
