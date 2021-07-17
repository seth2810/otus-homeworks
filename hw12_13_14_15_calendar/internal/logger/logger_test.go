package logger

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type LoggerTestSuite struct {
	suite.Suite
	output *os.File
}

func (s *LoggerTestSuite) BeforeTest(suiteName, testName string) {
	s.output, _ = os.CreateTemp(os.TempDir(), suiteName)
}

func (s *LoggerTestSuite) TearDownTest() {
	require.NoError(s.T(), os.Remove(s.output.Name()))
}

func (s *LoggerTestSuite) TestWrongLevel() {
	_, err := New("wrong", s.output.Name())
	require.ErrorIs(s.T(), err, errFailedToSetLevel)
}

func (s *LoggerTestSuite) TestOutputNotExist() {
	output := "/dev/nil"
	_, err := New("info", output)

	require.Contains(s.T(), err.Error(), fmt.Sprintf("couldn't open sink %q", output))
}

func (s *LoggerTestSuite) TestOutputNoPermission() {
	output := "/etc/sudoers"
	_, err := New("info", output)

	require.EqualError(
		s.T(), err,
		fmt.Sprintf("couldn't open sink %q: open %s: permission denied", output, output),
	)
}

func (s *LoggerTestSuite) TestMethods() {
	infoLine := "info line"
	errorLine := "error line"
	log, err := New("info", s.output.Name())

	require.NoError(s.T(), err)

	log.Info(infoLine)
	log.Error(errorLine)

	data, err := io.ReadAll(s.output)

	require.NoError(s.T(), err)
	require.Equal(s.T(), data, []byte(fmt.Sprintf("%s\n%s\n", infoLine, errorLine)))
}

func TestLogger(t *testing.T) {
	suite.Run(t, new(LoggerTestSuite))
}
