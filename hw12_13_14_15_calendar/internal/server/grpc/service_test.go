package internalgrpc_test

import (
	"context"
	"log"
	"net"
	"testing"
	"time"

	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcvalidator "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"github.com/pioz/faker"
	"github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/calendar"
	"github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/logger"
	internalgrpc "github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/server/grpc"
	"github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/server/grpc/pb"
	memorystorage "github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/storage/memory"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type dialer func(context.Context, string) (net.Conn, error)

func createDialer(app internalgrpc.Application) dialer {
	listener := bufconn.Listen(1024 * 1024)

	server := grpc.NewServer(
		grpc.UnaryInterceptor(grpcmiddleware.ChainUnaryServer(
			grpcvalidator.UnaryServerInterceptor(),
		)),
	)

	pb.RegisterCalendarServiceServer(server, internalgrpc.NewService(app))

	go func() {
		if err := server.Serve(listener); err != nil {
			log.Fatal(err)
		}
	}()

	return func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
}

type GRPCTestSuite struct {
	suite.Suite
	conn   *grpc.ClientConn
	client pb.CalendarServiceClient
}

func (s *GRPCTestSuite) SetupSuite() {
	storage := memorystorage.New()
	logger, _ := logger.New("info", "/dev/stdout")
	conn, err := grpc.DialContext(
		context.TODO(),
		"",
		grpc.WithInsecure(),
		grpc.WithContextDialer(createDialer(calendar.New(logger, storage))),
	)

	require.NoError(s.T(), err)

	s.conn = conn
	s.client = pb.NewCalendarServiceClient(conn)
}

func (s *GRPCTestSuite) TearDownSuite() {
	defer s.conn.Close()
}

func (s *GRPCTestSuite) TestListErrors() {
	test := struct {
		req           *pb.ListEventsRequest
		expectedError string
	}{&pb.ListEventsRequest{}, "rpc error: code = InvalidArgument desc = invalid ListEventsRequest.Date: value is required"}

	_, err := s.client.ListDayEvents(context.TODO(), test.req)
	require.EqualError(s.T(), err, test.expectedError)

	_, err = s.client.ListWeekEvents(context.TODO(), test.req)
	require.EqualError(s.T(), err, test.expectedError)

	_, err = s.client.ListMonthEvents(context.TODO(), test.req)
	require.EqualError(s.T(), err, test.expectedError)
}

func (s *GRPCTestSuite) TestEmpty() {
	res, err := s.client.ListDayEvents(context.TODO(), &pb.ListEventsRequest{
		Date: timestamppb.Now(),
	})
	require.NoError(s.T(), err)
	require.Len(s.T(), res.GetEvents(), 0)

	res, err = s.client.ListWeekEvents(context.TODO(), &pb.ListEventsRequest{
		Date: timestamppb.Now(),
	})
	require.NoError(s.T(), err)
	require.Len(s.T(), res.GetEvents(), 0)

	res, err = s.client.ListMonthEvents(context.TODO(), &pb.ListEventsRequest{
		Date: timestamppb.Now(),
	})
	require.NoError(s.T(), err)
	require.Len(s.T(), res.GetEvents(), 0)
}

func (s *GRPCTestSuite) TestCreateErrors() {
	_, err := s.client.CreateEvent(context.TODO(), &pb.CreateEventRequest{
		Title: faker.StringWithSize(9),
	})
	require.EqualError(s.T(), err, "rpc error: code = InvalidArgument desc = invalid CreateEventRequest.Title: value length must be at least 10 runes")
}

func (s *GRPCTestSuite) TestCreate() {
	res, err := s.client.CreateEvent(context.TODO(), &pb.CreateEventRequest{
		Title: faker.StringWithSize(10),
	})
	require.NoError(s.T(), err)
	require.Len(s.T(), res.GetId(), 36)
}

func (s *GRPCTestSuite) TestUpdateErrors() {
	tests := []struct {
		req           *pb.UpdateEventRequest
		expectedError string
	}{
		{
			&pb.UpdateEventRequest{},
			"rpc error: code = InvalidArgument desc = invalid UpdateEventRequest.Id: value must be a valid UUID | caused by: invalid uuid format",
		},
		{
			&pb.UpdateEventRequest{
				Id: faker.UUID(),
			},
			"rpc error: code = InvalidArgument desc = invalid UpdateEventRequest.Title: value length must be at least 10 runes",
		},
		{
			&pb.UpdateEventRequest{
				Id:    faker.UUID(),
				Title: faker.StringWithSize(10),
			},
			"rpc error: code = InvalidArgument desc = invalid UpdateEventRequest.StartsAt: value is required",
		},
		{
			&pb.UpdateEventRequest{
				Id:       faker.UUID(),
				Title:    faker.StringWithSize(10),
				StartsAt: timestamppb.Now(),
			},
			"rpc error: code = InvalidArgument desc = invalid UpdateEventRequest.Duration: value is required",
		},
		{
			&pb.UpdateEventRequest{
				Id:       faker.UUID(),
				Title:    faker.StringWithSize(10),
				StartsAt: timestamppb.Now(),
				Duration: durationpb.New(time.Second),
			},
			"rpc error: code = Internal desc = event update error: event not found",
		},
	}

	for _, t := range tests {
		_, err := s.client.UpdateEvent(context.TODO(), t.req)
		require.EqualError(s.T(), err, t.expectedError)
	}
}

func (s *GRPCTestSuite) TestUpdate() {
	event, _ := s.client.CreateEvent(context.TODO(), &pb.CreateEventRequest{
		Title: faker.StringWithSize(10),
	})

	req := &pb.UpdateEventRequest{
		Id:       event.GetId(),
		Title:    faker.StringWithSize(10),
		StartsAt: timestamppb.Now(),
		Duration: durationpb.New(time.Second),
	}

	res, err := s.client.UpdateEvent(context.TODO(), req)

	require.NoError(s.T(), err)
	require.Equal(s.T(), res.GetEvent().GetId(), event.GetId())
	require.Equal(s.T(), res.GetEvent().GetTitle(), req.GetTitle())
	require.Equal(s.T(), res.GetEvent().GetStartsAt().String(), req.GetStartsAt().String())
	require.Equal(s.T(), res.GetEvent().GetDuration().String(), req.GetDuration().String())
}

func (s *GRPCTestSuite) TestDeleteErrors() {
	tests := []struct {
		req           *pb.DeleteEventRequest
		expectedError string
	}{
		{
			&pb.DeleteEventRequest{},
			"rpc error: code = InvalidArgument desc = invalid DeleteEventRequest.Id: value must be a valid UUID | caused by: invalid uuid format",
		},
		{
			&pb.DeleteEventRequest{Id: faker.UUID()},
			"rpc error: code = Internal desc = event delete error: event not found",
		},
	}

	for _, t := range tests {
		_, err := s.client.DeleteEvent(context.TODO(), t.req)
		require.EqualError(s.T(), err, t.expectedError)
	}
}

func (s *GRPCTestSuite) TestDelete() {
	event, _ := s.client.CreateEvent(context.TODO(), &pb.CreateEventRequest{
		Title: faker.StringWithSize(10),
	})

	_, err := s.client.DeleteEvent(context.TODO(), &pb.DeleteEventRequest{
		Id: event.GetId(),
	})

	require.NoError(s.T(), err)

	_, err = s.client.UpdateEvent(context.TODO(), &pb.UpdateEventRequest{
		Id:       event.GetId(),
		Title:    faker.StringWithSize(10),
		StartsAt: timestamppb.Now(),
		Duration: durationpb.New(time.Second),
	})

	require.EqualError(s.T(), err, "rpc error: code = Internal desc = event update error: event not found")
}

func (s *GRPCTestSuite) TestList() {
	date := time.Date(2021, 6, 20, 0, 0, 0, 0, time.Local)

	event1, _ := s.client.CreateEvent(context.TODO(), &pb.CreateEventRequest{
		Title: faker.StringWithSize(10),
	})
	event2, _ := s.client.CreateEvent(context.TODO(), &pb.CreateEventRequest{
		Title: faker.StringWithSize(10),
	})

	s.client.UpdateEvent(context.TODO(), &pb.UpdateEventRequest{
		Id:       event1.GetId(),
		Title:    faker.StringWithSize(10),
		StartsAt: timestamppb.New(date.Add(90 * time.Minute)),
		Duration: durationpb.New(time.Second),
	})

	s.client.UpdateEvent(context.TODO(), &pb.UpdateEventRequest{
		Id:       event2.GetId(),
		Title:    faker.StringWithSize(10),
		StartsAt: timestamppb.New(date.AddDate(0, 0, 2)),
		Duration: durationpb.New(time.Second),
	})

	events, err := s.client.ListMonthEvents(context.TODO(), &pb.ListEventsRequest{
		Date: timestamppb.New(date),
	})
	require.NoError(s.T(), err)
	require.Len(s.T(), events.GetEvents(), 2)
	require.Contains(s.T(), events.GetEvents()[0].GetId(), event1.GetId())
	require.Contains(s.T(), events.GetEvents()[1].GetId(), event2.GetId())

	events, err = s.client.ListWeekEvents(context.TODO(), &pb.ListEventsRequest{
		Date: timestamppb.New(date),
	})
	require.NoError(s.T(), err)
	require.Len(s.T(), events.GetEvents(), 1)
	require.Contains(s.T(), events.GetEvents()[0].GetId(), event1.GetId())

	events, err = s.client.ListDayEvents(context.TODO(), &pb.ListEventsRequest{
		Date: timestamppb.New(date),
	})
	require.NoError(s.T(), err)
	require.Len(s.T(), events.GetEvents(), 1)
	require.Contains(s.T(), events.GetEvents()[0].GetId(), event1.GetId())

	nextMonthDate := date.AddDate(0, 1, 0)

	events, err = s.client.ListMonthEvents(context.TODO(), &pb.ListEventsRequest{
		Date: timestamppb.New(nextMonthDate),
	})
	require.NoError(s.T(), err)
	require.Len(s.T(), events.GetEvents(), 0)

	events, err = s.client.ListWeekEvents(context.TODO(), &pb.ListEventsRequest{
		Date: timestamppb.New(nextMonthDate),
	})
	require.NoError(s.T(), err)
	require.Len(s.T(), events.GetEvents(), 0)

	events, err = s.client.ListDayEvents(context.TODO(), &pb.ListEventsRequest{
		Date: timestamppb.New(nextMonthDate),
	})
	require.NoError(s.T(), err)
	require.Len(s.T(), events.GetEvents(), 0)
}

func TestGRPC(t *testing.T) {
	suite.Run(t, new(GRPCTestSuite))
}
