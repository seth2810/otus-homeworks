package internalgrpc

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/server/grpc/pb"
	"github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ErrDateIsRequired = errors.New("date is required")

type service struct {
	app Application
	pb.UnimplementedCalendarServiceServer
}

func NewService(app Application) pb.CalendarServiceServer {
	return &service{app: app}
}

func (s *service) CreateEvent(ctx context.Context, req *pb.CreateEventRequest) (*pb.CreateEventResponse, error) {
	id := uuid.New()

	if err := s.app.CreateEvent(ctx, id.String(), req.GetTitle()); err != nil {
		return nil, status.Errorf(codes.Internal, "event create error: %s", err)
	}

	return &pb.CreateEventResponse{Id: id.String()}, nil
}

func (s *service) UpdateEvent(ctx context.Context, req *pb.UpdateEventRequest) (*pb.UpdateEventResponse, error) {
	event := storage.Event{
		ID:           req.GetId(),
		Title:        req.GetTitle(),
		StartsAt:     req.GetStartsAt().AsTime(),
		Duration:     req.GetDuration().AsDuration(),
		Description:  req.GetDescription(),
		NotifyBefore: req.GetNotifyBefore().AsDuration(),
	}

	if err := s.app.UpdateEvent(ctx, req.GetId(), event); err != nil {
		return nil, status.Errorf(codes.Internal, "event update error: %s", err)
	}

	return &pb.UpdateEventResponse{Event: req}, nil
}

func (s *service) DeleteEvent(ctx context.Context, req *pb.DeleteEventRequest) (*emptypb.Empty, error) {
	if err := s.app.DeleteEvent(ctx, req.GetId()); err != nil {
		return nil, status.Errorf(codes.Internal, "event delete error: %s", err)
	}

	return &emptypb.Empty{}, nil
}

func (s *service) ListDayEvents(ctx context.Context, req *pb.ListEventsRequest) (*pb.ListEventsResponse, error) {
	events, err := s.app.ListDayEvents(ctx, req.GetDate().AsTime())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list day events error: %s", err)
	}

	return &pb.ListEventsResponse{Events: formatResponseEvents(events)}, nil
}

func (s *service) ListWeekEvents(ctx context.Context, req *pb.ListEventsRequest) (*pb.ListEventsResponse, error) {
	events, err := s.app.ListWeekEvents(ctx, req.GetDate().AsTime())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list week events error: %s", err)
	}

	return &pb.ListEventsResponse{Events: formatResponseEvents(events)}, nil
}

func (s *service) ListMonthEvents(ctx context.Context, req *pb.ListEventsRequest) (*pb.ListEventsResponse, error) {
	events, err := s.app.ListMonthEvents(ctx, req.GetDate().AsTime())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list month events error: %s", err)
	}

	return &pb.ListEventsResponse{Events: formatResponseEvents(events)}, nil
}

func formatResponseEvent(event storage.Event) *pb.Event {
	return &pb.Event{
		Id:           event.ID,
		Title:        event.Title,
		StartsAt:     timestamppb.New(event.StartsAt),
		Duration:     durationpb.New(event.Duration),
		Description:  event.Description,
		OwnerId:      event.OwnerID,
		NotifyBefore: durationpb.New(event.NotifyBefore),
	}
}

func formatResponseEvents(events []storage.Event) []*pb.Event {
	res := make([]*pb.Event, 0, len(events))

	for _, e := range events {
		res = append(res, formatResponseEvent(e))
	}

	return res
}
