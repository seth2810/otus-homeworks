package internalgrpc

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/app"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func loggingInterceptor(logger app.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		var ip string

		start := time.Now()
		resp, err = handler(ctx, req)

		if p, ok := peer.FromContext(ctx); ok {
			ip, _, _ = net.SplitHostPort(p.Addr.String())
		}

		logger.Info(fmt.Sprintf("%s [%s] %s %s %s %s %s",
			ip, time.Now().Format("02/Jan/2006:15:04:05 -0700"),
			info.FullMethod, status.Code(err), resp, err, time.Since(start),
		))

		return
	}
}
