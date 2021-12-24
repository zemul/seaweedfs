package security

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const PrivateSecret = "OP7XUA5SFHREMB9N0L814ZTJ6QIYDVCW"

type WithGrpcFilerTokenAuth struct {
}

func (c WithGrpcFilerTokenAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	// macid + secret key + sale > text
	// macid + text >= filer
	// 携带 凭证 + macid >=  filer， 揭秘， macid是否相同
	return map[string]string{
		"secret": PrivateSecret,
	}, nil
}

func (c WithGrpcFilerTokenAuth) RequireTransportSecurity() bool {
	return false
}

func FilerAuthInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	err := auth(ctx)
	if err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func auth(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Errorf(codes.Unauthenticated, "无Token认证信息")
	}
	var (
		secret string
	)
	if val, ok := md["secret"]; ok {
		secret = val[0]
	}
	if secret != PrivateSecret {
		return status.Errorf(codes.Unauthenticated, "secret invalide")
	}
	return nil
}
