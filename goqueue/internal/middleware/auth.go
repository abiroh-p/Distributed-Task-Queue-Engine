package middleware

import (
    "context"
    "fmt"
    "strings"

    "github.com/golang-jwt/jwt/v5"
    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/metadata"
    "google.golang.org/grpc/status"
)

const (
    authHeader = "authorization"
    bearerPrefix = "Bearer "
)

type Claims struct {
    ClientID string `json:"client_id"`
    jwt.RegisteredClaims
}

func extractToken(ctx context.Context) (string, error) {
    md, ok := metadata.FromIncomingContext(ctx)
    if !ok {
        return "", fmt.Errorf("no metadata in context")
    }

    values := md[authHeader]
    if len(values) == 0 {
        return "", fmt.Errorf("authorization header missing")
    }

    token := values[0]
    if !strings.HasPrefix(token, bearerPrefix) {
        return "", fmt.Errorf("invalid token format, expected Bearer <token>")
    }

    return strings.TrimPrefix(token, bearerPrefix), nil
}

func validateToken(tokenStr, secret string) (*Claims, error) {
    claims := &Claims{}

    token, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (interface{}, error) {
        if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
            return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
        }
        return []byte(secret), nil
    })

    if err != nil {
        return nil, fmt.Errorf("invalid token: %w", err)
    }
    if !token.Valid {
        return nil, fmt.Errorf("token is not valid")
    }

    return claims, nil
}

func UnaryAuthInterceptor(secret string) grpc.UnaryServerInterceptor {
    return func(
        ctx context.Context,
        req interface{},
        info *grpc.UnaryServerInfo,
        handler grpc.UnaryHandler,
    ) (interface{}, error) {
        tokenStr, err := extractToken(ctx)
        if err != nil {
            return nil, status.Errorf(codes.Unauthenticated, "auth error: %v", err)
        }

        claims, err := validateToken(tokenStr, secret)
        if err != nil {
            return nil, status.Errorf(codes.Unauthenticated, "auth error: %v", err)
        }

        ctx = context.WithValue(ctx, "client_id", claims.ClientID)
        return handler(ctx, req)
    }
}