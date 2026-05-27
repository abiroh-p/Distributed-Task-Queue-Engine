package config

import (
	"os"    
	"strconv"
)

type Config struct {
	PostgresDSN   string
	RedisAddr     string
	GRPCPort      string
	HTTPPort      string
	MetricsPort   string
	WorkerCount   int
	JWTSecret string

}

func Load() Config {
	return Config{
		PostgresDSN: getEnv("POSTGRES_DSN", "postgres://goqueue:goqueue@localhost:5432/goqueue?sslmode=disable"),
		RedisAddr:   getEnv("REDIS_ADDR", "localhost:6379"),
		GRPCPort:    getEnv("GRPC_PORT", "50051"),
		HTTPPort:    getEnv("HTTP_PORT", "8080"),
		MetricsPort: getEnv("METRICS_PORT", "9091"),
		WorkerCount: 10,
		JWTSecret: getEnv("JWT_SECRET", "supersecret-dev-key"),

	}
}

func getEnv(key, fallback string) string {
    if v := os.Getenv(key); v != "" {
        return v
    }
    return fallback
}

func getEnvInt(key string, fallback int) int {
    if v := os.Getenv(key); v != "" {
        if i, err := strconv.Atoi(v); err == nil {
            return i
        }
    }
    return fallback
}
