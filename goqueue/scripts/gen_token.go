package main

import (
    "fmt"
    "time"
    "github.com/golang-jwt/jwt/v5"
)

func main() {
    claims := jwt.MapClaims{
        "client_id": "test-client",
        "exp":       time.Now().Add(24 * time.Hour).Unix(),
    }
    token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
    signed, err := token.SignedString([]byte("supersecret-dev-key"))
    if err != nil {
        panic(err)
    }
    fmt.Println(signed)
}