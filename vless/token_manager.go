package vless

import (
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func CreateAccessToken(secret []byte) (string, error) {
	now := time.Now().UTC()
	exp := now.Add(time.Minute)

	claims := jwt.MapClaims{
		"scopes": []string{"internal"},
		"iat":    now.Unix(),
		"exp":    exp.Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}
