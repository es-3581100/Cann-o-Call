package authority

import (
	"crypto/subtle"
	"errors"
)

type Service struct {
	token string
}

func New(token string) *Service {
	return &Service{
		token: token,
	}
}

func (s *Service) HasToken() bool {
	return s.token != ""
}

func (s *Service) Check(provided string) error {
	if s.token == "" {
		return errors.New("authority token is not configured")
	}

	if subtle.ConstantTimeCompare([]byte(s.token), []byte(provided)) == 1 {
		return nil
	}

	return errors.New("invalid authority token")
}
