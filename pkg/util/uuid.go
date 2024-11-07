package util

import (
	"encoding/hex"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

func StringToPgtypeUUID(uuidStr string) (pgtype.UUID, error) {
	// Parse the string into a uuid.UUID
	parsedUUID, err := uuid.Parse(uuidStr)
	if err != nil {
		return pgtype.UUID{}, err
	}

	// Create a pgtype.UUID and set the Bytes and Status
	var pgUUID pgtype.UUID
	pgUUID.Bytes = parsedUUID
	pgUUID.Valid = true

	return pgUUID, nil
}

func PgtypeUUIDToString(uuid pgtype.UUID) (string, error) {
	if !uuid.Valid {
		return "", errors.New("invalid UUID")
	}

	var buf [36]byte
	hex.Encode(buf[:8], uuid.Bytes[:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], uuid.Bytes[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], uuid.Bytes[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], uuid.Bytes[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:], uuid.Bytes[10:])

	return string(buf[:]), nil
}

func MustPgtypeUUIDToString(uuid pgtype.UUID) string {
	s, err := PgtypeUUIDToString(uuid)
	if err != nil {
		panic(err)
	}
	return s
}
