package util

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestPgtypeUUIDToString(t *testing.T) {
	tests := []struct {
		name     string
		uuid     pgtype.UUID
		expected string
		wantErr  bool
	}{
		{
			name: "Valid UUID",
			uuid: pgtype.UUID{
				Bytes: [16]byte{0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef},
				Valid: true,
			},
			expected: "12345678-90ab-cdef-1234-567890abcdef",
			wantErr:  false,
		},
		{
			name: "Invalid UUID",
			uuid: pgtype.UUID{
				Valid: false,
			},
			expected: "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PgtypeUUIDToString(tt.uuid)
			if (err != nil) != tt.wantErr {
				t.Errorf("PgtypeUUIDToString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("PgtypeUUIDToString() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestMustPgtypeUUIDToString(t *testing.T) {
	tests := []struct {
		name      string
		uuid      pgtype.UUID
		expected  string
		wantPanic bool
	}{
		{
			name: "Valid UUID",
			uuid: pgtype.UUID{
				Bytes: [16]byte{0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef},
				Valid: true,
			},
			expected:  "12345678-90ab-cdef-1234-567890abcdef",
			wantPanic: false,
		},
		{
			name: "Invalid UUID",
			uuid: pgtype.UUID{
				Valid: false,
			},
			expected:  "",
			wantPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if (r != nil) != tt.wantPanic {
					t.Errorf("MustPgtypeUUIDToString() panic = %v, wantPanic %v", r, tt.wantPanic)
				}
			}()

			got := MustPgtypeUUIDToString(tt.uuid)
			if got != tt.expected {
				t.Errorf("MustPgtypeUUIDToString() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestStringToPgtypeUUID(t *testing.T) {
	tests := []struct {
		name    string
		uuidStr string
		want    pgtype.UUID
		wantErr bool
	}{
		{
			name:    "Valid UUID",
			uuidStr: "12345678-90ab-cdef-1234-567890abcdef",
			want: pgtype.UUID{
				Bytes: [16]byte{0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef},
				Valid: true,
			},
			wantErr: false,
		},
		{
			name:    "Invalid UUID format",
			uuidStr: "not-a-valid-uuid",
			want:    pgtype.UUID{},
			wantErr: true,
		},
		{
			name:    "Empty string",
			uuidStr: "",
			want:    pgtype.UUID{},
			wantErr: true,
		},
		{
			name:    "UUID with uppercase letters",
			uuidStr: "12345678-90AB-CDEF-1234-567890ABCDEF",
			want: pgtype.UUID{
				Bytes: [16]byte{0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef},
				Valid: true,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := StringToPgtypeUUID(tt.uuidStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("StringToPgtypeUUID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("StringToPgtypeUUID() = %v, want %v", got, tt.want)
			}
		})
	}
}
