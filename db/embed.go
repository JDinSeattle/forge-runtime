// Package db owns migration assets; application SQL lives in internal/persistence.
package db

import "embed"

// Migrations contains goose-compatible immutable migration files.
//
//go:embed migrations/*.sql
var Migrations embed.FS
