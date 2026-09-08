// Separate module boundary: never traverse persistent Docker/workspace data
// while running the source repository's go test ./... or go list ./....
module forge.local/runtime-state

go 1.26.0
