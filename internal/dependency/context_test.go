package dependency

import (
	"context"
	"testing"
	"time"
)

func TestDependencyBudgetsDoNotCancelParent(t *testing.T) {
	parent := context.Background()
	db, cancelDB := Database(parent)
	objects, cancelObjects := Artifact(parent)
	defer cancelObjects()
	databaseEnd, _ := db.Deadline()
	artifactEnd, _ := objects.Deadline()
	if time.Until(databaseEnd) > DatabaseTimeout || time.Until(databaseEnd) < DatabaseTimeout-time.Second || !artifactEnd.After(databaseEnd) {
		t.Fatal("separate budgets absent")
	}
	cancelDB()
	if parent.Err() != nil || objects.Err() != nil {
		t.Fatal("short DB operation canceled parent/artifact")
	}
	short, cancel := context.WithTimeout(parent, 20*time.Millisecond)
	defer cancel()
	child, end := Database(short)
	defer end()
	deadline, _ := short.Deadline()
	inherited, _ := child.Deadline()
	if !deadline.Equal(inherited) {
		t.Fatal("caller deadline extended")
	}
}
