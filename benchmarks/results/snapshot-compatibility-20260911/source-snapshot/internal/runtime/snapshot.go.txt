package runtime

// ValidateSnapshot is the same pure validation used before every transition.
// Operator compatibility checks cannot invent commands or migrate data.
func ValidateSnapshot(state State) error { return validateState(state) }
