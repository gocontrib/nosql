package q

// M is map of field conditions.
type M map[string]interface{}

// Not condition.
type Not struct {
	Condition interface{}
}

// And is array of conditions to join as logical conjuction.
type And []interface{}

// Or is array of conditions to join as logical disjuction.
type Or []interface{}

// In operator.
type In []interface{}

// NotIn operator.
type NotIn []interface{}

// Op condition.
type Op struct {
	Kind  OpKind
	Value interface{}
}

// OpKind defines available operators.
type OpKind string

const (
	// OpLT defines "<" operator.
	OpLT OpKind = "lt"
	// OpLTE defines "<=" operator.
	OpLTE OpKind = "lte"
	// OpGT defines ">" operator.
	OpGT OpKind = "gt"
	// OpGTE defines ">=" operator.
	OpGTE OpKind = "gte"
	// OpNE defines "not equal" operator.
	OpNE OpKind = "ne"
)

// LT makes "<" condition.
func LT(value interface{}) interface{} {
	return Op{OpLT, value}
}

// LTE makes "<=" condition.
func LTE(value interface{}) interface{} {
	return Op{OpLTE, value}
}

// GT makes ">" condition.
func GT(value interface{}) interface{} {
	return Op{OpGT, value}
}

// GTE makes ">=" condition.
func GTE(value interface{}) interface{} {
	return Op{OpGTE, value}
}

// NotEqual makes "not equal" condition.
func NotEqual(value interface{}) interface{} {
	return Op{OpNE, value}
}
