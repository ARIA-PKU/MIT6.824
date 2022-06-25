package mr
// restore some common values and usages

type TaskStatus uint8

const (
	Idle TaskStatus = iota
	Working
	Finshed
)

type OperationPhase uint8

const (
	MapPhase OperationPhase = iota
	ReducePhase
	CompletedPhase
)

type WorkType uint8

const (
	Map WorkType = iota
	Reduce
	Completed
)