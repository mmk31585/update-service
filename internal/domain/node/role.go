package node

type Role string

const (
	RoleEntry  Role = "entry"
	RoleWorker Role = "worker"
	RoleHybrid Role = "hybrid"
)

func ParseRole(s string) Role {
	switch s {
	case "entry":
		return RoleEntry
	case "worker":
		return RoleWorker
	case "hybrid":
		return RoleHybrid
	default:
		return RoleEntry
	}
}

func (r Role) String() string {
	return string(r)
}

func (r Role) IsEntry() bool {
	return r == RoleEntry || r == RoleHybrid
}

func (r Role) IsWorker() bool {
	return r == RoleWorker || r == RoleHybrid
}
