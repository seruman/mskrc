package internal

type Marshaler interface {
	Marshal() ([]byte, error)
}

type Cluster struct {
	Name    string
	Alias   string
	Brokers []string
	Version string
}
