package storage

type storage interface {
	get(string) ([]byte, error)
	set(string, []byte) error
	delete(string) error
}
