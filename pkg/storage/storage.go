package storage

type storage interface {
	Get(string) ([]byte, error)
	Set(string, []byte) error
	Delete(string) error
}
