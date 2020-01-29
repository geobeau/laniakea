package btree

import "errors"

// Btree is a B+Tree implementation
type Btree struct {
	blockSize uint32
	root      node
}

func newDefaultBtree() Btree {
	return Btree{blockSize: 4000, root: node{}}
}

func (b *Btree) Set(key string, value string) {
	b.root.Set(key, value)
}

func (b *Btree) Get(key string) (string, error) {
	return b.root.Get(key)
}

type node struct {
	refs []keyRef
}

func (n *node) Set(key string, value string) {
	n.refs = append(n.refs, keyRef{key: key, ref: nil})
}

func (n *node) Get(key string) (string, error) {
	for i := range n.refs {
		if n.refs[i].key == key {
			return "fake", nil
		}
	}
	return "", errors.New("notfound")
}

type keyRef struct {
	key string
	ref *node
}
