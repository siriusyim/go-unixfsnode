package builder

import (
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path"
	"sync"

	"github.com/ipfs/go-cid"
	chunk "github.com/ipfs/go-ipfs-chunker"
	"github.com/ipfs/go-unixfsnode/data"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/multiformats/go-multihash"
)

type FileSliceKey string
type FileSlice struct {
	Path   string
	Offset int
	size   int

	Cid cid.Cid
}

func (fs *FileSlice) Key() FileSliceKey {
	return FileSliceKey(fmt.Sprintf("%s-%d", fs.Path, fs.Offset))
}

type MerkleCar struct {
	size int
	max  int
	fi   []*FileSlice
	done bool
	ls   *linking.LinkSystem
}

func NewCar(max int, ls *linking.LinkSystem) *MerkleCar {
	return &MerkleCar{
		size: 0,
		max:  max,
		fi:   make([]*FileSlice, 0),
		ls:   ls,
	}
}

func (m *MerkleCar) isOverflow(size int) bool {
	if size+m.size > m.max {
		return false
	}
	return true
}

func (m *MerkleCar) addFileSlice(fs *FileSlice) {
	m.fi = append(m.fi, fs)
	m.size = m.size + fs.size
}

func (m *MerkleCar) complete() {
	m.done = true
}

func (m *MerkleCar) isComplete() bool {
	return m.done
}

type NextBytes struct {
	data []byte
	size int
}

type UnixFsMerkle struct {
	fi map[FileSliceKey]struct{}
	lk sync.Mutex

	olk sync.Mutex

	curFileSlice *FileSlice

	cars []*MerkleCar

	curCar *MerkleCar

	curData     *NextBytes
	nextOffsets map[string]int

	maxSize   int
	blockSize int
}

func NewUnixFsMerkle(max int, ls *linking.LinkSystem) *UnixFsMerkle {
	return &UnixFsMerkle{
		fi:           make(map[FileSliceKey]struct{}),
		cars:         make([]*MerkleCar, 0),
		maxSize:      max,
		curFileSlice: nil,
		blockSize:    1024 * 1024 * 2,
		nextOffsets:  make(map[string]int, 0),
		curCar:       NewCar(max, ls),
	}
}

func (u *UnixFsMerkle) ChangeCar(ls *linking.LinkSystem) {
	u.curCar = NewCar(u.maxSize, ls)
}

func (u *UnixFsMerkle) AddFileSlice(fs *FileSlice) error {
	u.lk.Lock()
	defer u.lk.Unlock()
	u.fi[fs.Key()] = struct{}{}
	u.curCar.addFileSlice(fs)
	return nil
}

func (u *UnixFsMerkle) LastFileSlice(path string, lastSlice string) (*FileSlice, error) {
	return nil, nil
}
func (u *UnixFsMerkle) NextFileSlice(path string, lastSlice string) (*FileSlice, error) {
	return nil, nil
}

func (u *UnixFsMerkle) BuildUnixFSRecursive(root string, ls *ipld.LinkSystem) (ipld.Link, uint64, error) {
	info, err := os.Lstat(root)
	if err != nil {
		return nil, 0, err
	}

	m := info.Mode()
	switch {
	case m.IsDir():
		var tsize uint64
		entries, err := os.ReadDir(root)
		if err != nil {
			return nil, 0, err
		}
		lnks := make([]dagpb.PBLink, 0, len(entries))

		for _, e := range entries {
			lnk, sz, err := u.BuildUnixFSRecursive(path.Join(root, e.Name()), ls)
			if err != nil {
				return nil, 0, err
			}

			tsize += sz
			entry, err := BuildUnixFSDirectoryEntry(e.Name(), int64(sz), lnk)
			if err != nil {
				return nil, 0, err
			}

			lnks = append(lnks, entry)
		}
		return u.BuildUnixFSDirectoryMerkle(lnks, ls, 0, root)

	case m.Type() == fs.ModeSymlink:
		content, err := os.Readlink(root)
		if err != nil {
			return nil, 0, err
		}
		outLnk, sz, err := BuildUnixFSSymlink(content, ls)
		if err != nil {
			return nil, 0, err
		}
		return outLnk, sz, nil
	case m.IsRegular():
		fp, err := os.Open(root)
		if err != nil {
			return nil, 0, err
		}
		defer fp.Close()
		outLnk, sz, err := u.BuildUnixFSFile(root, fp, fmt.Sprintf("size-%d", u.blockSize), ls)
		if err != nil {
			return nil, 0, err
		}
		return outLnk, sz, nil
	default:
		return nil, 0, fmt.Errorf("cannot encode non regular file: %s", root)
	}
}

func (u *UnixFsMerkle) BuildUnixFSFile(path string, r io.Reader, chunker string, ls *ipld.LinkSystem) (ipld.Link, uint64, error) {
	s, err := chunk.FromString(r, chunker)
	if err != nil {
		return nil, 0, err
	}

	var prev []ipld.Link
	var prevLen []uint64
	depth := 1
	for {
		root, size, err := u.fileTreeRecursive(path, depth, prev, prevLen, s, ls)
		if err != nil {
			return nil, 0, err
		}

		if prev != nil && prev[0] == root {
			if root == nil {
				node := basicnode.NewBytes([]byte{})
				link, err := ls.Store(ipld.LinkContext{}, leafLinkProto, node)
				return link, 0, err
			}
			return root, size, nil
		}

		prev = []ipld.Link{root}
		prevLen = []uint64{size}
		depth++
	}
}

func (u *UnixFsMerkle) fileTreeRecursive(path string, depth int, children []ipld.Link, childLen []uint64, src chunk.Splitter, ls *ipld.LinkSystem) (link ipld.Link, totalSize uint64, err error) {
	if depth == 1 && len(children) > 0 {
		return nil, 0, fmt.Errorf("leaf nodes cannot have children")
	} else if depth == 1 {
		var leaf []byte
		offset := 0
		if u.curCar.isComplete() {
			return nil, 0, nil
		}

		if u.curData != nil {
			leaf = u.curData.data
			if path != u.curFileSlice.Path {
				return nil, 0, nil
			}
			offset = u.curFileSlice.Offset
		} else {
			leaf, err = src.NextBytes()
			if err == io.EOF {
				return nil, 0, nil
			} else if err != nil {
				return nil, 0, err
			}

			leafSize := len(leaf)
			if v, ok := u.nextOffsets[path]; ok {
				offset = v
				v = v + leafSize
			} else {
				offset = 0
				u.nextOffsets[path] = 0
			}

		}

		leafSize := len(leaf)
		u.curData = &NextBytes{
			data: leaf,
			size: leafSize,
		}

		u.curFileSlice = &FileSlice{
			Path:   path,
			Offset: offset,
			size:   leafSize,
		}

		if u.curCar.isOverflow(u.curData.size) {
			u.curCar.complete()
			return nil, 0, nil
		}

		node := basicnode.NewBytes(leaf)

		u.curData = nil

		return u.sizedStore(ls, leafLinkProto, node)
	}
	// depth > 1.
	totalSize = uint64(0)
	blksizes := make([]uint64, 0, DefaultLinksPerBlock)
	if children == nil {
		children = make([]ipld.Link, 0)
	} else {
		for i := range children {
			blksizes = append(blksizes, childLen[i])
			totalSize += childLen[i]
		}
	}
	for len(children) < DefaultLinksPerBlock {
		nxt, sz, err := u.fileTreeRecursive(path, depth-1, nil, nil, src, ls)
		if err != nil {
			return nil, 0, err
		} else if nxt == nil {
			// eof
			break
		}
		totalSize += sz
		children = append(children, nxt)
		childLen = append(childLen, sz)
		blksizes = append(blksizes, sz)
	}
	if len(children) == 0 {
		// empty case.
		return nil, 0, nil
	} else if len(children) == 1 {
		// degenerate case
		return children[0], childLen[0], nil
	}

	// make the unixfs node.
	node, err := BuildUnixFS(func(b *Builder) {
		FileSize(b, totalSize)
		BlockSizes(b, blksizes)
	})
	if err != nil {
		return nil, 0, err
	}

	// Pack into the dagpb node.
	dpbb := dagpb.Type.PBNode.NewBuilder()
	pbm, err := dpbb.BeginMap(2)
	if err != nil {
		return nil, 0, err
	}
	pblb, err := pbm.AssembleEntry("Links")
	if err != nil {
		return nil, 0, err
	}
	pbl, err := pblb.BeginList(int64(len(children)))
	if err != nil {
		return nil, 0, err
	}
	for i, c := range children {
		pbln, err := BuildUnixFSDirectoryEntry("", int64(blksizes[i]), c)
		if err != nil {
			return nil, 0, err
		}
		if err = pbl.AssembleValue().AssignNode(pbln); err != nil {
			return nil, 0, err
		}
	}
	if err = pbl.Finish(); err != nil {
		return nil, 0, err
	}
	if err = pbm.AssembleKey().AssignString("Data"); err != nil {
		return nil, 0, err
	}
	if err = pbm.AssembleValue().AssignBytes(data.EncodeUnixFSData(node)); err != nil {
		return nil, 0, err
	}
	if err = pbm.Finish(); err != nil {
		return nil, 0, err
	}
	pbn := dpbb.Build()

	link, _, err = u.sizedStore(ls, fileLinkProto, pbn)
	if err != nil {
		return nil, 0, err
	}
	return link, totalSize, nil
}

func (u *UnixFsMerkle) BuildUnixFSDirectoryMerkle(entries []dagpb.PBLink, ls *ipld.LinkSystem, depth int, name string) (ipld.Link, uint64, error) {
	lg2 := uint64(math.Log2(float64(len(entries))))
	splnum := int(math.Pow(2, float64(lg2)))

	if len(entries) > 2 {
		var lentries []dagpb.PBLink
		var rentries []dagpb.PBLink
		if len(entries) > splnum {
			lentries = entries[:splnum]
			rentries = entries[splnum:]
		} else {
			lentries = entries[:splnum/2]
			rentries = entries[splnum/2:]
		}
		llnk, lsize, err := u.BuildUnixFSDirectoryMerkle(lentries, ls, depth+1, name)
		if err != nil {
			return nil, 0, err
		}

		lentry, err := BuildUnixFSDirectoryEntry(fmt.Sprintf("%s-vlayer-%d", name, depth+1), int64(lsize), llnk)
		if err != nil {
			return nil, 0, err
		}

		rlnk, rsize, err := u.BuildUnixFSDirectoryMerkle(rentries, ls, depth+1, name)
		if err != nil {
			return nil, 0, err
		}

		rentry, err := BuildUnixFSDirectoryEntry(fmt.Sprintf("%s-vlayer-%d", name, depth+1), int64(rsize), rlnk)
		if err != nil {
			return nil, 0, err
		}

		lnks := make([]dagpb.PBLink, 0, 2)
		lnks = append(lnks, lentry)
		lnks = append(lnks, rentry)
		return u.BuildUnixFSDirectoryMerkle(lnks, ls, depth, name)
	}

	if estimateDirSize(entries) > shardSplitThreshold {
		return BuildUnixFSShardedDirectory(defaultShardWidth, multihash.MURMUR3X64_64, entries, ls)
	}

	ufd, err := BuildUnixFS(func(b *Builder) {
		DataType(b, data.Data_Directory)
	})
	if err != nil {
		return nil, 0, err
	}
	pbb := dagpb.Type.PBNode.NewBuilder()
	pbm, err := pbb.BeginMap(2)
	if err != nil {
		return nil, 0, err
	}
	if err = pbm.AssembleKey().AssignString("Data"); err != nil {
		return nil, 0, err
	}
	if err = pbm.AssembleValue().AssignBytes(data.EncodeUnixFSData(ufd)); err != nil {
		return nil, 0, err
	}
	if err = pbm.AssembleKey().AssignString("Links"); err != nil {
		return nil, 0, err
	}

	lnks, err := pbm.AssembleValue().BeginList(int64(len(entries)))
	if err != nil {
		return nil, 0, err
	}
	// sorting happens in codec-dagpb
	var totalSize uint64
	for _, e := range entries {
		totalSize += uint64(e.Tsize.Must().Int())
		if err := lnks.AssembleValue().AssignNode(e); err != nil {
			return nil, 0, err
		}
	}
	if err := lnks.Finish(); err != nil {
		return nil, 0, err
	}
	if err := pbm.Finish(); err != nil {
		return nil, 0, err
	}
	node := pbb.Build()
	lnk, sz, err := u.sizedStore(ls, fileLinkProto, node)
	if err != nil {
		return nil, 0, err
	}
	return lnk, totalSize + sz, err
}

func (u *UnixFsMerkle) sizedStore(ls *ipld.LinkSystem, lp datamodel.LinkPrototype, n datamodel.Node) (datamodel.Link, uint64, error) {
	var byteCount int
	lnk, err := wrappedLinkSystem(ls, func(bc int) {
		byteCount = bc
	}).Store(ipld.LinkContext{}, lp, n)
	if err == nil {
		rcl, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, 0, fmt.Errorf("could not interpret %s", lnk)
		}
		u.curFileSlice.Cid = rcl.Cid
		u.AddFileSlice(u.curFileSlice)
	}

	return lnk, uint64(byteCount), err
}
