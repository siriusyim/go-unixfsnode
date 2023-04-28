package builder

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sync"

	"github.com/ipfs/go-cid"
	chunk "github.com/ipfs/go-ipfs-chunker"
	"github.com/ipfs/go-unixfsnode/data"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

type FileSlice struct {
	Path   string
	Offset uint64
	size   uint64

	Cid cid.Cid
}

type UnixFsMerkle struct {
	fi      []*FileSlice
	lk      sync.Mutex
	maxSize uint64
	size    uint64

	offsets map[string]uint64
	olk     sync.Mutex
}

func NewUnixFsMerkle(max uint64) *UnixFsMerkle {
	return &UnixFsMerkle{
		fi:      make([]*FileSlice, 0),
		maxSize: max,
		size:    0,
		offsets: make(map[string]uint64),
	}
}

func (u *UnixFsMerkle) AddFileSlice(fs *FileSlice) error {
	u.lk.Lock()
	defer u.lk.Unlock()
	u.fi = append(u.fi, fs)
	return nil
}

func (u *UnixFsMerkle) tryAddFileSlice(fs *FileSlice, link ipld.Link) bool {
	if fs.size+u.size > u.maxSize {
		return false
	}

	offset, err := u.sliceOffset(fs.Path)
	if err != nil {
		return false
	}

	fs.Offset = offset
	rcl, ok := link.(cidlink.Link)
	if !ok {
		return false
	}
	fs.Cid = rcl.Cid

	u.AddFileSlice(fs)

	u.size = u.size + fs.size

	return true
}

func (u *UnixFsMerkle) LastFileSlice(path string, lastSlice string) (*FileSlice, error) {
	return nil, nil
}
func (u *UnixFsMerkle) NextFileSlice(path string, lastSlice string) (*FileSlice, error) {
	return nil, nil
}

func (u *UnixFsMerkle) sliceOffset(path string) (uint64, error) {
	u.olk.Lock()
	defer u.olk.Unlock()
	if v, ok := u.offsets[path]; ok {
		return v, nil
	}
	return 0, xerrors.Errorf("cant find offset of path:%s ", path)
}

func (u *UnixFsMerkle) BuildUnixFSRecursive(root string, ls *ipld.LinkSystem) (bool, ipld.Link, uint64, error) {
	var _abort_o bool
	info, err := os.Lstat(root)
	if err != nil {
		return _abort_o, nil, 0, err
	}

	m := info.Mode()
	switch {
	case m.IsDir():
		var tsize uint64
		entries, err := os.ReadDir(root)
		if err != nil {
			return _abort_o, nil, 0, err
		}
		lnks := make([]dagpb.PBLink, 0, len(entries))

		for _, e := range entries {
			_abort, lnk, sz, err := u.BuildUnixFSRecursive(path.Join(root, e.Name()), ls)
			if err != nil {
				return _abort_o, nil, 0, err
			}
			_abort_o = _abort
			if _abort {
				break
			}

			tsize += sz
			entry, err := BuildUnixFSDirectoryEntry(e.Name(), int64(sz), lnk)
			if err != nil {
				return _abort_o, nil, 0, err
			}

			lnks = append(lnks, entry)
		}
		link, tsize, err := u.BuildUnixFSDirectory(lnks, ls)
		return _abort_o, link, tsize, err

	case m.Type() == fs.ModeSymlink:
		content, err := os.Readlink(root)
		if err != nil {
			return _abort_o, nil, 0, err
		}
		outLnk, sz, err := BuildUnixFSSymlink(content, ls)
		if err != nil {
			return _abort_o, nil, 0, err
		}
		return _abort_o, outLnk, sz, nil
	case m.IsRegular():
		fp, err := os.Open(root)
		if err != nil {
			return _abort_o, nil, 0, err
		}
		defer fp.Close()
		outLnk, sz, err := u.BuildUnixFSFile(root, fp, "", ls)
		if err != nil {
			return _abort_o, nil, 0, err
		}
		return _abort_o, outLnk, sz, nil
	default:
		return _abort_o, nil, 0, fmt.Errorf("cannot encode non regular file: %s", root)
	}
}

//目录内的文件个数超过2个构建虚拟文件夹构成二叉结构
func (u *UnixFsMerkle) BuildUnixFSDirectory(entries []dagpb.PBLink, ls *ipld.LinkSystem) (ipld.Link, uint64, error) {
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
	lnk, sz, err := sizedStore(ls, fileLinkProto, node)
	if err != nil {
		return nil, 0, err
	}
	return lnk, totalSize + sz, err
}

func (u *UnixFsMerkle) BuildUnixFSFile(root string, r io.Reader, chunker string, ls *ipld.LinkSystem) (ipld.Link, uint64, error) {
	s, err := chunk.FromString(r, chunker)
	if err != nil {
		return nil, 0, err
	}

	var prev []ipld.Link
	var prevLen []uint64
	depth := 1
	for {
		root, size, err := u.fileTreeRecursive(root, depth, prev, prevLen, s, ls)
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

func (u *UnixFsMerkle) fileTreeRecursive(root string, depth int, children []ipld.Link, childLen []uint64, src chunk.Splitter, ls *ipld.LinkSystem) (link ipld.Link, totalSize uint64, err error) {
	if depth == 1 && len(children) > 0 {
		return nil, 0, fmt.Errorf("leaf nodes cannot have children")
	} else if depth == 1 {
		leaf, err := src.NextBytes()
		if err == io.EOF {
			return nil, 0, nil
		} else if err != nil {
			return nil, 0, err
		}
		node := basicnode.NewBytes(leaf)
		link, totalSize, err = sizedStore(ls, leafLinkProto, node)
		if err != nil {
			return nil, 0, err
		}
		if !u.tryAddFileSlice(&FileSlice{
			Path: root,
			size: totalSize,
		}, link) {

		}

		return link, totalSize, err
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
		nxt, sz, err := u.fileTreeRecursive(root, depth-1, nil, nil, src, ls)
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

	link, _, err = sizedStore(ls, fileLinkProto, pbn)
	if err != nil {
		return nil, 0, err
	}
	return link, totalSize, nil
}
