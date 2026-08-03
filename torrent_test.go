package torrent

import (
	"context"
	"expvar"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	g "github.com/anacrolix/generics"
	"github.com/anacrolix/missinggo/v2"
	"github.com/go-quicktest/qt"

	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/internal/testutil"
	"github.com/anacrolix/torrent/metainfo"
	pp "github.com/anacrolix/torrent/peer_protocol"
	"github.com/anacrolix/torrent/storage"
)

func r(i, b, l pp.Integer) Request {
	return Request{i, ChunkSpec{b, l}}
}

// Check the given request is correct for various torrent offsets.
func TestTorrentRequest(t *testing.T) {
	const s = 472183431 // Length of torrent.
	for _, _case := range []struct {
		off int64   // An offset into the torrent.
		req Request // The expected request. The zero value means !ok.
	}{
		// Invalid offset.
		{-1, Request{}},
		{0, r(0, 0, 16384)},
		// One before the end of a piece.
		{1<<18 - 1, r(0, 1<<18-16384, 16384)},
		// Offset beyond torrent length.
		{472 * 1 << 20, Request{}},
		// One before the end of the torrent. Complicates the chunk length.
		{s - 1, r((s-1)/(1<<18), (s-1)%(1<<18)/(16384)*(16384), 12935)},
		{1, r(0, 0, 16384)},
		// One before end of chunk.
		{16383, r(0, 0, 16384)},
		// Second chunk.
		{16384, r(0, 16384, 16384)},
	} {
		req, ok := torrentOffsetRequest(472183431, 1<<18, 16384, _case.off)
		if (_case.req == Request{}) == ok {
			t.Fatalf("expected %v, got %v", _case.req, req)
		}
		if req != _case.req {
			t.Fatalf("expected %v, got %v", _case.req, req)
		}
	}
}

func TestAppendToCopySlice(t *testing.T) {
	orig := []int{1, 2, 3}
	dupe := append([]int{}, orig...)
	dupe[0] = 4
	if orig[0] != 1 {
		t.FailNow()
	}
}

func TestTorrentString(t *testing.T) {
	tor := &Torrent{}
	tor.infoHash.Ok = true
	tor.infoHash.Value[0] = 1
	s := tor.InfoHash().HexString()
	if s != "0100000000000000000000000000000000000000" {
		t.FailNow()
	}
}

// This benchmark is from the observation that a lot of overlapping Readers on
// a large torrent with small pieces had a lot of overhead in recalculating
// piece priorities everytime a reader (possibly in another Torrent) changed.
func BenchmarkUpdatePiecePriorities(b *testing.B) {
	const (
		numPieces   = 13410
		pieceLength = 256 << 10
	)
	cl := newTestingClient(b)
	t := cl.newTorrentForTesting()
	qt.Assert(b, qt.IsNil(t.setInfoUnlocked(&metainfo.Info{
		Pieces:      make([]byte, metainfo.HashSize*numPieces),
		PieceLength: pieceLength,
		Length:      pieceLength * numPieces,
	})))
	qt.Check(b, qt.Equals(t.numPieces(), 13410))
	for i := 0; i < 7; i += 1 {
		r := t.NewReader()
		r.SetReadahead(32 << 20)
		r.Seek(3500000, io.SeekStart)
	}
	qt.Check(b, qt.HasLen(t.readers, 7))
	for i := 0; i < t.numPieces(); i += 3 {
		t._completedPieces.Add(i)
	}
	t.DownloadPieces(0, t.numPieces())
	for b.Loop() {
		cl.lock()
		t.updateAllPiecePriorities("")
		cl.unlock()
	}
}

// Check that a torrent containing zero-length file(s) will start, and that
// they're created in the filesystem. The client storage is assumed to be
// file-based on the native filesystem based.
func testEmptyFilesAndZeroPieceLength(t *testing.T, cfg *ClientConfig) {
	cl, err := NewClient(cfg)
	qt.Assert(t, qt.IsNil(err))
	defer cl.Close()
	ib, err := bencode.Marshal(metainfo.Info{
		Name:        "empty",
		Length:      0,
		PieceLength: 0,
	})
	qt.Assert(t, qt.IsNil(err))
	fp := filepath.Join(cfg.DataDir, "empty")
	os.Remove(fp)
	qt.Check(t, qt.IsFalse(missinggo.FilePathExists(fp)))
	tt, err := cl.AddTorrent(&metainfo.MetaInfo{
		InfoBytes: ib,
	})
	qt.Assert(t, qt.IsNil(err))
	defer tt.Drop()
	tt.DownloadAll()
	qt.Assert(t, qt.IsTrue(cl.WaitAll()))
	qt.Check(t, qt.IsTrue(tt.Complete().Bool()))
	qt.Check(t, qt.IsTrue(missinggo.FilePathExists(fp)))
}

func TestEmptyFilesAndZeroPieceLengthWithFileStorage(t *testing.T) {
	cfg := TestingConfig(t)
	ci := storage.NewFile(cfg.DataDir)
	defer ci.Close()
	cfg.DefaultStorage = ci
	testEmptyFilesAndZeroPieceLength(t, cfg)
}

func TestPieceHashFailed(t *testing.T) {
	mi := testutil.GreetingMetaInfo()
	cl := newTestingClient(t)
	tt := cl.newTorrent(mi.HashInfoBytes(), badStorage{})
	tt.setChunkSize(2)
	tt.cl.lock()
	qt.Assert(t, qt.IsNil(tt.setInfoBytesLocked(mi.InfoBytes)))
	tt.cl.unlock()
	tt.cl.lock()
	tt.dirtyChunks.AddRange(
		uint64(tt.pieceRequestIndexBegin(1)),
		uint64(tt.pieceRequestIndexBegin(1)+3))
	qt.Assert(t, qt.IsTrue(tt.pieceAllDirty(1)))
	tt.pieceHashed(1, false, nil)
	// Dirty chunks should be cleared so we can try again.
	qt.Assert(t, qt.IsFalse(tt.pieceAllDirty(1)))
	tt.cl.unlock()
}

// Check the behaviour of Torrent.Metainfo when metadata is not completed.
func TestTorrentMetainfoIncompleteMetadata(t *testing.T) {
	cfg := TestingConfig(t)
	cfg.Debug = true
	// Disable this just because we manually initiate a connection without it.
	cfg.MinPeerExtensions.SetBit(pp.ExtensionBitFast, false)
	cl, err := NewClient(cfg)
	qt.Assert(t, qt.IsNil(err))
	defer cl.Close()

	mi := testutil.GreetingMetaInfo()
	ih := mi.HashInfoBytes()

	tt, _ := cl.AddTorrentInfoHash(ih)
	qt.Check(t, qt.IsNil(tt.Metainfo().InfoBytes))
	qt.Check(t, qt.IsFalse(tt.haveAllMetadataPieces()))

	nc, err := net.Dial("tcp", fmt.Sprintf(":%d", cl.LocalPort()))
	qt.Assert(t, qt.IsNil(err))
	defer nc.Close()

	var pex PeerExtensionBits
	pex.SetBit(pp.ExtensionBitLtep, true)
	hr, err := pp.Handshake(context.Background(), nc, &ih, [20]byte{}, pex)
	qt.Assert(t, qt.IsNil(err))
	qt.Check(t, qt.IsTrue(hr.PeerExtensionBits.GetBit(pp.ExtensionBitLtep)))
	qt.Check(t, qt.Equals(hr.PeerID, cl.PeerID()))
	qt.Check(t, qt.Equals(hr.Hash, ih))

	qt.Check(t, qt.Equals(tt.metadataSize(), 0))

	func() {
		cl.lock()
		defer cl.unlock()
		go func() {
			_, err = nc.Write(pp.Message{
				Type:       pp.Extended,
				ExtendedID: pp.HandshakeExtendedID,
				ExtendedPayload: func() []byte {
					d := map[string]interface{}{
						"metadata_size": len(mi.InfoBytes),
					}
					b, err := bencode.Marshal(d)
					if err != nil {
						panic(err)
					}
					return b
				}(),
			}.MustMarshalBinary())
			qt.Assert(t, qt.IsNil(err))
		}()
		tt.metadataChanged.Wait()
	}()
	qt.Check(t, qt.DeepEquals(tt.metadataBytes, make([]byte, len(mi.InfoBytes))))
	qt.Check(t, qt.IsFalse(tt.haveAllMetadataPieces()))
	qt.Check(t, qt.IsNil(tt.Metainfo().InfoBytes))
}

func TestRelativeAvailabilityHaveNone(t *testing.T) {
	var err error
	cl := newTestingClient(t)
	mi, _ := testutil.Greeting.Generate(5)
	tt := cl.newTorrentOpt(AddTorrentOpts{InfoHash: mi.HashInfoBytes()})
	tt.setChunkSize(2)
	g.MakeMapIfNil(&tt.conns)
	pc := PeerConn{}
	pc.t = tt
	pc.legacyPeerImpl = &pc
	pc.initRequestState()
	g.InitNew(&pc.callbacks)
	tt.cl.lock()
	tt.conns[&pc] = struct{}{}
	err = pc.peerSentHave(0)
	tt.cl.unlock()
	qt.Assert(t, qt.IsNil(err))
	err = tt.SetInfoBytes(mi.InfoBytes)
	qt.Assert(t, qt.IsNil(err))
	tt.cl.lock()
	err = pc.peerSentHaveNone()
	tt.cl.unlock()
	qt.Assert(t, qt.IsNil(err))
	tt.Drop()
	tt.assertAllPiecesRelativeAvailabilityZero()
}

// Wraps a storage.ClientImpl so that piece reads signal when initial hashing begins and then block
// until released. This holds a torrent in its initial piece-verification window for as long as the
// test needs, making the connection-rejection window deterministic.
type hashBlockingStorage struct {
	inner       storage.ClientImpl
	readStarted chan struct{}
	release     chan struct{}
	startedOnce sync.Once
}

func (me *hashBlockingStorage) OpenTorrent(
	ctx context.Context,
	info *metainfo.Info,
	infoHash metainfo.Hash,
) (storage.TorrentImpl, error) {
	t, err := me.inner.OpenTorrent(ctx, info, infoHash)
	if err != nil {
		return t, err
	}
	if inner := t.PieceWithHash; inner != nil {
		t.PieceWithHash = func(p metainfo.Piece, pieceHash g.Option[[]byte]) storage.PieceImpl {
			return &hashBlockingPiece{inner(p, pieceHash), me}
		}
	}
	if inner := t.Piece; inner != nil {
		t.Piece = func(p metainfo.Piece) storage.PieceImpl {
			return &hashBlockingPiece{inner(p), me}
		}
	}
	// Force all reads through PieceImpl.ReadAt so they can be blocked.
	t.NewReader = nil
	t.NewPieceReader = nil
	return t, nil
}

// The wrapper deliberately exposes only the plain PieceImpl method set: optimized read paths
// (io.WriterTo, storage.PieceReaderer, storage.SelfHashing) on the wrapped piece must not be
// visible, or hashing would bypass the blocking ReadAt.
type hashBlockingPiece struct {
	storage.PieceImpl
	s *hashBlockingStorage
}

func (me *hashBlockingPiece) ReadAt(b []byte, off int64) (int, error) {
	me.s.startedOnce.Do(func() { close(me.s.readStarted) })
	<-me.s.release
	return me.PieceImpl.ReadAt(b, off)
}

func rejectedAcceptedConnsValue() int64 {
	if v := torrent.Get("rejected accepted connections"); v != nil {
		return v.(*expvar.Int).Value()
	}
	return 0
}

// A peer added while the remote torrent is still performing its initial piece verification must
// not be lost. The dialing torrent pops the peer from its pending queue and never retries a failed
// handshake, so a pre-handshake rejection leaves no peer to contact and the download stalls
// forever.
func TestPeerLostAfterRejectionDuringInitialVerification(t *testing.T) {
	testPeerAddedDuringInitialVerification(t, false)
}

// Same scenario with AlwaysWantConns on the seeder: it accepts the connection during initial
// verification and the download completes. Guards that escape hatch against regressions.
func TestAlwaysWantConnsAllowsPeerDuringInitialVerification(t *testing.T) {
	testPeerAddedDuringInitialVerification(t, true)
}

// A seeder verifying pre-existing data has its first hash read blocked, holding it in the
// initial-verification window. During that window a leecher is given the seeder as its only peer,
// exactly once. Once the window closes the download must still complete, whether the seeder
// accepted the connection during verification or the leecher retries the peer later.
func testPeerAddedDuringInitialVerification(t *testing.T, seederAlwaysWantConns bool) {
	greetingTempDir, mi := testutil.GreetingTestTorrent()
	defer os.RemoveAll(greetingTempDir)

	// Part files disabled so piece completion cannot be inferred from file presence: the seeder
	// must hash the existing data, opening the initial-verification window this test targets.
	fileStorage := storage.NewFileOpts(storage.NewFileClientOpts{
		ClientBaseDir:   greetingTempDir,
		PieceCompletion: storage.NewMapPieceCompletion(),
		UsePartFiles:    g.Some(false),
	})
	defer fileStorage.Close()
	seederStorage := &hashBlockingStorage{
		inner:       fileStorage,
		readStarted: make(chan struct{}),
		release:     make(chan struct{}),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(seederStorage.release) }) }

	cfg := TestingConfig(t)
	cfg.Seed = true
	cfg.DataDir = greetingTempDir
	cfg.DefaultStorage = seederStorage
	cfg.DisableUTP = true
	cfg.DisablePEX = true
	cfg.AlwaysWantConns = seederAlwaysWantConns
	seeder, err := NewClient(cfg)
	qt.Assert(t, qt.IsNil(err))
	defer seeder.Close()
	// Registered after seeder.Close so blocked piece hashers are released before the client shuts
	// down, whichever way the test exits.
	defer release()

	rejectedBefore := rejectedAcceptedConnsValue()

	seederTorrent, isNew, err := seeder.AddTorrentSpec(TorrentSpecFromMetaInfo(mi))
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.IsTrue(isNew))

	// Initial verification has started reading piece data and is now blocked. Until it completes,
	// the seeder has needData()==false and haveAnyPieces()==false, so it rejects incoming
	// connections.
	select {
	case <-seederStorage.readStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the seeder to start verifying pieces")
	}
	qt.Assert(t, qt.IsFalse(seederTorrent.Complete().Bool()))

	cfg = TestingConfig(t)
	cfg.Seed = false
	cfg.DataDir = t.TempDir()
	cfg.DisableUTP = true
	cfg.DisablePEX = true
	leecher, err := NewClient(cfg)
	qt.Assert(t, qt.IsNil(err))
	defer leecher.Close()

	leecherTorrent, isNew, err := leecher.AddTorrentSpec(TorrentSpecFromMetaInfo(mi))
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.IsTrue(isNew))
	leecherTorrent.DownloadAll()

	// The seeder is the only peer the leecher will ever hear about: DHT, trackers, PEX and uTP are
	// all disabled, so there is no rediscovery mechanism.
	added := leecherTorrent.AddClientPeer(seeder)
	qt.Assert(t, qt.Not(qt.Equals(added, 0)))

	// Make sure the connection attempt happens while the seeder is still verifying, and reaches a
	// conclusion before verification is allowed to finish. Either the seeder accepts the
	// connection during verification (one acceptable fix), or it rejects it and all of the
	// leecher's attempts for the peer conclude (half-open drains) before the window closes.
	settleDeadline := time.Now().Add(10 * time.Second)
	for {
		stats := leecherTorrent.Stats()
		if stats.ActivePeers > 0 {
			break
		}
		if rejectedAcceptedConnsValue() > rejectedBefore &&
			stats.HalfOpenPeers == 0 {
			break
		}
		if time.Now().After(settleDeadline) {
			t.Fatalf(
				"timed out waiting for a connection attempt during verification; leecher gauges: %+v",
				stats.TorrentGauges,
			)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Let the seeder finish its initial verification. From here on it would happily serve data.
	release()
	select {
	case <-seederTorrent.Complete().On():
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the seeder to finish verifying")
	}

	// The peer the leecher was given was only temporarily unavailable, so the download should
	// still complete without the peer being added again. With the current behavior the leecher
	// never retries the popped peer and stalls with zero peers.
	done := make(chan struct{})
	go func() {
		leecher.WaitAll()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		stats := leecherTorrent.Stats()
		t.Fatalf(
			"leecher stalled: its only peer was dropped after rejection during the seeder's initial verification and never retried; gauges: %+v, useful bytes read: %v",
			stats.TorrentGauges, stats.BytesReadUsefulData.Int64(),
		)
	}
}
