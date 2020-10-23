package initialsync

import (
	"context"
	"fmt"
	"testing"

	"github.com/kevinms/leakybucket-go"
	eth "github.com/prysmaticlabs/ethereumapis/eth/v1alpha1"
	"github.com/prysmaticlabs/prysm/shared/testutil/assert"
)

func TestBlocksFetcher_nonSkippedSlotAfter(t *testing.T) {
	peersGen := func(size int) []*peerData {
		blocks := append(makeSequence(1, 64), makeSequence(500, 640)...)
		blocks = append(blocks, makeSequence(51200, 51264)...)
		blocks = append(blocks, 55000)
		blocks = append(blocks, makeSequence(57000, 57256)...)
		var peersData []*peerData
		for i := 0; i < size; i++ {
			peersData = append(peersData, &peerData{
				blocks:         blocks,
				finalizedEpoch: 1800,
				headSlot:       57000,
			})
		}
		return peersData
	}
	chainConfig := struct {
		peers []*peerData
	}{
		peers: peersGen(5),
	}

	mc, p2p, _ := initializeTestServices(t, []uint64{}, chainConfig.peers)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fetcher := newBlocksFetcher(
		ctx,
		&blocksFetcherConfig{
			finalizationFetcher: mc,
			headFetcher:         mc,
			p2p:                 p2p,
		},
	)
	fetcher.rateLimiter = leakybucket.NewCollector(6400, 6400, false)
	seekSlots := map[uint64]uint64{
		0:     1,
		10:    11,
		31:    32,
		32:    33,
		63:    64,
		64:    500,
		160:   500,
		352:   500,
		480:   500,
		512:   513,
		639:   640,
		640:   51200,
		6640:  51200,
		51200: 51201,
	}
	for seekSlot, expectedSlot := range seekSlots {
		t.Run(fmt.Sprintf("range: %d (%d-%d)", expectedSlot-seekSlot, seekSlot, expectedSlot), func(t *testing.T) {
			slot, err := fetcher.nonSkippedSlotAfter(ctx, seekSlot)
			assert.NoError(t, err)
			assert.Equal(t, expectedSlot, slot, "Unexpected slot")
		})
	}

	t.Run("test isolated non-skipped slot", func(t *testing.T) {
		seekSlot := uint64(51264)
		expectedSlot := uint64(55000)
		found := false
		var i int
		for i = 0; i < 100; i++ {
			slot, err := fetcher.nonSkippedSlotAfter(ctx, seekSlot)
			assert.NoError(t, err)
			if slot == expectedSlot {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Isolated non-skipped slot not found in %d iterations: %v", i, expectedSlot)
		} else {
			t.Logf("Isolated non-skipped slot found in %d iterations", i)
		}
	})
}

func TestBlocksFetcher_currentHeadAndTargetEpochs(t *testing.T) {
	tests := []struct {
		name               string
		mode               syncMode
		peers              []*peerData
		headEpoch          uint64
		targetEpoch        uint64
		targetEpochSupport int
	}{
		{
			name:               "ignore lower epoch peers (stop on finalized epoch)",
			mode:               modeStopOnFinalizedEpoch,
			headEpoch:          4,
			targetEpoch:        10,
			targetEpochSupport: 3,
			peers: []*peerData{
				{finalizedEpoch: 3, headSlot: 160},
				{finalizedEpoch: 3, headSlot: 160},
				{finalizedEpoch: 3, headSlot: 160},
				{finalizedEpoch: 3, headSlot: 160},
				{finalizedEpoch: 3, headSlot: 160},
				{finalizedEpoch: 8, headSlot: 320},
				{finalizedEpoch: 8, headSlot: 320},
				{finalizedEpoch: 10, headSlot: 320},
				{finalizedEpoch: 10, headSlot: 320},
				{finalizedEpoch: 10, headSlot: 320},
			},
		},
		{
			name:               "resolve ties (stop on finalized epoch)",
			mode:               modeStopOnFinalizedEpoch,
			headEpoch:          4,
			targetEpoch:        10,
			targetEpochSupport: 3,
			peers: []*peerData{
				{finalizedEpoch: 3, headSlot: 160},
				{finalizedEpoch: 3, headSlot: 160},
				{finalizedEpoch: 3, headSlot: 160},
				{finalizedEpoch: 3, headSlot: 160},
				{finalizedEpoch: 3, headSlot: 160},
				{finalizedEpoch: 8, headSlot: 320},
				{finalizedEpoch: 8, headSlot: 320},
				{finalizedEpoch: 8, headSlot: 320},
				{finalizedEpoch: 10, headSlot: 320},
				{finalizedEpoch: 10, headSlot: 320},
				{finalizedEpoch: 10, headSlot: 320},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc, p2p, _ := initializeTestServices(t, []uint64{}, tt.peers)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fetcher := newBlocksFetcher(
				ctx,
				&blocksFetcherConfig{
					headFetcher:         mc,
					finalizationFetcher: mc,
					p2p:                 p2p,
				},
			)
			mc.FinalizedCheckPoint = &eth.Checkpoint{
				Epoch: tt.headEpoch,
			}
			fetcher.mode = modeStopOnFinalizedEpoch
			headEpoch, targetEpoch, peers := fetcher.currentHeadAndTargetEpochs()
			assert.Equal(t, tt.headEpoch, headEpoch, "Unexpected head epoch")
			assert.Equal(t, tt.targetEpoch, targetEpoch, "Unexpected target epoch")
			assert.Equal(t, tt.targetEpochSupport, len(peers), "Unexpected number of peers supporting target epoch")
		})
	}
}
