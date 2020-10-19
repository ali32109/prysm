package client

import (
	"bytes"
	"context"
	"fmt"

	"github.com/pkg/errors"
	ethpb "github.com/prysmaticlabs/ethereumapis/eth/v1alpha1"
	"github.com/prysmaticlabs/prysm/shared/featureconfig"
	"github.com/prysmaticlabs/prysm/shared/params"
	"github.com/prysmaticlabs/prysm/validator/db/kv"
)

var failedPreAttSignLocalErr = "attempted to make slashable attestation, rejected by local slashing protection"
var failedPreAttSignExternalErr = "attempted to make slashable attestation, rejected by external slasher service"
var failedPostAttSignExternalErr = "external slasher service detected a submitted slashable attestation"

func (v *validator) preAttSignValidations(ctx context.Context, indexedAtt *ethpb.IndexedAttestation, pubKey [48]byte) error {
	fmtKey := fmt.Sprintf("%#x", pubKey[:])

	v.attesterHistoryByPubKeyLock.RLock()
	attesterHistory, ok := v.attesterHistoryByPubKey[pubKey]
	v.attesterHistoryByPubKeyLock.RUnlock()
	if ok && v.isNewAttSlashable(ctx, attesterHistory, indexedAtt.Data.Source.Epoch, indexedAtt.Data.Target.Epoch, indexedAtt) {
		if v.emitAccountMetrics {
			ValidatorAttestFailVec.WithLabelValues(fmtKey).Inc()
		}
		return errors.New(failedPreAttSignLocalErr)
	} else if !ok {
		log.WithField("publicKey", fmtKey).Debug("Could not get local slashing protection data for validator")
	}

	if featureconfig.Get().SlasherProtection && v.protector != nil {
		if !v.protector.CheckAttestationSafety(ctx, indexedAtt) {
			if v.emitAccountMetrics {
				ValidatorAttestFailVecSlasher.WithLabelValues(fmtKey).Inc()
			}
			return errors.New(failedPreAttSignExternalErr)
		}
	}
	return nil
}

func (v *validator) postAttSignUpdate(ctx context.Context, indexedAtt *ethpb.IndexedAttestation, pubKey [48]byte) error {
	fmtKey := fmt.Sprintf("%#x", pubKey[:])
	v.attesterHistoryByPubKeyLock.Lock()
	attesterHistory, ok := v.attesterHistoryByPubKey[pubKey]
	if ok {
		attesterHistory = markAttestationForTargetEpoch(ctx, attesterHistory, indexedAtt.Data.Source.Epoch, indexedAtt.Data.Target.Epoch)
		v.attesterHistoryByPubKey[pubKey] = attesterHistory
	} else {
		log.WithField("publicKey", fmtKey).Debug("Could not get local slashing protection data for validator")
	}
	v.attesterHistoryByPubKeyLock.Unlock()

	if featureconfig.Get().SlasherProtection && v.protector != nil {
		if !v.protector.CommitAttestation(ctx, indexedAtt) {
			if v.emitAccountMetrics {
				ValidatorAttestFailVecSlasher.WithLabelValues(fmtKey).Inc()
			}
			return errors.New(failedPostAttSignExternalErr)
		}
	}
	return nil
}

// isNewAttSlashable uses the attestation history to determine if an attestation of sourceEpoch
// and targetEpoch would be slashable. It can detect double, surrounding, and surrounded votes.
func (v *validator) isNewAttSlashable(ctx context.Context, history *kv.EncHistoryData, sourceEpoch, targetEpoch uint64, indexedAtt *ethpb.IndexedAttestation) bool {
	if history == nil {
		return false
	}
	wsPeriod := params.BeaconConfig().WeakSubjectivityPeriod
	_, sr, err := v.getDomainAndSigningRoot(ctx, indexedAtt.Data)

	// Previously pruned, we should return false.
	lew, err := history.GetLatestEpochWritten(ctx)
	if err != nil {
		log.WithError(err).Error("Could not get latest epoch written from encapsulated data")
		return false
	}
	if int(targetEpoch) <= int(lew)-int(wsPeriod) {
		return false
	}

	// Check if there has already been a vote for this target epoch.
	hd, err := history.GetTargetData(ctx, targetEpoch)
	if err != nil {
		log.WithError(err).Error("Could not get target data for target epoch: %d", targetEpoch)
		return false
	}
	if hd != (*kv.HistoryData)(nil) && !bytes.Equal(sr[:], hd.SigningRoot) {
		return true
	}

	// Check if the new attestation would be surrounding another attestation.
	for i := sourceEpoch; i <= targetEpoch; i++ {
		// Unattested for epochs are marked as (*kv.HistoryData)(nil).
		historyBoundry := safeTargetToSource(ctx, history, i)
		if historyBoundry == (*kv.HistoryData)(nil) {
			continue
		}

		if historyBoundry.Source > sourceEpoch {
			return true
		}
	}

	// Check if the new attestation is being surrounded.
	for i := targetEpoch; i <= lew; i++ {
		if safeTargetToSource(ctx, history, i).Source < sourceEpoch {
			return true
		}
	}

	return false
}

// markAttestationForTargetEpoch returns the modified attestation history with the passed-in epochs marked
// as attested for. This is done to prevent the validator client from signing any slashable attestations.
func markAttestationForTargetEpoch(ctx context.Context, history *kv.EncHistoryData, sourceEpoch, targetEpoch uint64) *kv.EncHistoryData {
	if history == nil {
		return nil
	}
	wsPeriod := params.BeaconConfig().WeakSubjectivityPeriod
	lew, err := history.GetLatestEpochWritten(ctx)
	if err != nil {
		log.WithError(err).Error("Could not get latest epoch written from encapsulated data")
		return nil
	}
	if targetEpoch > lew {
		// If the target epoch to mark is ahead of latest written epoch, override the old targets and mark the requested epoch.
		// Limit the overwriting to one weak subjectivity period as further is not needed.
		maxToWrite := lew + wsPeriod
		for i := lew + 1; i < targetEpoch && i <= maxToWrite; i++ {
			history, err = history.SetTargetData(ctx, i%wsPeriod, (*kv.HistoryData)(nil))
			if err != nil {
				log.WithError(err).Error("Could not set target to the encapsulated data")
				return nil
			}
		}
		history, err = history.SetLatestEpochWritten(ctx, targetEpoch)
		if err != nil {
			log.WithError(err).Error("Could not set latest epoch written to the encapsulated data")
			return nil
		}
	}
	history, err = history.SetTargetData(ctx, targetEpoch%wsPeriod, &kv.HistoryData{Source: sourceEpoch})
	if err != nil {
		log.WithError(err).Error("Could not set target to the encapsulated data")
		return nil
	}
	return history
}

// safeTargetToSource makes sure the epoch accessed is within bounds, and if it's not it at
// returns the "default" FAR_FUTURE_EPOCH value.
func safeTargetToSource(ctx context.Context, history *kv.EncHistoryData, targetEpoch uint64) *kv.HistoryData {
	wsPeriod := params.BeaconConfig().WeakSubjectivityPeriod
	lew, err := history.GetLatestEpochWritten(ctx)
	if err != nil {
		log.WithError(err).Error("Could not get latest epoch written from encapsulated data")
		return nil
	}
	if targetEpoch > lew || int(targetEpoch) < int(lew)-int(wsPeriod) {
		return nil
	}
	hd, err := history.GetTargetData(ctx, targetEpoch%wsPeriod)
	if err != nil {
		log.WithError(err).Error("Could not get target data for target epoch: %d", targetEpoch)
		return nil
	}
	return hd
}
