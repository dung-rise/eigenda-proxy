package da_client

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/Layr-Labs/eigenda/api/clients"
	grpcdisperser "github.com/Layr-Labs/eigenda/api/grpc/disperser"
	"github.com/Layr-Labs/eigenda/disperser"
)

func PutBlob(ctx context.Context, m *clients.EigenDAClient, data []byte) (*grpcdisperser.BlobInfo, error) {
	resultChan, errorChan := PutBlobAsync(ctx, m, data)
	select { // no timeout here because we depend on the configured timeout in PutBlobAsync
	case result := <-resultChan:
		return result, nil
	case err := <-errorChan:
		return nil, err
	}
}

func PutBlobAsync(ctx context.Context, m *clients.EigenDAClient, data []byte) (resultChan chan *grpcdisperser.BlobInfo, errChan chan error) {
	resultChan = make(chan *grpcdisperser.BlobInfo, 1)
	errChan = make(chan error, 1)
	go putBlob(ctx, m, data, resultChan, errChan)
	return
}

func putBlob(ctx context.Context, m *clients.EigenDAClient, rawData []byte, resultChan chan *grpcdisperser.BlobInfo, errChan chan error) {
	m.Log.Info("Attempting to disperse blob to EigenDA using patched function")

	// encode blob
	if m.Codec == nil {
		errChan <- fmt.Errorf("Codec cannot be nil")
		return
	}

	data, err := m.Codec.EncodeBlob(rawData)
	if err != nil {
		errChan <- fmt.Errorf("error encoding blob: %w", err)
		return
	}

	customQuorumNumbers := make([]uint8, len(m.Config.CustomQuorumIDs))
	for i, e := range m.Config.CustomQuorumIDs {
		customQuorumNumbers[i] = uint8(e)
	}
	// disperse blob
	blobStatus, requestID, err := m.Client.DisperseBlobAuthenticated(ctx, data, customQuorumNumbers)
	if err != nil {
		errChan <- fmt.Errorf("error initializing DisperseBlobAuthenticated() client: %w", err)
		return
	}

	// process response
	if *blobStatus == disperser.Failed {
		m.Log.Error("Unable to disperse blob to EigenDA, aborting", "err", err)
		errChan <- fmt.Errorf("reply status is %d", blobStatus)
		return
	}

	base64RequestID := base64.StdEncoding.EncodeToString(requestID)
	m.Log.Info("Blob dispersed to EigenDA, now waiting for confirmation", "requestID", base64RequestID)

	ticker := time.NewTicker(m.Config.StatusQueryRetryInterval)
	defer ticker.Stop()

	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, m.Config.StatusQueryTimeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			errChan <- fmt.Errorf("timed out waiting for EigenDA blob to confirm blob with request id=%s: %w", base64RequestID, ctx.Err())
			return
		case <-ticker.C:
			statusRes, err := m.Client.GetBlobStatus(ctx, requestID)
			if err != nil {
				m.Log.Error("Unable to retrieve blob dispersal status, will retry", "requestID", base64RequestID, "err", err)
				continue
			}

			switch statusRes.Status {
			case grpcdisperser.BlobStatus_PROCESSING, grpcdisperser.BlobStatus_DISPERSING:
				m.Log.Info("Blob submitted, waiting for dispersal from EigenDA", "requestID", base64RequestID)
			case grpcdisperser.BlobStatus_FAILED:
				m.Log.Error("EigenDA blob dispersal failed in processing", "requestID", base64RequestID, "err", err)
				errChan <- fmt.Errorf("EigenDA blob dispersal failed in processing, requestID=%s: %w", base64RequestID, err)
				return
			case grpcdisperser.BlobStatus_INSUFFICIENT_SIGNATURES:
				m.Log.Error("EigenDA blob dispersal failed in processing with insufficient signatures", "requestID", base64RequestID, "err", err)
				errChan <- fmt.Errorf("EigenDA blob dispersal failed in processing with insufficient signatures, requestID=%s: %w", base64RequestID, err)
				return
			// case grpcdisperser.BlobStatus_CONFIRMED:
			// m.Log.Info("EigenDA blob confirmed, waiting for finalization", "requestID", base64RequestID)
			case grpcdisperser.BlobStatus_CONFIRMED, grpcdisperser.BlobStatus_FINALIZED:
				batchHeaderHashHex := fmt.Sprintf("0x%s", hex.EncodeToString(statusRes.Info.BlobVerificationProof.BatchMetadata.BatchHeaderHash))
				m.Log.Info("Successfully dispersed blob to EigenDA", "requestID", base64RequestID, "batchHeaderHash", batchHeaderHashHex)
				resultChan <- statusRes.Info
				return
			default:
				errChan <- fmt.Errorf("EigenDA blob dispersal failed in processing with reply status %d", statusRes.Status)
				return
			}
		}
	}
}
