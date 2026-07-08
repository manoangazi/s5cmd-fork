package storage

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	urlpkg "net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/storage/url"
)

var sentinelURL = urlpkg.URL{}

const (
	// deleteObjectsMax is the max allowed objects to be deleted on single HTTP
	// request.
	deleteObjectsMax = 1000

	// Amazon Accelerated Transfer endpoint
	transferAccelEndpoint = "s3-accelerate.amazonaws.com"

	// Google Cloud Storage endpoint
	gcsEndpoint = "storage.googleapis.com"

	// the key of the object metadata which is used to handle retry decision on NoSuchUpload error
	metadataKeyRetryID = "s5cmd-upload-retry-id"
)

// Re-used AWS configs dramatically improve performance.
var globalSessionCache = &SessionCache{
	sessions: map[Options]*s3Session{},
}

// skipFutureObjects controls s5cmd's guard that omits objects whose
// LastModified leads the local clock at listing time. Upstream that guard is
// always on (it protects wildcard cp/sync from grabbing objects written
// mid-listing), but it also makes `ls` silently hide freshly-uploaded objects
// whenever the S3 server's clock leads the local clock — which repeatedly broke
// browsing and post-upload verification here. In this fork the guard is OFF by
// default so a desktop S3 manager always sees every object; set
// S5CMD_SKIP_FUTURE_OBJECTS to restore the upstream behavior.
var skipFutureObjects = os.Getenv("S5CMD_SKIP_FUTURE_OBJECTS") != ""

// uploadChecksumAlgorithm selects the checksum algorithm requested on uploads.
// Both CRC32C (default) and CRC64NVME are full-object-capable, so either yields
// a whole-object checksum comparable to a locally-computed digest — the property
// that lets multipart uploads be bit-verified. Set via S5CMD_UPLOAD_CHECKSUM_ALGO
// (values: "crc32c" or "crc64nvme"); anything else falls back to CRC32C.
// SHA-family algorithms are intentionally not offered: S3 only supports composite
// (per-part) checksums for them on multipart uploads, which are not comparable to
// a whole-file digest.
func uploadChecksumAlgorithm() types.ChecksumAlgorithm {
	switch strings.ToLower(os.Getenv("S5CMD_UPLOAD_CHECKSUM_ALGO")) {
	case "crc64nvme":
		return types.ChecksumAlgorithmCrc64nvme
	default:
		return types.ChecksumAlgorithmCrc32c
	}
}

// checksumAlgoString maps the ChecksumAlgorithm slice ListObjectsV2 returns for
// an object into a single lowercase algorithm name for the Object.ChecksumAlgo
// field ("crc32c", "crc64nvme", "sha256", "sha1", "crc32"). Returns "" when the
// object carries no checksum. AWS returns at most one algorithm per object here,
// so the first entry is authoritative.
func checksumAlgoString(algos []types.ChecksumAlgorithm) string {
	if len(algos) == 0 {
		return ""
	}
	return strings.ToLower(string(algos[0]))
}

// S3 is a storage type which interacts with an S3 API client, a
// Downloader and an Uploader.
type S3 struct {
	api                    *s3.Client
	downloader             *manager.Downloader
	uploader               *manager.Uploader
	endpointURL            urlpkg.URL
	dryRun                 bool
	useListObjectsV1       bool
	noSuchUploadRetryCount int
	requestPayerStr        string
}

func (s *S3) requestPayer() types.RequestPayer {
	if s.requestPayerStr == "" {
		return ""
	}
	return types.RequestPayer(s.requestPayerStr)
}

func parseEndpoint(endpoint string) (urlpkg.URL, error) {
	if endpoint == "" {
		return sentinelURL, nil
	}

	u, err := urlpkg.Parse(endpoint)
	if err != nil {
		return sentinelURL, fmt.Errorf("parse endpoint %q: %v", endpoint, err)
	}

	return *u, nil
}

// NewS3Storage creates new S3 session.
func newS3Storage(ctx context.Context, opts Options) (*S3, error) {
	endpointURL, err := parseEndpoint(opts.Endpoint)
	if err != nil {
		return nil, err
	}

	sess, err := globalSessionCache.newSession(ctx, opts)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(sess.cfg, sess.s3OptFns...)

	return &S3{
		api:                    client,
		downloader:             manager.NewDownloader(client),
		uploader:               manager.NewUploader(client, withFullObjectChecksum),
		endpointURL:            endpointURL,
		dryRun:                 opts.DryRun,
		useListObjectsV1:       opts.UseListObjectsV1,
		requestPayerStr:        opts.RequestPayer,
		noSuchUploadRetryCount: opts.NoSuchUploadRetryCount,
	}, nil
}

// Stat retrieves metadata from S3 object without returning the object itself.
func (s *S3) Stat(ctx context.Context, url *url.URL) (*Object, error) {
	input := &s3.HeadObjectInput{
		Bucket:       aws.String(url.Bucket),
		Key:          aws.String(url.Path),
		RequestPayer: s.requestPayer(),
		ChecksumMode: types.ChecksumModeEnabled,
	}
	if url.VersionID != "" {
		input.VersionId = aws.String(url.VersionID)
	}

	output, err := s.api.HeadObject(ctx, input)
	if err != nil {
		if errHasCode(err, "NotFound") {
			return nil, &ErrGivenObjectNotFound{ObjectAbsPath: url.Absolute()}
		}
		return nil, err
	}

	etag := aws.ToString(output.ETag)
	mod := aws.ToTime(output.LastModified)

	obj := &Object{
		URL:               url,
		Etag:              strings.Trim(etag, `"`),
		ChecksumCRC32C:    aws.ToString(output.ChecksumCRC32C),
		ChecksumCRC64NVME: aws.ToString(output.ChecksumCRC64NVME),
		ChecksumSHA256:    aws.ToString(output.ChecksumSHA256),
		ModTime:           &mod,
		Size:              aws.ToInt64(output.ContentLength),
	}

	if s.noSuchUploadRetryCount > 0 {
		if retryID, ok := output.Metadata[metadataKeyRetryID]; ok {
			obj.retryID = retryID
		}
	}

	return obj, nil
}

// List is a non-blocking S3 list operation which paginates and filters S3
// keys. If no object found or an error is encountered during this period,
// it sends these errors to object channel.
func (s *S3) List(ctx context.Context, url *url.URL, _ bool) <-chan *Object {
	if url.VersionID != "" || url.AllVersions {
		return s.listObjectVersions(ctx, url)
	}
	if s.useListObjectsV1 {
		return s.listObjects(ctx, url)
	}

	return s.listObjectsV2(ctx, url)
}

func (s *S3) listObjectVersions(ctx context.Context, url *url.URL) <-chan *Object {
	listInput := &s3.ListObjectVersionsInput{
		Bucket: aws.String(url.Bucket),
		Prefix: aws.String(url.Prefix),
	}

	if url.Delimiter != "" {
		listInput.Delimiter = aws.String(url.Delimiter)
	}

	objCh := make(chan *Object)

	go func() {
		defer close(objCh)
		objectFound := false

		var now time.Time

		paginator := s3.NewListObjectVersionsPaginator(s.api, listInput)
		for paginator.HasMorePages() {
			p, err := paginator.NextPage(ctx)
			if err != nil {
				objCh <- &Object{Err: err}
				return
			}

			for _, c := range p.CommonPrefixes {
				prefix := aws.ToString(c.Prefix)
				if !url.Match(prefix) {
					continue
				}

				newurl := url.Clone()
				newurl.Path = prefix
				objCh <- &Object{
					URL:  newurl,
					Type: ObjectType{os.ModeDir},
				}

				objectFound = true
			}
			// track the instant object iteration began,
			// so it can be used to bypass objects created after this instant
			if now.IsZero() {
				now = time.Now().UTC()
			}

			// iterate over all versions of the objects (except the delete markers)
			for _, v := range p.Versions {
				key := aws.ToString(v.Key)
				if !url.Match(key) {
					continue
				}
				if url.VersionID != "" && url.VersionID != aws.ToString(v.VersionId) {
					continue
				}

				mod := aws.ToTime(v.LastModified).UTC()
				if skipFutureObjects && mod.After(now) {
					objectFound = true
					continue
				}

				var objtype os.FileMode
				if strings.HasSuffix(key, "/") {
					objtype = os.ModeDir
				}

				newurl := url.Clone()
				newurl.Path = aws.ToString(v.Key)
				newurl.VersionID = aws.ToString(v.VersionId)
				etag := aws.ToString(v.ETag)

				objCh <- &Object{
					URL:          newurl,
					Etag:         strings.Trim(etag, `"`),
					ModTime:      &mod,
					Type:         ObjectType{objtype},
					Size:         aws.ToInt64(v.Size),
					StorageClass: StorageClass(v.StorageClass),
				}

				objectFound = true
			}

			// iterate over all delete marker versions of the objects
			for _, d := range p.DeleteMarkers {
				key := aws.ToString(d.Key)
				if !url.Match(key) {
					continue
				}
				if url.VersionID != "" && url.VersionID != aws.ToString(d.VersionId) {
					continue
				}

				mod := aws.ToTime(d.LastModified).UTC()
				if skipFutureObjects && mod.After(now) {
					objectFound = true
					continue
				}

				var objtype os.FileMode
				if strings.HasSuffix(key, "/") {
					objtype = os.ModeDir
				}

				newurl := url.Clone()
				newurl.Path = aws.ToString(d.Key)
				newurl.VersionID = aws.ToString(d.VersionId)

				objCh <- &Object{
					URL:     newurl,
					ModTime: &mod,
					Type:    ObjectType{objtype},
					Size:    0,
				}

				objectFound = true
			}
		}

		if !objectFound && !url.IsBucket() {
			objCh <- &Object{Err: ErrNoObjectFound}
		}
	}()

	return objCh
}

func (s *S3) listObjectsV2(ctx context.Context, url *url.URL) <-chan *Object {
	listInput := &s3.ListObjectsV2Input{
		Bucket:       aws.String(url.Bucket),
		Prefix:       aws.String(url.Prefix),
		RequestPayer: s.requestPayer(),
	}

	if url.Delimiter != "" {
		listInput.Delimiter = aws.String(url.Delimiter)
	}

	objCh := make(chan *Object)

	go func() {
		defer close(objCh)
		objectFound := false

		var now time.Time

		paginator := s3.NewListObjectsV2Paginator(s.api, listInput)
		for paginator.HasMorePages() {
			p, err := paginator.NextPage(ctx)
			if err != nil {
				objCh <- &Object{Err: err}
				return
			}

			for _, c := range p.CommonPrefixes {
				prefix := aws.ToString(c.Prefix)
				if !url.Match(prefix) {
					continue
				}

				newurl := url.Clone()
				newurl.Path = prefix
				objCh <- &Object{
					URL:  newurl,
					Type: ObjectType{os.ModeDir},
				}

				objectFound = true
			}
			// track the instant object iteration began,
			// so it can be used to bypass objects created after this instant
			if now.IsZero() {
				now = time.Now().UTC()
			}

			for _, c := range p.Contents {
				key := aws.ToString(c.Key)
				if !url.Match(key) {
					continue
				}

				mod := aws.ToTime(c.LastModified).UTC()
				if skipFutureObjects && mod.After(now) {
					objectFound = true
					continue
				}

				var objtype os.FileMode
				if strings.HasSuffix(key, "/") {
					objtype = os.ModeDir
				}

				newurl := url.Clone()
				newurl.Path = aws.ToString(c.Key)
				etag := aws.ToString(c.ETag)

				objCh <- &Object{
					URL:          newurl,
					Etag:         strings.Trim(etag, `"`),
					ChecksumAlgo: checksumAlgoString(c.ChecksumAlgorithm),
					ModTime:      &mod,
					Type:         ObjectType{objtype},
					Size:         aws.ToInt64(c.Size),
					StorageClass: StorageClass(c.StorageClass),
				}

				objectFound = true
			}
		}

		if !objectFound && !url.IsBucket() {
			objCh <- &Object{Err: ErrNoObjectFound}
		}
	}()

	return objCh
}

// listObjects is used for cloud services that does not support S3
// ListObjectsV2 API. I'm looking at you GCS.
func (s *S3) listObjects(ctx context.Context, url *url.URL) <-chan *Object {
	listInput := &s3.ListObjectsInput{
		Bucket:       aws.String(url.Bucket),
		Prefix:       aws.String(url.Prefix),
		RequestPayer: s.requestPayer(),
	}

	if url.Delimiter != "" {
		listInput.Delimiter = aws.String(url.Delimiter)
	}

	objCh := make(chan *Object)

	go func() {
		defer close(objCh)
		objectFound := false

		var now time.Time

		for {
			p, err := s.api.ListObjects(ctx, listInput)
			if err != nil {
				objCh <- &Object{Err: err}
				return
			}

			for _, c := range p.CommonPrefixes {
				prefix := aws.ToString(c.Prefix)
				if !url.Match(prefix) {
					continue
				}

				newurl := url.Clone()
				newurl.Path = prefix
				objCh <- &Object{
					URL:  newurl,
					Type: ObjectType{os.ModeDir},
				}

				objectFound = true
			}
			// track the instant object iteration began,
			// so it can be used to bypass objects created after this instant
			if now.IsZero() {
				now = time.Now().UTC()
			}

			for _, c := range p.Contents {
				key := aws.ToString(c.Key)
				if !url.Match(key) {
					continue
				}

				mod := aws.ToTime(c.LastModified).UTC()
				if skipFutureObjects && mod.After(now) {
					objectFound = true
					continue
				}

				var objtype os.FileMode
				if strings.HasSuffix(key, "/") {
					objtype = os.ModeDir
				}

				newurl := url.Clone()
				newurl.Path = aws.ToString(c.Key)
				etag := aws.ToString(c.ETag)

				objCh <- &Object{
					URL:          newurl,
					Etag:         strings.Trim(etag, `"`),
					ChecksumAlgo: checksumAlgoString(c.ChecksumAlgorithm),
					ModTime:      &mod,
					Type:         ObjectType{objtype},
					Size:         aws.ToInt64(c.Size),
					StorageClass: StorageClass(c.StorageClass),
				}

				objectFound = true
			}

			if !aws.ToBool(p.IsTruncated) {
				break
			}
			if p.NextMarker != nil {
				listInput.Marker = p.NextMarker
			} else if len(p.Contents) > 0 {
				listInput.Marker = p.Contents[len(p.Contents)-1].Key
			} else {
				break
			}
		}

		if !objectFound && !url.IsBucket() {
			objCh <- &Object{Err: ErrNoObjectFound}
		}
	}()

	return objCh
}

// Copy is a single-object copy operation which copies objects to S3
// destination from another S3 source.
func (s *S3) Copy(ctx context.Context, from, to *url.URL, metadata Metadata) error {
	if s.dryRun {
		return nil
	}

	// SDK expects CopySource like "bucket[/key]"
	copySource := from.EscapedPath()

	input := &s3.CopyObjectInput{
		Bucket:       aws.String(to.Bucket),
		Key:          aws.String(to.Path),
		CopySource:   aws.String(copySource),
		RequestPayer: s.requestPayer(),
	}
	if from.VersionID != "" {
		// Unlike many other *Input and *Output types version ID is not a field,
		// but rather something that must be appended to CopySource string.
		// This is same in both v1 and v2 SDKs:
		// https://pkg.go.dev/github.com/aws/aws-sdk-go/service/s3#CopyObjectInput
		// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/s3#CopyObjectInput
		input.CopySource = aws.String(copySource + "?versionId=" + from.VersionID)
	}

	storageClass := metadata.StorageClass
	if storageClass != "" {
		input.StorageClass = types.StorageClass(storageClass)
	}

	acl := metadata.ACL
	if acl != "" {
		input.ACL = types.ObjectCannedACL(acl)
	}

	cacheControl := metadata.CacheControl
	if cacheControl != "" {
		input.CacheControl = aws.String(cacheControl)
	}

	expires := metadata.Expires
	if expires != "" {
		t, err := time.Parse(time.RFC3339, expires)
		if err != nil {
			return err
		}
		input.Expires = aws.Time(t)
	}

	sseEncryption := metadata.EncryptionMethod
	if sseEncryption != "" {
		input.ServerSideEncryption = types.ServerSideEncryption(sseEncryption)
		sseKmsKeyID := metadata.EncryptionKeyID
		if sseKmsKeyID != "" {
			input.SSEKMSKeyId = aws.String(sseKmsKeyID)
		}
	}

	contentEncoding := metadata.ContentEncoding
	if contentEncoding != "" {
		input.ContentEncoding = aws.String(contentEncoding)
	}

	contentDisposition := metadata.ContentDisposition
	if contentDisposition != "" {
		input.ContentDisposition = aws.String(contentDisposition)
	}

	if len(metadata.UserDefined) != 0 {
		input.Metadata = make(map[string]string, len(metadata.UserDefined))
		for k, v := range metadata.UserDefined {
			input.Metadata[k] = v
		}
	}

	// add retry ID to the object metadata
	if s.noSuchUploadRetryCount > 0 {
		if input.Metadata == nil {
			input.Metadata = make(map[string]string)
		}
		input.Metadata[metadataKeyRetryID] = *generateRetryID()
	}

	if metadata.Directive != "" {
		input.MetadataDirective = types.MetadataDirective(metadata.Directive)
	}

	if metadata.ContentType != "" {
		input.ContentType = aws.String(metadata.ContentType)
	}

	_, err := s.api.CopyObject(ctx, input)
	return err
}

// Read fetches the remote object and returns its contents as an io.ReadCloser.
func (s *S3) Read(ctx context.Context, src *url.URL) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket:       aws.String(src.Bucket),
		Key:          aws.String(src.Path),
		RequestPayer: s.requestPayer(),
	}
	if src.VersionID != "" {
		input.VersionId = aws.String(src.VersionID)
	}

	resp, err := s.api.GetObject(ctx, input)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (s *S3) Presign(ctx context.Context, from *url.URL, expire time.Duration) (string, error) {
	input := &s3.GetObjectInput{
		Bucket:       aws.String(from.Bucket),
		Key:          aws.String(from.Path),
		RequestPayer: s.requestPayer(),
	}

	presignClient := s3.NewPresignClient(s.api)
	req, err := presignClient.PresignGetObject(ctx, input, s3.WithPresignExpires(expire))
	if err != nil {
		return "", err
	}

	return req.URL, nil
}

// Get is a multipart download operation which downloads S3 objects into any
// destination that implements io.WriterAt interface.
// Makes a single 'GetObject' call if 'concurrency' is 1 and ignores 'partSize'.
func (s *S3) Get(
	ctx context.Context,
	from *url.URL,
	to io.WriterAt,
	concurrency int,
	partSize int64,
) (int64, error) {
	if s.dryRun {
		return 0, nil
	}

	input := &s3.GetObjectInput{
		Bucket:       aws.String(from.Bucket),
		Key:          aws.String(from.Path),
		RequestPayer: s.requestPayer(),
	}
	if from.VersionID != "" {
		input.VersionId = aws.String(from.VersionID)
	}

	return s.downloader.Download(ctx, to, input, func(d *manager.Downloader) {
		d.PartSize = partSize
		d.Concurrency = concurrency
	})
}

type SelectQuery struct {
	InputFormat           string
	InputContentStructure string
	FileHeaderInfo        string
	OutputFormat          string
	ExpressionType        string
	Expression            string
	CompressionType       string
}

type eventType string

const (
	jsonType    eventType = "json"
	csvType     eventType = "csv"
	parquetType eventType = "parquet"
)

func parseInputSerialization(e eventType, c string, delimiter string, headerInfo string) (*types.InputSerialization, error) {
	var s *types.InputSerialization

	switch e {
	case jsonType:
		s = &types.InputSerialization{
			JSON: &types.JSONInput{
				Type: types.JSONType(delimiter),
			},
		}
		if c != "" {
			s.CompressionType = types.CompressionType(c)
		}
	case csvType:
		s = &types.InputSerialization{
			CSV: &types.CSVInput{
				FieldDelimiter: aws.String(delimiter),
				FileHeaderInfo: types.FileHeaderInfo(headerInfo),
			},
		}
		if c != "" {
			s.CompressionType = types.CompressionType(c)
		}
	case parquetType:
		s = &types.InputSerialization{
			Parquet: &types.ParquetInput{},
		}
	default:
		return nil, fmt.Errorf("input format is not valid")
	}

	return s, nil
}

func parseOutputSerialization(e eventType, delimiter string, reader io.Reader) (*types.OutputSerialization, EventStreamDecoder, error) {
	var s *types.OutputSerialization
	var decoder EventStreamDecoder

	switch e {
	case jsonType:
		s = &types.OutputSerialization{
			JSON: &types.JSONOutput{},
		}
		decoder = NewJSONDecoder(reader)
	case csvType:
		s = &types.OutputSerialization{
			CSV: &types.CSVOutput{
				FieldDelimiter: aws.String(delimiter),
			},
		}
		decoder = NewCsvDecoder(reader)
	default:
		return nil, nil, fmt.Errorf("output serialization is not valid")
	}
	return s, decoder, nil
}

func (s *S3) Select(ctx context.Context, url *url.URL, query *SelectQuery, resultCh chan<- json.RawMessage) error {
	if s.dryRun {
		return nil
	}

	var (
		inputFormat  *types.InputSerialization
		outputFormat *types.OutputSerialization
		decoder      EventStreamDecoder
	)
	reader, writer := io.Pipe()

	inputFormat, err := parseInputSerialization(
		eventType(query.InputFormat),
		query.CompressionType,
		query.InputContentStructure,
		query.FileHeaderInfo,
	)
	if err != nil {
		return err
	}

	// set the delimiter to ','. Otherwise, delimiter is set to "lines" or "document"
	// for json queries.
	if query.InputFormat == string(jsonType) && query.OutputFormat == string(csvType) {
		query.InputContentStructure = ","
	}

	outputFormat, decoder, err = parseOutputSerialization(
		eventType(query.OutputFormat),
		query.InputContentStructure,
		reader,
	)
	if err != nil {
		return err
	}

	input := &s3.SelectObjectContentInput{
		Bucket:              aws.String(url.Bucket),
		Key:                 aws.String(url.Path),
		ExpressionType:      types.ExpressionType(query.ExpressionType),
		Expression:          aws.String(query.Expression),
		InputSerialization:  inputFormat,
		OutputSerialization: outputFormat,
	}

	resp, err := s.api.SelectObjectContent(ctx, input)
	if err != nil {
		return err
	}

	stream := resp.GetStream()

	go func() {
		defer writer.Close()

		eventch := stream.Events()
		defer stream.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-eventch:
				if !ok {
					return
				}

				switch e := event.(type) {
				case *types.SelectObjectContentEventStreamMemberRecords:
					writer.Write(e.Value.Payload)
				}
			}
		}
	}()
	for {
		val, err := decoder.Decode()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		resultCh <- val
	}

	return stream.Err()
}

// withFullObjectChecksum configures the upload manager so that its multipart
// path requests a FULL_OBJECT (whole-object) CRC32C checksum rather than the
// default COMPOSITE (hash-of-part-hashes, "…-N") one.
//
// The manager builds its CreateMultipartUploadInput by copying our
// PutObjectInput, but PutObjectInput has no ChecksumType field (checksum type
// only exists on the multipart create/complete calls), so the type would
// otherwise default server-side. We register a per-call Initialize middleware,
// scoped to the uploader's own client, that stamps ChecksumType=FULL_OBJECT
// onto the CreateMultipartUploadInput. A full-object CRC32C is directly
// comparable to a local streaming CRC32C of the whole file — the property that
// lets us bit-verify multipart uploads (impossible with a composite checksum,
// and impossible with SHA-family checksums, which S3 only supports as
// composite for multipart).
func withFullObjectChecksum(u *manager.Uploader) {
	u.ClientOptions = append(u.ClientOptions, func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Initialize.Add(
				middleware.InitializeMiddlewareFunc("s5cmdFullObjectChecksum",
					func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
						if cmu, ok := in.Parameters.(*s3.CreateMultipartUploadInput); ok {
							// Force FULL_OBJECT for any CRC algorithm so the multipart
							// checksum is a whole-object digest (not composite "…-N").
							isCRC := cmu.ChecksumAlgorithm == types.ChecksumAlgorithmCrc32c ||
								cmu.ChecksumAlgorithm == types.ChecksumAlgorithmCrc64nvme
							if isCRC && cmu.ChecksumType == "" {
								cmu.ChecksumType = types.ChecksumTypeFullObject
							}
						}
						return next.HandleInitialize(ctx, in)
					}),
				middleware.Before,
			)
		})
	})
}

// Put is a multipart upload operation to upload resources, which implements
// io.Reader interface, into S3 destination.
func (s *S3) Put(
	ctx context.Context,
	reader io.Reader,
	to *url.URL,
	metadata Metadata,
	concurrency int,
	partSize int64,
) error {
	if s.dryRun {
		return nil
	}

	contentType := metadata.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	input := &s3.PutObjectInput{
		Bucket:       aws.String(to.Bucket),
		Key:          aws.String(to.Path),
		Body:         reader,
		ContentType:  aws.String(contentType),
		Metadata:     make(map[string]string),
		RequestPayer: s.requestPayer(),
		// A CRC algorithm (CRC32C or CRC64NVME) gives a whole-object checksum we
		// can recompute locally to bit-verify uploads. A single-part PutObject is
		// inherently a full-object checksum; the multipart path is forced to
		// FULL_OBJECT by withFullObjectChecksum (ChecksumType lives on
		// CreateMultipartUpload, not PutObjectInput). SHA-family checksums can't do
		// this — S3 only supports composite ("…-N") checksums for SHA multipart.
		ChecksumAlgorithm: uploadChecksumAlgorithm(),
	}

	storageClass := metadata.StorageClass
	if storageClass != "" {
		input.StorageClass = types.StorageClass(storageClass)
	}

	acl := metadata.ACL
	if acl != "" {
		input.ACL = types.ObjectCannedACL(acl)
	}

	cacheControl := metadata.CacheControl
	if cacheControl != "" {
		input.CacheControl = aws.String(cacheControl)
	}

	expires := metadata.Expires
	if expires != "" {
		t, err := time.Parse(time.RFC3339, expires)
		if err != nil {
			return err
		}
		input.Expires = aws.Time(t)
	}

	sseEncryption := metadata.EncryptionMethod
	if sseEncryption != "" {
		input.ServerSideEncryption = types.ServerSideEncryption(sseEncryption)
		sseKmsKeyID := metadata.EncryptionKeyID
		if sseKmsKeyID != "" {
			input.SSEKMSKeyId = aws.String(sseKmsKeyID)
		}
	}

	contentEncoding := metadata.ContentEncoding
	if contentEncoding != "" {
		input.ContentEncoding = aws.String(contentEncoding)
	}

	contentDisposition := metadata.ContentDisposition
	if contentDisposition != "" {
		input.ContentDisposition = aws.String(contentDisposition)
	}

	if len(metadata.UserDefined) != 0 {
		input.Metadata = make(map[string]string, len(metadata.UserDefined))
		for k, v := range metadata.UserDefined {
			input.Metadata[k] = v
		}
	}

	// add retry ID to the object metadata
	if s.noSuchUploadRetryCount > 0 {
		input.Metadata[metadataKeyRetryID] = *generateRetryID()
	}

	uploaderOptsFn := func(u *manager.Uploader) {
		u.PartSize = partSize
		u.Concurrency = concurrency
	}
	_, err := s.uploader.Upload(ctx, input, uploaderOptsFn)

	if errHasCode(err, "NoSuchUpload") && s.noSuchUploadRetryCount > 0 {
		return s.retryOnNoSuchUpload(ctx, to, input, err, uploaderOptsFn)
	}

	return err
}

func (s *S3) retryOnNoSuchUpload(ctx context.Context, to *url.URL, input *s3.PutObjectInput,
	err error, uploaderOpts ...func(*manager.Uploader),
) error {
	var expectedRetryID string
	if ID, ok := input.Metadata[metadataKeyRetryID]; ok {
		expectedRetryID = ID
	}

	attempts := 0
	for ; errHasCode(err, "NoSuchUpload") && attempts < s.noSuchUploadRetryCount; attempts++ {
		// check if object exists and has the retry ID we provided, if it does
		// then it means that one of previous uploads was succesfull despite the received error.
		obj, sErr := s.Stat(ctx, to)
		if sErr == nil && obj.retryID == expectedRetryID {
			err = nil
			break
		}

		msg := log.DebugMessage{Err: fmt.Sprintf("Retrying to upload %v upon error: %q", to, err.Error())}
		log.Debug(msg)

		_, err = s.uploader.Upload(ctx, input, uploaderOpts...)
	}

	if errHasCode(err, "NoSuchUpload") && s.noSuchUploadRetryCount > 0 {
		err = fmt.Errorf("RetryOnNoSuchUpload: %v attempts to retry resulted in %v: %w", attempts, "NoSuchUpload", err)
	}
	return err
}

// chunk is an object identifier container which is used on MultiDelete
// operations. Since DeleteObjects API allows deleting objects up to 1000,
// splitting keys into multiple chunks is required.
type chunk struct {
	Bucket string
	Keys   []types.ObjectIdentifier
}

// calculateChunks calculates chunks for given URL channel and returns
// read-only chunk channel.
func (s *S3) calculateChunks(ch <-chan *url.URL) <-chan chunk {
	chunkch := make(chan chunk)

	chunkSize := deleteObjectsMax
	// delete each object individually if using gcs.
	if IsGoogleEndpoint(s.endpointURL) {
		chunkSize = 1
	}

	go func() {
		defer close(chunkch)

		var keys []types.ObjectIdentifier
		initKeys := func() {
			keys = make([]types.ObjectIdentifier, 0)
		}

		var bucket string
		for url := range ch {
			bucket = url.Bucket

			objid := types.ObjectIdentifier{Key: aws.String(url.Path)}
			if url.VersionID != "" {
				objid.VersionId = aws.String(url.VersionID)
			}

			keys = append(keys, objid)
			if len(keys) == chunkSize {
				chunkch <- chunk{
					Bucket: bucket,
					Keys:   keys,
				}
				initKeys()
			}
		}

		if len(keys) > 0 {
			chunkch <- chunk{
				Bucket: bucket,
				Keys:   keys,
			}
		}
	}()

	return chunkch
}

// Delete is a single object delete operation.
func (s *S3) Delete(ctx context.Context, url *url.URL) error {
	chunk := chunk{
		Bucket: url.Bucket,
		Keys: []types.ObjectIdentifier{
			{Key: aws.String(url.Path)},
		},
	}

	resultch := make(chan *Object, 1)
	defer close(resultch)

	s.doDelete(ctx, chunk, resultch)
	obj := <-resultch
	return obj.Err
}

// doDelete deletes the given keys given by chunk. Results are piggybacked via
// the Object container.
func (s *S3) doDelete(ctx context.Context, chunk chunk, resultch chan *Object) {
	if s.dryRun {
		for _, k := range chunk.Keys {
			key := fmt.Sprintf("s3://%v/%v", chunk.Bucket, aws.ToString(k.Key))
			url, _ := url.New(key)
			url.VersionID = aws.ToString(k.VersionId)
			resultch <- &Object{URL: url}
		}
		return
	}

	// GCS does not support multi delete.
	if IsGoogleEndpoint(s.endpointURL) {
		for _, k := range chunk.Keys {
			_, err := s.api.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket:       aws.String(chunk.Bucket),
				Key:          k.Key,
				RequestPayer: s.requestPayer(),
			})
			if err != nil {
				resultch <- &Object{Err: err}
				return
			}
			key := fmt.Sprintf("s3://%v/%v", chunk.Bucket, aws.ToString(k.Key))
			url, _ := url.New(key)
			resultch <- &Object{URL: url}
		}
		return
	}

	bucket := chunk.Bucket
	o, err := s.api.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket:       aws.String(bucket),
		Delete:       &types.Delete{Objects: chunk.Keys},
		RequestPayer: s.requestPayer(),
	})
	if err != nil {
		resultch <- &Object{Err: err}
		return
	}

	for _, d := range o.Deleted {
		key := fmt.Sprintf("s3://%v/%v", bucket, aws.ToString(d.Key))
		url, _ := url.New(key)
		url.VersionID = aws.ToString(d.VersionId)
		resultch <- &Object{URL: url}
	}

	for _, e := range o.Errors {
		key := fmt.Sprintf("s3://%v/%v", bucket, aws.ToString(e.Key))
		url, _ := url.New(key)
		url.VersionID = aws.ToString(e.VersionId)

		resultch <- &Object{
			URL: url,
			Err: fmt.Errorf("%v", aws.ToString(e.Message)),
		}
	}
}

// MultiDelete is a asynchronous removal operation for multiple objects.
// It reads given url channel, creates multiple chunks and run these
// chunks in parallel. Each chunk may have at most 1000 objects since DeleteObjects
// API has a limitation.
// See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html.
func (s *S3) MultiDelete(ctx context.Context, urlch <-chan *url.URL) <-chan *Object {
	resultch := make(chan *Object)

	go func() {
		sem := make(chan struct{}, 10)
		defer close(sem)
		defer close(resultch)

		chunks := s.calculateChunks(urlch)

		var wg sync.WaitGroup
		for chunk := range chunks {
			chunk := chunk

			wg.Add(1)
			sem <- struct{}{}

			go func() {
				defer wg.Done()
				s.doDelete(ctx, chunk, resultch)
				<-sem
			}()
		}

		wg.Wait()
	}()

	return resultch
}

// ListBuckets is a blocking list-operation which gets bucket list and returns
// the buckets that match with given prefix.
func (s *S3) ListBuckets(ctx context.Context, prefix string) ([]Bucket, error) {
	o, err := s.api.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	var buckets []Bucket
	for _, b := range o.Buckets {
		bucketName := aws.ToString(b.Name)
		if prefix == "" || strings.HasPrefix(bucketName, prefix) {
			buckets = append(buckets, Bucket{
				CreationDate: aws.ToTime(b.CreationDate),
				Name:         bucketName,
			})
		}
	}
	return buckets, nil
}

// MakeBucket creates an S3 bucket with the given name.
func (s *S3) MakeBucket(ctx context.Context, name string) error {
	if s.dryRun {
		return nil
	}

	_, err := s.api.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(name),
	})
	return err
}

// RemoveBucket removes an S3 bucket with the given name.
func (s *S3) RemoveBucket(ctx context.Context, name string) error {
	if s.dryRun {
		return nil
	}

	_, err := s.api.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(name),
	})
	return err
}

// SetBucketVersioning sets the versioning property of the bucket
func (s *S3) SetBucketVersioning(ctx context.Context, versioningStatus, bucket string) error {
	if s.dryRun {
		return nil
	}

	_, err := s.api.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatus(versioningStatus),
		},
	})
	return err
}

// GetBucketVersioning returnsversioning property of the bucket
func (s *S3) GetBucketVersioning(ctx context.Context, bucket string) (string, error) {
	output, err := s.api.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucket),
	})
	if err != nil || output.Status == "" {
		return "", err
	}

	return string(output.Status), nil
}

func (s *S3) HeadBucket(ctx context.Context, url *url.URL) error {
	_, err := s.api.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(url.Bucket),
	})
	return err
}

func (s *S3) HeadObject(ctx context.Context, url *url.URL) (*Object, *Metadata, error) {
	input := &s3.HeadObjectInput{
		Bucket:       aws.String(url.Bucket),
		Key:          aws.String(url.Path),
		RequestPayer: s.requestPayer(),
		ChecksumMode: types.ChecksumModeEnabled,
	}

	if url.VersionID != "" {
		input.VersionId = aws.String(url.VersionID)
	}

	output, err := s.api.HeadObject(ctx, input)
	if err != nil {
		if errHasCode(err, "NotFound") {
			return nil, nil, &ErrGivenObjectNotFound{ObjectAbsPath: url.Absolute()}
		}
		return nil, nil, err
	}

	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#AmazonS3-HeadObject-response-header-StorageClass
	// If the object's storage class is STANDARD, this header is not returned in the response.
	storageClassStr := "STANDARD"
	if output.StorageClass != "" {
		storageClassStr = string(output.StorageClass)
	}

	obj := &Object{
		URL:               url,
		ModTime:           output.LastModified,
		Etag:              strings.Trim(aws.ToString(output.ETag), `"`),
		ChecksumCRC32C:    aws.ToString(output.ChecksumCRC32C),
		ChecksumCRC64NVME: aws.ToString(output.ChecksumCRC64NVME),
		ChecksumSHA256:    aws.ToString(output.ChecksumSHA256),
		Size:              aws.ToInt64(output.ContentLength),
		StorageClass:      StorageClass(storageClassStr),
	}

	metadata := &Metadata{
		ContentType:      aws.ToString(output.ContentType),
		EncryptionMethod: string(output.ServerSideEncryption),
		UserDefined:      output.Metadata,
	}

	return obj, metadata, nil
}

type sdkLogger struct{}

func (l sdkLogger) Logf(classification logging.Classification, format string, v ...interface{}) {
	msg := log.TraceMessage{
		Message: fmt.Sprintf(format, v...),
	}
	log.Trace(msg)
}

// s3Session holds the resolved aws.Config together with the s3.Options
// mutators (path-style, accelerate, endpoint) that must be applied on every
// s3.Client built from it.
type s3Session struct {
	cfg      aws.Config
	s3OptFns []func(*s3.Options)
}

// SessionCache holds s3Session according to s3Opts and it synchronizes
// access/modification.
type SessionCache struct {
	sync.Mutex
	sessions map[Options]*s3Session
}

// newSession initializes a new AWS config with region fallback and custom
// options.
func (sc *SessionCache) newSession(ctx context.Context, opts Options) (*s3Session, error) {
	sc.Lock()
	defer sc.Unlock()

	if sess, ok := sc.sessions[opts]; ok {
		return sess, nil
	}

	var loadOpts []func(*config.LoadOptions) error

	useSharedConfig := true
	{
		// Reverse of what the SDK does: if AWS_SDK_LOAD_CONFIG is 0 (or a
		// falsy value) disable shared configs
		loadCfg := os.Getenv("AWS_SDK_LOAD_CONFIG")
		if loadCfg != "" {
			if enable, _ := strconv.ParseBool(loadCfg); !enable {
				useSharedConfig = false
			}
		}
	}
	if !useSharedConfig {
		loadOpts = append(loadOpts, config.WithSharedConfigFiles([]string{}))
	}

	if opts.NoSignRequest {
		// do not sign requests when making service API calls
		loadOpts = append(loadOpts, config.WithCredentialsProvider(aws.AnonymousCredentials{}))
	} else if opts.CredentialFile != "" || opts.Profile != "" {
		loadOpts = append(loadOpts,
			config.WithSharedConfigFiles([]string{}),
			config.WithSharedCredentialsFiles([]string{opts.CredentialFile}),
			config.WithSharedConfigProfile(opts.Profile),
		)
	}

	var httpClient *http.Client
	if opts.NoVerifySSL {
		httpClient = insecureHTTPClient
		loadOpts = append(loadOpts, config.WithHTTPClient(httpClient))
	}

	retryer := newCustomRetryer(opts.MaxRetries)
	loadOpts = append(loadOpts, config.WithRetryer(func() aws.Retryer { return retryer }))

	if opts.LogLevel == log.LevelTrace {
		loadOpts = append(loadOpts,
			config.WithLogger(sdkLogger{}),
			config.WithClientLogMode(aws.LogRequest|aws.LogResponse|aws.LogRetries),
		)
	}

	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, err
	}

	endpointURL, err := parseEndpoint(opts.Endpoint)
	if err != nil {
		return nil, err
	}

	// use virtual-host-style if the endpoint is known to support it,
	// otherwise use the path-style approach.
	isVirtualHostStyleEndpoint := isVirtualHostStyle(endpointURL)

	useAccelerate := supportsTransferAcceleration(endpointURL)
	// AWS SDK handles transfer acceleration automatically. Setting the
	// Endpoint to a transfer acceleration endpoint would cause bucket
	// operations fail.
	if useAccelerate {
		endpointURL = sentinelURL
	}

	usePathStyle := !isVirtualHostStyleEndpoint
	rawEndpoint := endpointURL.String()

	s3OptFns := []func(*s3.Options){
		func(o *s3.Options) {
			o.UsePathStyle = usePathStyle
			o.UseAccelerate = useAccelerate
			if endpointURL != sentinelURL {
				ep := rawEndpoint
				o.BaseEndpoint = &ep
			}
		},
	}

	// get region of the bucket and create session accordingly. if the region
	// is not provided, it means we want region-independent session
	// for operations such as listing buckets, making a new bucket etc.
	// only get bucket region when it is not specified.
	if opts.region != "" {
		cfg.Region = opts.region
	} else {
		if err := setSessionRegion(ctx, &cfg, opts.bucket, s3OptFns); err != nil {
			return nil, err
		}
	}

	sess := &s3Session{cfg: cfg, s3OptFns: s3OptFns}
	sc.sessions[opts] = sess

	return sess, nil
}

func (sc *SessionCache) clear() {
	sc.Lock()
	defer sc.Unlock()
	sc.sessions = map[Options]*s3Session{}
}

func setSessionRegion(ctx context.Context, cfg *aws.Config, bucket string, s3OptFns []func(*s3.Options)) error {
	if cfg.Region != "" {
		return nil
	}

	// set default region
	cfg.Region = "us-east-1"

	if bucket == "" {
		return nil
	}

	// auto-detection
	client := s3.NewFromConfig(*cfg, s3OptFns...)
	region, err := manager.GetBucketRegion(ctx, client, bucket)
	if err != nil {
		// manager.GetBucketRegion synthesizes its own not-found error type
		// (rather than a smithy.APIError with code "NotFound") when the
		// HeadBucket probe comes back 404, so it isn't caught by errHasCode.
		var bnf manager.BucketNotFound
		if errors.As(err, &bnf) {
			return &smithy.GenericAPIError{Code: "NotFound", Message: err.Error()}
		}
		if errHasCode(err, "NotFound") {
			return err
		}
		// don't deny any request to the service if region auto-fetching
		// receives an error. Delegate error handling to command execution.
		err = fmt.Errorf("session: fetching region failed: %v", err)
		msg := log.ErrorMessage{Err: err.Error()}
		log.Error(msg)
	} else if region != "" {
		cfg.Region = region
	}

	return nil
}

// customRetryable adds additional retryable error codes/messages that are
// not covered by the SDK's built in standard retryer, and explicitly
// disables retries for expired/invalid token errors.
type customRetryable struct{}

func (customRetryable) IsErrorRetryable(err error) aws.Ternary {
	if err == nil {
		return aws.UnknownTernary
	}

	// Errors related to tokens must never be retried.
	if errHasCode(err, "ExpiredToken") || errHasCode(err, "ExpiredTokenException") || errHasCode(err, "InvalidToken") {
		return aws.FalseTernary
	}

	shouldRetry := errHasCode(err, "InternalError") ||
		errHasCode(err, "RequestTimeTooSkewed") ||
		errHasCode(err, "SlowDown") ||
		strings.Contains(err.Error(), "connection reset") ||
		strings.Contains(err.Error(), "connection timed out")

	if shouldRetry {
		msg := log.DebugMessage{Err: fmt.Sprintf("retryable error: %v", err)}
		log.Debug(msg)
		return aws.TrueTernary
	}

	return aws.UnknownTernary
}

// newCustomRetryer wraps the SDK's built in standard retryer, adding
// additional error codes/messages such as, retry for S3 InternalError code.
func newCustomRetryer(maxRetries int) aws.Retryer {
	return retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = maxRetries + 1
		o.Retryables = append([]retry.IsErrorRetryable{customRetryable{}}, o.Retryables...)
	})
}

var insecureHTTPClient = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		Proxy:           http.ProxyFromEnvironment,
	},
}

func supportsTransferAcceleration(endpoint urlpkg.URL) bool {
	return endpoint.Hostname() == transferAccelEndpoint
}

func IsGoogleEndpoint(endpoint urlpkg.URL) bool {
	return endpoint.Hostname() == gcsEndpoint
}

// isVirtualHostStyle reports whether the given endpoint supports S3 virtual
// host style bucket name resolving. If a custom S3 API compatible endpoint is
// given, resolve the bucketname from the URL path.
func isVirtualHostStyle(endpoint urlpkg.URL) bool {
	return endpoint == sentinelURL || supportsTransferAcceleration(endpoint) || IsGoogleEndpoint(endpoint)
}

// errHasCode reports whether err (or an error it wraps) is an API error with
// the given error code. It also recognizes the synthesized codes ("NotFound",
// "AccessDenied") that the SDK derives from bare HTTP status codes when the
// service does not return a structured error body (e.g. HeadObject/HeadBucket
// 404s).
func errHasCode(err error, code string) bool {
	if err == nil || code == "" {
		return false
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode() == code {
			return true
		}
	}

	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		switch re.HTTPStatusCode() {
		case http.StatusNotFound:
			if code == "NotFound" {
				return true
			}
		case http.StatusForbidden:
			if code == "AccessDenied" || code == "Forbidden" {
				return true
			}
		}
	}

	return false
}

// ErrHasCode reports whether err is an API error whose code matches any of
// the given codes. It is exported so that callers outside this package (e.g.
// command.Sync) don't need to import the AWS SDK directly to make retry/stop
// decisions based on S3 error codes.
func ErrHasCode(err error, codes ...string) bool {
	for _, code := range codes {
		if errHasCode(err, code) {
			return true
		}
	}
	return false
}

// IsCancelationError reports whether given error is a storage related
// cancelation error.
func IsCancelationError(err error) bool {
	return errors.Is(err, context.Canceled)
}

// generate a retry ID for this upload attempt
func generateRetryID() *string {
	num, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	return aws.String(num.String())
}

// EventStreamDecoder decodes a s3.Event with
// the given decoder.
type EventStreamDecoder interface {
	Decode() ([]byte, error)
}

type JSONDecoder struct {
	decoder *json.Decoder
}

func NewJSONDecoder(reader io.Reader) EventStreamDecoder {
	return &JSONDecoder{
		decoder: json.NewDecoder(reader),
	}
}

func (jd *JSONDecoder) Decode() ([]byte, error) {
	var val json.RawMessage
	err := jd.decoder.Decode(&val)
	if err != nil {
		return nil, err
	}
	return val, nil
}

type CsvDecoder struct {
	decoder   *csv.Reader
	delimiter string
}

func NewCsvDecoder(reader io.Reader) EventStreamDecoder {
	csvDecoder := &CsvDecoder{
		decoder:   csv.NewReader(reader),
		delimiter: ",",
	}
	// returned values from AWS has double quotes in it
	// so we enable lazy quotes
	csvDecoder.decoder.LazyQuotes = true
	return csvDecoder
}

func (cd *CsvDecoder) Decode() ([]byte, error) {
	res, err := cd.decoder.Read()
	if err != nil {
		return nil, err
	}

	result := []byte{}
	for i, str := range res {
		if i != len(res)-1 {
			str = fmt.Sprintf("%s%s", str, cd.delimiter)
		}
		result = append(result, []byte(str)...)
	}
	return result, nil
}
