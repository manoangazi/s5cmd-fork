package storage

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	urlpkg "net/url"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
	"github.com/google/go-cmp/cmp"
	"gotest.tools/v3/assert"

	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/storage/url"
)

func TestS3ImplementsStorageInterface(t *testing.T) {
	var i interface{} = new(S3)
	if _, ok := i.(Storage); !ok {
		t.Errorf("expected %t to implement Storage interface", i)
	}
}

// newTestClient builds an *s3.Client pointed at the given httptest.Server
// with anonymous credentials and path-style addressing, bypassing
// globalSessionCache. Useful for tests that need full control over the wire
// response.
func newTestClient(t *testing.T, ts *httptest.Server) *s3.Client {
	t.Helper()
	return s3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: aws.AnonymousCredentials{},
	}, func(o *s3.Options) {
		o.UsePathStyle = true
		ep := ts.URL
		o.BaseEndpoint = &ep
		o.Retryer = aws.NopRetryer{}
	})
}

func TestNewSessionPathStyle(t *testing.T) {
	testcases := []struct {
		name            string
		endpoint        urlpkg.URL
		expectPathStyle bool
	}{
		{
			name:            "expect_virtual_host_style_when_missing_endpoint",
			endpoint:        urlpkg.URL{},
			expectPathStyle: false,
		},
		{
			name:            "expect_virtual_host_style_for_transfer_accel",
			endpoint:        urlpkg.URL{Scheme: "https", Host: transferAccelEndpoint},
			expectPathStyle: false,
		},
		{
			name:            "expect_virtual_host_style_for_google_cloud_storage",
			endpoint:        urlpkg.URL{Scheme: "https", Host: gcsEndpoint},
			expectPathStyle: false,
		},
		{
			name:            "expect_path_style_for_localhost",
			endpoint:        urlpkg.URL{Scheme: "http", Host: "127.0.0.1"},
			expectPathStyle: true,
		},
		{
			name:            "expect_path_style_for_secure_localhost",
			endpoint:        urlpkg.URL{Scheme: "https", Host: "127.0.0.1"},
			expectPathStyle: true,
		},
		{
			name:            "expect_path_style_for_custom_endpoint",
			endpoint:        urlpkg.URL{Scheme: "https", Host: "example.com"},
			expectPathStyle: true,
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			globalSessionCache.clear()
			opts := Options{Endpoint: tc.endpoint.String(), NoSignRequest: true}
			sess, err := globalSessionCache.newSession(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}

			client := s3.NewFromConfig(sess.cfg, sess.s3OptFns...)
			got := client.Options().UsePathStyle
			if got != tc.expectPathStyle {
				t.Fatalf("expected: %v, got: %v", tc.expectPathStyle, got)
			}
		})
	}
}

func TestNewSessionWithRegionSetViaEnv(t *testing.T) {
	globalSessionCache.clear()

	const expectedRegion = "us-west-2"

	os.Setenv("AWS_REGION", expectedRegion)
	defer os.Unsetenv("AWS_REGION")

	sess, err := globalSessionCache.newSession(context.Background(), Options{NoSignRequest: true})
	if err != nil {
		t.Fatal(err)
	}

	got := sess.cfg.Region
	if got != expectedRegion {
		t.Fatalf("expected %v, got %v", expectedRegion, got)
	}
}

func TestNewSessionWithNoSignRequest(t *testing.T) {
	globalSessionCache.clear()

	sess, err := globalSessionCache.newSession(context.Background(), Options{
		NoSignRequest: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if !aws.IsCredentialsProvider(sess.cfg.Credentials, aws.AnonymousCredentials{}) {
		t.Fatalf("expected anonymous credentials, got %+v", sess.cfg.Credentials)
	}
}

func TestNewSessionWithProfileFromFile(t *testing.T) {
	// create a temporary credentials file
	file, err := os.CreateTemp("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())

	profiles := `[default]
aws_access_key_id = default_profile_key_id
aws_secret_access_key = default_profile_access_key

[p1]
aws_access_key_id = p1_profile_key_id
aws_secret_access_key = p1_profile_access_key

[p2]
aws_access_key_id = p2_profile_key_id
aws_secret_access_key = p2_profile_access_key`

	_, err = file.Write([]byte(profiles))
	if err != nil {
		t.Fatal(err)
	}

	testcases := []struct {
		name               string
		fileName           string
		profileName        string
		expAccessKeyID     string
		expSecretAccessKey string
	}{
		{
			name:               "use default profile",
			fileName:           file.Name(),
			profileName:        "",
			expAccessKeyID:     "default_profile_key_id",
			expSecretAccessKey: "default_profile_access_key",
		},
		{
			name:               "use a non-default profile",
			fileName:           file.Name(),
			profileName:        "p1",
			expAccessKeyID:     "p1_profile_key_id",
			expSecretAccessKey: "p1_profile_access_key",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			globalSessionCache.clear()
			sess, err := globalSessionCache.newSession(context.Background(), Options{
				Profile:        tc.profileName,
				CredentialFile: tc.fileName,
			})
			if err != nil {
				t.Fatal(err)
			}

			got, err := sess.cfg.Credentials.Retrieve(context.Background())
			if err != nil {
				t.Fatal(err)
			}

			if got.AccessKeyID != tc.expAccessKeyID || got.SecretAccessKey != tc.expSecretAccessKey {
				t.Errorf("Expected credentials does not match the credential we got!\nExpected: Access Key ID: %v, Secret Access Key: %v\nGot    : Access Key ID: %v, Secret Access Key: %v\n", tc.expAccessKeyID, tc.expSecretAccessKey, got.AccessKeyID, got.SecretAccessKey)
			}
		})
	}
}

func TestNewSessionWithNonExistentProfile(t *testing.T) {
	file, err := os.CreateTemp("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())

	_, err = file.Write([]byte("[default]\naws_access_key_id = a\naws_secret_access_key = b\n"))
	if err != nil {
		t.Fatal(err)
	}

	globalSessionCache.clear()
	_, err = globalSessionCache.newSession(context.Background(), Options{
		Profile:        "non-existent-profile",
		CredentialFile: file.Name(),
	})
	if err == nil {
		t.Fatalf("expected an error loading a non-existent profile")
	}
}

// xmlListObjectsV2Response renders a minimal ListObjectsV2 XML response.
func xmlListObjectsV2Response(prefixes, keys []string) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>`)
	for _, p := range prefixes {
		fmt.Fprintf(&b, "<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>", p)
	}
	for _, k := range keys {
		fmt.Fprintf(&b, "<Contents><Key>%s</Key><LastModified>2021-01-01T00:00:00.000Z</LastModified><Size>0</Size></Contents>", k)
	}
	b.WriteString(`<IsTruncated>false</IsTruncated></ListBucketResult>`)
	return b.String()
}

func TestS3ListURL(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, xmlListObjectsV2Response(
			[]string{"key/a/", "key/b/"},
			[]string{"key/test.txt", "key/test.pdf"},
		))
	}))
	defer ts.Close()

	mockS3 := &S3{api: newTestClient(t, ts)}

	responses := []struct {
		isDir  bool
		url    string
		relurl string
	}{
		{isDir: true, url: "s3://bucket/key/a/", relurl: "a/"},
		{isDir: true, url: "s3://bucket/key/b/", relurl: "b/"},
		{isDir: false, url: "s3://bucket/key/test.txt", relurl: "test.txt"},
		{isDir: false, url: "s3://bucket/key/test.pdf", relurl: "test.pdf"},
	}

	index := 0
	for got := range mockS3.List(context.Background(), url, true) {
		if got.Err != nil {
			t.Errorf("unexpected error: %v", got.Err)
			continue
		}

		want := responses[index]
		if diff := cmp.Diff(want.isDir, got.Type.IsDir()); diff != "" {
			t.Errorf("(-want +got):\n%v", diff)
		}
		if diff := cmp.Diff(want.url, got.URL.Absolute()); diff != "" {
			t.Errorf("(-want +got):\n%v", diff)
		}
		if diff := cmp.Diff(want.relurl, got.URL.Relative()); diff != "" {
			t.Errorf("(-want +got):\n%v", diff)
		}
		index++
	}
}

func TestS3ListError(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code><Message>mock error</Message></Error>`)
	}))
	defer ts.Close()

	mockS3 := &S3{api: newTestClient(t, ts)}

	for got := range mockS3.List(context.Background(), url, true) {
		if got.Err == nil {
			t.Errorf("expected an error, got nil")
		}
	}
}

func TestS3ListNoItemFound(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		// output does not include keys that match with given key
		fmt.Fprint(w, xmlListObjectsV2Response(
			[]string{"anotherkey/a/", "anotherkey/b/"},
			[]string{"a/b/c/d/test.txt", "unknown/test.pdf"},
		))
	}))
	defer ts.Close()

	mockS3 := &S3{api: newTestClient(t, ts)}

	for got := range mockS3.List(context.Background(), url, true) {
		if got.Err != ErrNoObjectFound {
			t.Errorf("error got = %v, want %v", got.Err, ErrNoObjectFound)
		}
	}
}

func TestS3ListContextCancelled(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, xmlListObjectsV2Response([]string{"key/a/"}, nil))
	}))
	defer ts.Close()

	mockS3 := &S3{api: newTestClient(t, ts)}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for got := range mockS3.List(ctx, url, true) {
		if got.Err == nil {
			t.Errorf("expected a cancelation error, got nil")
			continue
		}
		if !IsCancelationError(got.Err) {
			t.Errorf("error got = %v, want a cancelation error", got.Err)
		}
	}
}

// TestS3RetryDecision verifies the additional retry codes/messages the fork
// layers on top of the SDK's default standard retryer, and that token-related
// errors are never retried, matching the behavior the previous v1
// customRetryer enforced.
func TestS3RetryDecision(t *testing.T) {
	log.Init("debug", false)

	testcases := []struct {
		name          string
		err           error
		expectedRetry bool
	}{
		{name: "InternalError", err: &smithy.GenericAPIError{Code: "InternalError"}, expectedRetry: true},
		{name: "RequestTimeTooSkewed", err: &smithy.GenericAPIError{Code: "RequestTimeTooSkewed"}, expectedRetry: true},
		{name: "SlowDown", err: &smithy.GenericAPIError{Code: "SlowDown"}, expectedRetry: true},
		{name: "ConnectionReset", err: fmt.Errorf("connection reset by peer"), expectedRetry: true},
		{name: "ConnectionTimedOut", err: fmt.Errorf("read: connection timed out"), expectedRetry: true},

		// codes covered by the SDK's own default retryables
		{name: "ThrottlingException", err: &smithy.GenericAPIError{Code: "ThrottlingException"}, expectedRetry: true},
		{name: "RequestLimitExceeded", err: &smithy.GenericAPIError{Code: "RequestLimitExceeded"}, expectedRetry: true},

		// token errors must never be retried, even though the underlying
		// error type otherwise looks like an API error.
		{name: "ExpiredToken", err: &smithy.GenericAPIError{Code: "ExpiredToken"}, expectedRetry: false},
		{name: "ExpiredTokenException", err: &smithy.GenericAPIError{Code: "ExpiredTokenException"}, expectedRetry: false},
		{name: "InvalidToken", err: &smithy.GenericAPIError{Code: "InvalidToken"}, expectedRetry: false},
	}

	retryer := newCustomRetryer(5)
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := retryer.IsErrorRetryable(tc.err)
			if got != tc.expectedRetry {
				t.Errorf("expected retry=%v, got %v", tc.expectedRetry, got)
			}
		})
	}
}

func TestS3RetryMaxAttempts(t *testing.T) {
	const maxRetries = 5
	retryer := newCustomRetryer(maxRetries)
	// v1's NumMaxRetries/opts.MaxRetries counted retries (excluding the
	// initial attempt); v2's MaxAttempts counts the total, so it must be
	// maxRetries+1.
	if got, want := retryer.MaxAttempts(), maxRetries+1; got != want {
		t.Errorf("expected MaxAttempts=%v, got %v", want, got)
	}
}

func TestS3RetryOnNoSuchUpload(t *testing.T) {
	log.Init("debug", false)

	testcases := []struct {
		name       string
		retryCount int
	}{
		{name: "Don't retry", retryCount: 0},
		{name: "Retry 5 times on NoSuchUpload error", retryCount: 5},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			u, err := url.New("s3://bucket/key")
			if err != nil {
				t.Fatal(err)
			}

			var putCount, headCount int
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodPut:
					putCount++
					w.WriteHeader(http.StatusNotFound)
					fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchUpload</Code><Message>no such upload</Message></Error>`)
				case http.MethodHead:
					headCount++
					// retry ID never matches, forcing the retry loop to run
					// its full course.
					w.Header().Set("x-amz-meta-s5cmd-upload-retry-id", "never-matches")
					w.WriteHeader(http.StatusOK)
				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer ts.Close()

			client := newTestClient(t, ts)
			mockS3 := &S3{
				api:                    client,
				uploader:               manager.NewUploader(client),
				noSuchUploadRetryCount: tc.retryCount,
			}

			ctx := context.Background()
			_ = mockS3.Put(ctx, strings.NewReader(""), u, Metadata{}, 1, 5*1024*1024)

			// +1 is for the original request
			// *1 is to account for the "Stat" (HeadObject) requests made to
			// obtain the retry code from object metadata for each retry.
			wantPut := tc.retryCount + 1
			wantHead := tc.retryCount
			if putCount != wantPut {
				t.Errorf("expected PUT request count %d, got %d", wantPut, putCount)
			}
			if headCount != wantHead {
				t.Errorf("expected HEAD request count %d, got %d", wantHead, headCount)
			}
		})
	}
}

func TestS3CopyEncryptionRequest(t *testing.T) {
	testcases := []struct {
		name     string
		sse      string
		sseKeyID string
		acl      string

		expectedSSE      string
		expectedSSEKeyID string
		expectedACL      string
	}{
		{name: "no encryption/no acl, by default"},
		{
			name:        "aws:kms encryption with server side generated keys",
			sse:         "aws:kms",
			expectedSSE: "aws:kms",
		},
		{
			name:     "aws:kms encryption with user provided key",
			sse:      "aws:kms",
			sseKeyID: "sdkjn12SDdci#@#EFRFERTqW/ke",

			expectedSSE:      "aws:kms",
			expectedSSEKeyID: "sdkjn12SDdci#@#EFRFERTqW/ke",
		},
		{
			name:     "provide key without encryption flag, shall be ignored",
			sseKeyID: "1234567890",
		},
		{
			name:        "acl flag with a value",
			acl:         "bucket-owner-full-control",
			expectedACL: "bucket-owner-full-control",
		},
	}

	u, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var gotSSE, gotSSEKeyID, gotACL string
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotSSE = r.Header.Get("x-amz-server-side-encryption")
				gotSSEKeyID = r.Header.Get("x-amz-server-side-encryption-aws-kms-key-id")
				gotACL = r.Header.Get("x-amz-acl")
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><CopyObjectResult></CopyObjectResult>`)
			}))
			defer ts.Close()

			mockS3 := &S3{api: newTestClient(t, ts)}

			metadata := Metadata{}
			metadata.EncryptionMethod = tc.sse
			metadata.EncryptionKeyID = tc.sseKeyID
			metadata.ACL = tc.acl

			err = mockS3.Copy(context.Background(), u, u, metadata)
			if err != nil {
				t.Errorf("Expected %v, but received %q", nil, err)
			}

			assert.Equal(t, gotSSE, tc.expectedSSE)
			assert.Equal(t, gotSSEKeyID, tc.expectedSSEKeyID)
			assert.Equal(t, gotACL, tc.expectedACL)
		})
	}
}

func TestS3PutEncryptionRequest(t *testing.T) {
	testcases := []struct {
		name     string
		sse      string
		sseKeyID string
		acl      string

		expectedSSE      string
		expectedSSEKeyID string
		expectedACL      string
	}{
		{name: "no encryption, no acl flag"},
		{
			name:        "aws:kms encryption with server side generated keys",
			sse:         "aws:kms",
			expectedSSE: "aws:kms",
		},
		{
			name:     "aws:kms encryption with user provided key",
			sse:      "aws:kms",
			sseKeyID: "sdkjn12SDdci#@#EFRFERTqW/ke",

			expectedSSE:      "aws:kms",
			expectedSSEKeyID: "sdkjn12SDdci#@#EFRFERTqW/ke",
		},
		{
			name:     "provide key without encryption flag, shall be ignored",
			sseKeyID: "1234567890",
		},
		{
			name:        "acl flag with a value",
			acl:         "bucket-owner-full-control",
			expectedACL: "bucket-owner-full-control",
		},
	}
	u, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var gotSSE, gotSSEKeyID, gotACL string
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotSSE = r.Header.Get("x-amz-server-side-encryption")
				gotSSEKeyID = r.Header.Get("x-amz-server-side-encryption-aws-kms-key-id")
				gotACL = r.Header.Get("x-amz-acl")
				w.Header().Set("x-amz-checksum-sha256", "deadbeef")
				w.WriteHeader(http.StatusOK)
			}))
			defer ts.Close()

			client := newTestClient(t, ts)
			mockS3 := &S3{
				uploader: manager.NewUploader(client),
			}

			metadata := Metadata{}
			metadata.EncryptionMethod = tc.sse
			metadata.EncryptionKeyID = tc.sseKeyID
			metadata.ACL = tc.acl

			err = mockS3.Put(context.Background(), strings.NewReader(""), u, metadata, 1, 5242880)
			if err != nil {
				t.Errorf("Expected %v, but received %q", nil, err)
			}

			assert.Equal(t, gotSSE, tc.expectedSSE)
			assert.Equal(t, gotSSEKeyID, tc.expectedSSEKeyID)
			assert.Equal(t, gotACL, tc.expectedACL)
		})
	}
}

func TestS3listObjectsV2(t *testing.T) {
	const (
		numObjectsToReturn = 1010
		numObjectsToIgnore = 113

		pre = "s3://bucket/key"
	)

	u, err := url.New(pre)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mapReturnObjNameToModtime := map[string]bool{}

	var keys []string
	for i := 0; i < numObjectsToReturn; i++ {
		fname := fmt.Sprintf("%s/%d", pre, i)
		mapReturnObjNameToModtime[pre+"/"+fname] = true
		keys = append(keys, fmt.Sprintf("key/%s", fname))
	}

	var futureKeys []string
	for i := 0; i < numObjectsToIgnore; i++ {
		fname := fmt.Sprintf("%s/%d", pre, numObjectsToReturn+i)
		futureKeys = append(futureKeys, fmt.Sprintf("key/%s", fname))
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b strings.Builder
		b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>`)
		for _, k := range keys {
			fmt.Fprintf(&b, "<Contents><Key>%s</Key><LastModified>2000-01-01T00:00:00.000Z</LastModified><Size>0</Size></Contents>", k)
		}
		for _, k := range futureKeys {
			fmt.Fprintf(&b, "<Contents><Key>%s</Key><LastModified>2999-01-01T00:00:00.000Z</LastModified><Size>0</Size></Contents>", k)
		}
		b.WriteString(`<IsTruncated>false</IsTruncated></ListBucketResult>`)
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, b.String())
	}))
	defer ts.Close()

	mockS3 := &S3{api: newTestClient(t, ts)}

	outputCh := mockS3.listObjectsV2(context.Background(), u)

	// skipFutureObjects defaults to off in this fork, so every object
	// (including the "future" ones) should be returned.
	total := 0
	for obj := range outputCh {
		if obj.Err != nil {
			t.Fatalf("unexpected error: %v", obj.Err)
		}
		total++
	}

	want := numObjectsToReturn + numObjectsToIgnore
	assert.Equal(t, total, want)
}

func TestSessionCreateAndCachingWithDifferentBuckets(t *testing.T) {
	log.Init("error", false)
	globalSessionCache.clear()

	testcases := []struct {
		bucket         string
		alreadyCreated bool // sessions should not be created again if they already have been created before
	}{
		{bucket: "bucket"},
		{bucket: "bucket", alreadyCreated: true},
		{bucket: "test-bucket"},
	}

	seen := map[string]*s3Session{}

	for _, tc := range testcases {
		sess, err := globalSessionCache.newSession(context.Background(), Options{
			bucket:        tc.bucket,
			NoSignRequest: true,
		})
		if err != nil {
			t.Error(err)
		}

		if tc.alreadyCreated {
			prev, ok := seen[tc.bucket]
			assert.Check(t, ok, "session should not have been created again")
			assert.Check(t, prev == sess, "expected the exact same cached session")
		} else {
			seen[tc.bucket] = sess
		}
	}
}

func TestSessionRegionDetection(t *testing.T) {
	bucketRegion := "sa-east-1"

	testcases := []struct {
		name           string
		bucket         string
		optsRegion     string
		envRegion      string
		expectedRegion string
	}{
		{
			name:           "RegionWithSourceRegionParameter",
			bucket:         "bucket",
			optsRegion:     "ap-east-1",
			envRegion:      "ca-central-1",
			expectedRegion: "ap-east-1",
		},
		{
			name:           "RegionWithEnvironmentVariable",
			bucket:         "bucket",
			optsRegion:     "",
			envRegion:      "ca-central-1",
			expectedRegion: "ca-central-1",
		},
		{
			name:           "RegionWithBucketRegion",
			bucket:         "bucket",
			optsRegion:     "",
			envRegion:      "",
			expectedRegion: bucketRegion,
		},
		{
			name:           "DefaultRegion",
			bucket:         "",
			optsRegion:     "",
			envRegion:      "",
			expectedRegion: "us-east-1",
		},
	}

	// ignore local profile loading
	os.Setenv("AWS_SDK_LOAD_CONFIG", "0")
	defer os.Unsetenv("AWS_SDK_LOAD_CONFIG")

	// mock auto bucket detection
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Amz-Bucket-Region", bucketRegion)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{
				Endpoint: server.URL,

				// since profile loading disabled above, we need to provide
				// credentials to the session. NoSignRequest could be used
				// for anonymous credentials.
				NoSignRequest: true,
			}

			if tc.optsRegion != "" {
				opts.region = tc.optsRegion
			}

			if tc.envRegion != "" {
				os.Setenv("AWS_REGION", tc.envRegion)
				defer os.Unsetenv("AWS_REGION")
			}

			if tc.bucket != "" {
				opts.bucket = tc.bucket
			}

			globalSessionCache.clear()

			sess, err := globalSessionCache.newSession(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}

			got := sess.cfg.Region
			if got != tc.expectedRegion {
				t.Fatalf("expected %v, got %v", tc.expectedRegion, got)
			}
		})
	}
}

func TestSessionAutoRegion(t *testing.T) {
	log.Init("error", false)

	testcases := []struct {
		name              string
		bucket            string
		region            string
		status            int
		expectedRegion    string
		expectedErrorCode string
	}{
		{
			name:           "NoLocationConstraint",
			bucket:         "bucket",
			region:         "",
			status:         http.StatusOK,
			expectedRegion: "us-east-1",
		},
		{
			name:           "LocationConstraintDefaultRegion",
			bucket:         "bucket",
			region:         "us-east-1",
			status:         http.StatusOK,
			expectedRegion: "us-east-1",
		},
		{
			name:           "LocationConstraintAnotherRegion",
			bucket:         "bucket",
			region:         "us-west-2",
			status:         http.StatusOK,
			expectedRegion: "us-west-2",
		},
		{
			name:              "BucketNotFoundErrorMustFail",
			bucket:            "bucket",
			status:            http.StatusNotFound,
			expectedRegion:    "us-east-1",
			expectedErrorCode: "NotFound",
		},
		{
			name:           "AccessDeniedErrorMustNotFail",
			bucket:         "bucket",
			status:         http.StatusForbidden,
			expectedRegion: "us-east-1",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.region != "" {
					w.Header().Set("X-Amz-Bucket-Region", tc.region)
				}
				w.WriteHeader(tc.status)
			}))
			defer ts.Close()

			cfg := aws.Config{
				Region:      "",
				Credentials: aws.AnonymousCredentials{},
			}
			s3OptFns := []func(*s3.Options){
				func(o *s3.Options) {
					o.UsePathStyle = true
					ep := ts.URL
					o.BaseEndpoint = &ep
					o.Retryer = aws.NopRetryer{}
				},
			}

			err := setSessionRegion(context.Background(), &cfg, tc.bucket, s3OptFns)
			if tc.expectedErrorCode != "" && !errHasCode(err, tc.expectedErrorCode) {
				t.Errorf("expected error code: %v, got error: %v", tc.expectedErrorCode, err)
				return
			}

			if expected, got := tc.expectedRegion, cfg.Region; expected != got {
				t.Errorf("expected: %v, got: %v", expected, got)
			}
		})
	}
}

func TestS3ListObjectsAPIVersions(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	var gotQuery string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
	}))
	defer ts.Close()

	mockS3 := &S3{api: newTestClient(t, ts)}

	t.Run("list-objects-v2", func(t *testing.T) {
		mockS3.useListObjectsV1 = false
		for range mockS3.List(context.Background(), url, false) {
		}
		if !strings.Contains(gotQuery, "list-type=2") {
			t.Errorf("expected a ListObjectsV2 request (list-type=2), got query: %v", gotQuery)
		}
	})

	t.Run("list-objects-v1", func(t *testing.T) {
		mockS3.useListObjectsV1 = true
		for range mockS3.List(context.Background(), url, false) {
		}
		if strings.Contains(gotQuery, "list-type=2") {
			t.Errorf("expected a legacy ListObjects request, got query: %v", gotQuery)
		}
	})
}

func TestS3HeadObject(t *testing.T) {
	testcases := []struct {
		name     string
		url      string
		expected string
	}{
		{
			name:     "HeadObject",
			url:      "s3://bucket/key",
			expected: "bucket/key",
		},
		{
			name:     "HeadObject with different URL",
			url:      "s3://another-bucket/another-key",
			expected: "another-bucket/another-key",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := url.New(tc.url)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("x-amz-checksum-crc32c", "deadbeef")
				w.WriteHeader(http.StatusOK)
			}))
			defer ts.Close()

			mockS3 := &S3{api: newTestClient(t, ts)}

			obj, _, err := mockS3.HeadObject(context.Background(), u)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if obj.ChecksumCRC32C != "deadbeef" {
				t.Errorf("expected checksum to be surfaced from HeadObject, got %q", obj.ChecksumCRC32C)
			}
		})
	}
}

func TestS3PutRequestsCRC32CChecksum(t *testing.T) {
	u, err := url.New("s3://bucket/key")
	if err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("x-amz-sdk-checksum-algorithm"); got != "CRC32C" && r.Method == http.MethodPut {
			t.Errorf("expected PutObject to request a CRC32C checksum, got algorithm header %q", got)
		}
		w.Header().Set("x-amz-checksum-crc32c", "abc123==")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	client := newTestClient(t, ts)
	mockS3 := &S3{uploader: manager.NewUploader(client)}

	if err := mockS3.Put(context.Background(), strings.NewReader("hello world"), u, Metadata{}, 1, 5*1024*1024); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestErrHasCode(t *testing.T) {
	notFound := &smithy.GenericAPIError{Code: "NotFound"}
	if !errHasCode(notFound, "NotFound") {
		t.Errorf("expected errHasCode to match on APIError code")
	}
	if errHasCode(notFound, "AccessDenied") {
		t.Errorf("expected errHasCode to not match a different code")
	}
	if errHasCode(nil, "NotFound") {
		t.Errorf("expected errHasCode(nil, ...) to be false")
	}
	if !ErrHasCode(notFound, "AccessDenied", "NotFound") {
		t.Errorf("expected ErrHasCode to match if any of the given codes match")
	}
}

func TestIsCancelationError(t *testing.T) {
	if !IsCancelationError(context.Canceled) {
		t.Errorf("expected context.Canceled to be a cancelation error")
	}
	if !IsCancelationError(fmt.Errorf("wrapped: %w", context.Canceled)) {
		t.Errorf("expected a wrapped context.Canceled to be a cancelation error")
	}
	if IsCancelationError(fmt.Errorf("some other error")) {
		t.Errorf("expected an unrelated error to not be a cancelation error")
	}
}
