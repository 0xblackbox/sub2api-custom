package middleware

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsAsyncImageTaskRead(t *testing.T) {
	require.True(t, isAsyncImageTaskRead(http.MethodGet, "/v1/images/tasks/imgtask_123"))
	require.True(t, isAsyncImageTaskRead(http.MethodGet, "/images/tasks/imgtask_123"))
	require.False(t, isAsyncImageTaskRead(http.MethodPost, "/v1/images/tasks/imgtask_123"))
	require.False(t, isAsyncImageTaskRead(http.MethodGet, "/v1/images/generations"))
}

func TestIsBackgroundResponseRead(t *testing.T) {
	require.True(t, isBackgroundResponseRead(http.MethodGet, "/v1/responses/resp_bg_123"))
	require.True(t, isBackgroundResponseRead(http.MethodGet, "/responses/resp_bg_123"))
	require.False(t, isBackgroundResponseRead(http.MethodPost, "/v1/responses/resp_bg_123"))
	require.False(t, isBackgroundResponseRead(http.MethodGet, "/v1/responses"))
	// Similar prefixes must not accidentally bypass billing enforcement.
	require.False(t, isBackgroundResponseRead(http.MethodGet, "/v1/responses-archive/resp_bg_123"))
}
