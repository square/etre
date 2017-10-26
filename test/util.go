// Copyright 2017, Square, Inc.

// Package test provides helper functions for tests.
package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

// MakeHTTPRequest is a helper function for making an http request. The
// response body of the http request is unmarshalled into the struct pointed to
// by the respStruct argument (if it's not nil). The status code of the
// response is returned.
func MakeHTTPRequest(httpVerb, url string, payload []byte, respStruct interface{}) (int, error) {
	var statusCode int

	// Make the http request.
	req, err := http.NewRequest(httpVerb, url, bytes.NewReader(payload))
	if err != nil {
		return statusCode, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := (http.DefaultClient).Do(req)
	if err != nil {
		return statusCode, err
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)

	// Decode response into respSruct
	if respStruct != nil && len(body) > 0 {
		if err := json.Unmarshal(body, &respStruct); err != nil {
			return statusCode, fmt.Errorf("Can't decode response body: %s: %s", err, string(body))
		}
	}

	statusCode = res.StatusCode

	return statusCode, nil
}
