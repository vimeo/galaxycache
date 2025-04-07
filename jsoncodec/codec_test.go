/*
Copyright 2025 Vimeo Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package jsoncodec_test

import (
	"testing"

	"github.com/vimeo/galaxycache/jsoncodec"
)

func TestJSONCodec(t *testing.T) {
	inProtoCodec := jsoncodec.Codec[testMessage]{}
	inProtoCodec.Set(testMessage{
		Name: "TestName",
		City: "TestCity",
	})

	testMsgBytes, err := inProtoCodec.MarshalBinary()
	if err != nil {
		t.Errorf("Error marshaling from protoCodec: %s", err)
	}
	t.Logf("Marshaled Bytes: %q", string(testMsgBytes))

	outProtoCodec := jsoncodec.Codec[testMessage]{}

	if unmarshalErr := outProtoCodec.UnmarshalBinary(testMsgBytes); unmarshalErr != nil {
		t.Errorf("Error unmarshaling: %s", unmarshalErr)
	}

	if *outProtoCodec.Get() != *inProtoCodec.Get() {
		t.Errorf("UnmarshalBinary resulted in %+v; want %+v", *outProtoCodec.Get(), *inProtoCodec.Get())
	}
}
