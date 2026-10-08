/*
Copyright 2026 The llm-d Authors.

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

package preciseprefixcache

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/sets"

	datagraph "github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
	"github.com/llm-d/llm-d-router/pkg/kvcache"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

func TestAdmissionPreparationDefersStateAndReusesMatchingSnapshot(t *testing.T) {
	queries := 0
	match := 1
	indexer := &fakeKVCacheIndexer{
		computeFromTokens: func(context.Context, []uint32, string, []*kvblock.BlockExtraFeatures) ([]kvblock.BlockHash, error) {
			return []kvblock.BlockHash{123}, nil
		},
		matchBlockKeys: func(context.Context, []kvblock.BlockHash, sets.Set[string]) (map[string]kvcache.PodMatch, error) {
			queries++
			return map[string]kvcache.PodMatch{"10.0.0.1:8080": {WeightedScore: float64(match), MatchedBlocks: match}}, nil
		},
	}
	p := newProducerWithIndexer(t.Context(), indexer)
	p.speculativeEnabled = true
	request := &scheduling.InferenceRequest{RequestID: "queued", Body: &fwkrh.InferenceRequestBody{
		TokenizedRequest: fwkrh.NewTokenizedRequest([][]uint32{make([]uint32, testBlockSize)}),
	}}
	endpoints := freshEndpoints()
	require.NoError(t, p.PrepareForAdmission(t.Context(), request, endpoints))
	_, err := plugin.ReadPluginStateKey[*blockKeysState](p.pluginState, request.RequestID, blockKeysStateKey)
	require.Error(t, err)
	info, _ := endpoints[0].Get(p.dk)
	require.Equal(t, 1, info.(*attrprefix.PrefixCacheMatchInfo).MatchBlocks())

	match = 0
	require.NoError(t, p.PrepareForAdmission(t.Context(), request, endpoints))
	require.Equal(t, 2, queries)
	_, err = plugin.ReadPluginStateKey[*blockKeysState](p.pluginState, request.RequestID, blockKeysStateKey)
	require.Error(t, err)
	match = 1
	datagraph.RegisterScopeSpecs([]plugin.Plugin{p})
	scoped, violations := datagraph.ScopeRequest(logr.Discard(), requestcontrol.DataProducerExtensionPoint, p, request)
	require.NoError(t, p.Produce(t.Context(), scoped, endpoints))
	require.NoError(t, violations.Write())
	require.Equal(t, 2, queries)
	info, _ = endpoints[0].Get(p.dk)
	require.Zero(t, info.(*attrprefix.PrefixCacheMatchInfo).MatchBlocks())
	state, err := plugin.ReadPluginStateKey[*blockKeysState](p.pluginState, request.RequestID, blockKeysStateKey)
	require.NoError(t, err)
	require.Equal(t, [][]kvblock.BlockHash{{123}}, state.perPromptKeys)

	prepared, ok := scheduling.ReadRequestAttribute[*admissionPrefix](request, p.admissionKey())
	require.True(t, ok)
	cloned := prepared.Clone().(*admissionPrefix)
	prepared.perPromptKeys[0][0]++
	require.Equal(t, kvblock.BlockHash(123), cloned.perPromptKeys[0][0])
}

func TestAdmissionPreparationCancellationPublishesNoState(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	indexer := &fakeKVCacheIndexer{
		computeFromTokens: func(context.Context, []uint32, string, []*kvblock.BlockExtraFeatures) ([]kvblock.BlockHash, error) {
			return []kvblock.BlockHash{123}, nil
		},
		matchBlockKeys: func(context.Context, []kvblock.BlockHash, sets.Set[string]) (map[string]kvcache.PodMatch, error) {
			cancel()
			return map[string]kvcache.PodMatch{"10.0.0.1:8080": {WeightedScore: 1}}, nil
		},
	}
	p := newProducerWithIndexer(t.Context(), indexer)
	p.speculativeEnabled = true
	request := &scheduling.InferenceRequest{RequestID: "cancelled", Body: &fwkrh.InferenceRequestBody{
		TokenizedRequest: fwkrh.NewTokenizedRequest([][]uint32{make([]uint32, testBlockSize)}),
	}}
	endpoints := freshEndpoints()
	require.ErrorIs(t, p.PrepareForAdmission(ctx, request, endpoints), context.Canceled)
	_, ok := request.GetAttribute(p.admissionKey())
	require.False(t, ok)
	for _, endpoint := range endpoints {
		_, ok = endpoint.Get(p.dk)
		require.False(t, ok)
	}
	_, err := plugin.ReadPluginStateKey[*blockKeysState](p.pluginState, request.RequestID, blockKeysStateKey)
	require.Error(t, err)
}
