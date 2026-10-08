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

package approximateprefix

import (
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"

	datagraph "github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/prefixhash"
)

type admissionCountingIndexer struct {
	indexerInterface
	queries int
}

func (i *admissionCountingIndexer) MatchLongestPrefix(hashes []blockHash, candidates []ServerID) []int {
	i.queries++
	return i.indexerInterface.MatchLongestPrefix(hashes, candidates)
}

func TestAdmissionPreparationDefersStateAndReusesMatchingSnapshot(t *testing.T) {
	disableMinBlockSizeClamp(t)
	p, err := newDataProducer(t.Context(), "prepared-prefix", config{BlockSizeTokens: 1}, testHandle())
	require.NoError(t, err)
	indexer := &admissionCountingIndexer{indexerInterface: p.indexerInst}
	p.indexerInst = indexer
	endpoint := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod"}}, nil, nil)
	endpoints := []fwksched.Endpoint{endpoint}
	request := &fwksched.InferenceRequest{RequestID: "queued", Body: tokenizedBody([]uint32{1, 2})}
	hashes := prefixhash.GetBlockHashes(t.Context(), request, 1, unlimitedPrefixBlocks)
	indexer.Add(hashes[0], server{ServerID: ServerID(endpoint.GetMetadata().ID), NumOfGPUBlocks: 10})

	require.NoError(t, p.PrepareForAdmission(t.Context(), request, endpoints))
	_, err = plugin.ReadPluginStateKey[*SchedulingContextState](p.pluginState, request.RequestID, plugin.StateKey(p.typedName.Name))
	require.Error(t, err)
	info, _ := endpoint.Get(p.dk)
	require.Equal(t, 2, info.(*attrprefix.PrefixCacheMatchInfo).MatchBlocks())

	indexer.RemovePod(ServerID(endpoint.GetMetadata().ID))
	require.NoError(t, p.PrepareForAdmission(t.Context(), request, endpoints))
	info, _ = endpoint.Get(p.dk)
	require.Zero(t, info.(*attrprefix.PrefixCacheMatchInfo).MatchBlocks())
	_, err = plugin.ReadPluginStateKey[*SchedulingContextState](p.pluginState, request.RequestID, plugin.StateKey(p.typedName.Name))
	require.Error(t, err)

	indexer.Add(hashes[0], server{ServerID: ServerID(endpoint.GetMetadata().ID), NumOfGPUBlocks: 10})
	queries := indexer.queries
	datagraph.RegisterScopeSpecs([]plugin.Plugin{p})
	scoped, violations := datagraph.ScopeRequest(logr.Discard(), requestcontrol.DataProducerExtensionPoint, p, request)
	require.NoError(t, p.Produce(t.Context(), scoped, endpoints))
	require.NoError(t, violations.Write())
	require.Equal(t, queries, indexer.queries)
	info, _ = endpoint.Get(p.dk)
	require.Zero(t, info.(*attrprefix.PrefixCacheMatchInfo).MatchBlocks())
	state, err := plugin.ReadPluginStateKey[*SchedulingContextState](p.pluginState, request.RequestID, plugin.StateKey(p.typedName.Name))
	require.NoError(t, err)
	require.Zero(t, state.PrefixCacheServers[ServerID(endpoint.GetMetadata().ID)])

	prepared, ok := fwksched.ReadRequestAttribute[*admissionPrefix](request, p.admissionKey())
	require.True(t, ok)
	cloned := prepared.Clone().(*admissionPrefix)
	prepared.state.PerPromptHashes[0][0]++
	prepared.state.PrefixCacheServers[ServerID(endpoint.GetMetadata().ID)] = 42
	require.NotEqual(t, prepared.state.PerPromptHashes, cloned.state.PerPromptHashes)
	require.Zero(t, cloned.state.PrefixCacheServers[ServerID(endpoint.GetMetadata().ID)])
}
