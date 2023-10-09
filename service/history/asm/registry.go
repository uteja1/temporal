// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package asm

import (
	"fmt"

	plugin "go.temporal.io/server/service/history/asm/plugin"
)

type (
	Registry interface {
		Register(name string, plugin plugin.Plugin)
		Get(name string) plugin.Plugin
		List() []plugin.Plugin
	}

	registeryImpl struct {
		plugins map[string]plugin.Plugin
	}
)

func NewRegistry() Registry {
	return &registeryImpl{
		plugins: make(map[string]plugin.Plugin),
	}
}

func (r *registeryImpl) Register(name string, plugin plugin.Plugin) {
	if _, ok := r.plugins[name]; ok {
		panic(fmt.Sprintf("plugin %v already registered", name))
	}
	r.plugins[name] = plugin
}

func (r *registeryImpl) Get(name string) plugin.Plugin {
	return r.plugins[name]
}

func (r *registeryImpl) List() []plugin.Plugin {
	var plugins []plugin.Plugin
	for _, p := range r.plugins {
		plugins = append(plugins, p)
	}
	return plugins
}
