/*
Copyright 2021 The Vitess Authors.

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

package asthelpergen

import (
	"fmt"
	"strings"
	"testing"

	"vitess.io/vitess/go/tools/codegen"

	"github.com/stretchr/testify/require"
)

func TestFullGeneration(t *testing.T) {
	result, err := GenerateASTHelpers(&Options{
		Packages:      []string{"./integration/..."},
		RootInterface: "vitess.io/vitess/go/tools/asthelpergen/integration.AST",
		Clone: CloneOptions{
			Exclude: []string{"*NoCloneType"},
		},
	})
	require.NoError(t, err)

	verifyErrors := VerifyFilesOnDisk(result)
	require.Empty(t, verifyErrors)

	for _, file := range result {
		contents := fmt.Sprintf("%#v", file)
		require.Contains(t, contents, "http://www.apache.org/licenses/LICENSE-2.0")
		applyIdx := strings.Index(contents, "func (a *application) apply(parent, node AST, replacer replacerFunc)")
		cloneIdx := strings.Index(contents, "CloneAST(in AST) AST")
		require.False(t, applyIdx == 0 && cloneIdx == 0, "file doesn't contain expected contents")
	}
}

func TestRecreateAllFiles(t *testing.T) {
	// t.Skip("This test recreates all files in the integration directory. It should only be run when the ASTHelperGen code has changed.")
	result, err := GenerateASTHelpers(&Options{
		Packages:      []string{"./integration/..."},
		RootInterface: "vitess.io/vitess/go/tools/asthelpergen/integration.AST",
		Clone: CloneOptions{
			Exclude: []string{"*NoCloneType"},
		},
	})
	require.NoError(t, err)

	for fullPath, file := range result {
		err := codegen.SaveJenFile(fullPath, file)
		require.NoError(t, err)
	}
}
