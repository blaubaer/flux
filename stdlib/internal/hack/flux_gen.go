// DO NOT EDIT: This file is autogenerated via the builtin command.

package hack

import (
	ast "github.com/influxdata/flux/ast"
	runtime "github.com/influxdata/flux/runtime"
)

func init() {
	runtime.RegisterPackage(pkgAST)
}

var pkgAST = &ast.Package{
	BaseNode: ast.BaseNode{
		Errors: nil,
		Loc:    nil,
	},
	Files: []*ast.File{&ast.File{
		BaseNode: ast.BaseNode{
			Errors: nil,
			Loc: &ast.SourceLocation{
				End: ast.Position{
					Column: 14,
					Line:   8,
				},
				File:   "hack.flux",
				Source: "package hack\n\n// flatten will flatten the incoming tables so they are copied into a single\n// buffer instead of potentially split between multiple buffers.\nbuiltin flatten\n\n// order will force the order of the tables to be sorted.\nbuiltin order",
				Start: ast.Position{
					Column: 1,
					Line:   1,
				},
			},
		},
		Body: []ast.Statement{&ast.BuiltinStatement{
			BaseNode: ast.BaseNode{
				Errors: nil,
				Loc: &ast.SourceLocation{
					End: ast.Position{
						Column: 16,
						Line:   5,
					},
					File:   "hack.flux",
					Source: "builtin flatten",
					Start: ast.Position{
						Column: 1,
						Line:   5,
					},
				},
			},
			ID: &ast.Identifier{
				BaseNode: ast.BaseNode{
					Errors: nil,
					Loc: &ast.SourceLocation{
						End: ast.Position{
							Column: 16,
							Line:   5,
						},
						File:   "hack.flux",
						Source: "flatten",
						Start: ast.Position{
							Column: 9,
							Line:   5,
						},
					},
				},
				Name: "flatten",
			},
		}, &ast.BuiltinStatement{
			BaseNode: ast.BaseNode{
				Errors: nil,
				Loc: &ast.SourceLocation{
					End: ast.Position{
						Column: 14,
						Line:   8,
					},
					File:   "hack.flux",
					Source: "builtin order",
					Start: ast.Position{
						Column: 1,
						Line:   8,
					},
				},
			},
			ID: &ast.Identifier{
				BaseNode: ast.BaseNode{
					Errors: nil,
					Loc: &ast.SourceLocation{
						End: ast.Position{
							Column: 14,
							Line:   8,
						},
						File:   "hack.flux",
						Source: "order",
						Start: ast.Position{
							Column: 9,
							Line:   8,
						},
					},
				},
				Name: "order",
			},
		}},
		Imports:  nil,
		Metadata: "parser-type=rust",
		Name:     "hack.flux",
		Package: &ast.PackageClause{
			BaseNode: ast.BaseNode{
				Errors: nil,
				Loc: &ast.SourceLocation{
					End: ast.Position{
						Column: 13,
						Line:   1,
					},
					File:   "hack.flux",
					Source: "package hack",
					Start: ast.Position{
						Column: 1,
						Line:   1,
					},
				},
			},
			Name: &ast.Identifier{
				BaseNode: ast.BaseNode{
					Errors: nil,
					Loc: &ast.SourceLocation{
						End: ast.Position{
							Column: 13,
							Line:   1,
						},
						File:   "hack.flux",
						Source: "hack",
						Start: ast.Position{
							Column: 9,
							Line:   1,
						},
					},
				},
				Name: "hack",
			},
		},
	}},
	Package: "hack",
	Path:    "internal/hack",
}
