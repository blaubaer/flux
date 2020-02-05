package semantic

import (
	codes "github.com/influxdata/flux/codes"
	errors "github.com/influxdata/flux/internal/errors"
	fbsemantic "github.com/influxdata/flux/semantic/internal/fbsemantic"
)

// DO NOT EDIT.  This file was generated by the fbgen command.

func (rcv *ArrayExpression) FromBuf(fb *fbsemantic.ArrayExpression) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "ArrayExpression.loc")
		}
	}
	if fb.ElementsLength() > 0 {
		rcv.Elements = make([]Expression, fb.ElementsLength())
		for i := 0; i < fb.ElementsLength(); i++ {
			fbWrappedExpression := new(fbsemantic.WrappedExpression)
			if !fb.Elements(fbWrappedExpression, i) {
				return errors.New(codes.Internal, "could not deserialize ArrayExpression.Elements")
			}
			if rcv.Elements[i], err = fromWrappedExpression(fbWrappedExpression); err != nil {
				return errors.Wrap(err, codes.Inherit, "ArrayExpression.Elements")
			}
		}
	}
	if rcv.Type, err = getMonoType(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "ArrayExpression.Type")
	}
	return nil
}

func (rcv *BinaryExpression) FromBuf(fb *fbsemantic.BinaryExpression) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "BinaryExpression.loc")
		}
	}
	if rcv.Operator, err = fromFBOperator(fb.Operator()); err != nil {
		return errors.Wrap(err, codes.Inherit, "BinaryExpression.Operator")
	}
	if rcv.Left, err = fromExpressionTable(fb.Left, fb.LeftType()); err != nil {
		return errors.Wrap(err, codes.Inherit, "BinaryExpression.Left")
	}
	if rcv.Right, err = fromExpressionTable(fb.Right, fb.RightType()); err != nil {
		return errors.Wrap(err, codes.Inherit, "BinaryExpression.Right")
	}
	if rcv.typ, err = getMonoType(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "BinaryExpression.typ")
	}
	return nil
}

func (rcv *Block) FromBuf(fb *fbsemantic.Block) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "Block.loc")
		}
	}
	if fb.BodyLength() > 0 {
		rcv.Body = make([]Statement, fb.BodyLength())
		for i := 0; i < fb.BodyLength(); i++ {
			fbWrappedStatement := new(fbsemantic.WrappedStatement)
			if !fb.Body(fbWrappedStatement, i) {
				return errors.New(codes.Internal, "could not deserialize Block.Body")
			}
			if rcv.Body[i], err = fromWrappedStatement(fbWrappedStatement); err != nil {
				return errors.Wrap(err, codes.Inherit, "Block.Body")
			}
		}
	}
	return nil
}

func (rcv *BooleanLiteral) FromBuf(fb *fbsemantic.BooleanLiteral) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "BooleanLiteral.loc")
		}
	}
	rcv.Value = fb.Value()
	if rcv.typ, err = getMonoType(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "BooleanLiteral.typ")
	}
	return nil
}

func (rcv *BuiltinStatement) FromBuf(fb *fbsemantic.BuiltinStatement) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "BuiltinStatement.loc")
		}
	}
	if fbID := fb.Id(nil); fbID != nil {
		rcv.ID = new(Identifier)
		if err = rcv.ID.FromBuf(fbID); err != nil {
			return errors.Wrap(err, codes.Inherit, "BuiltinStatement.ID")
		}
	}
	return nil
}

func (rcv *CallExpression) FromBuf(fb *fbsemantic.CallExpression) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "CallExpression.loc")
		}
	}
	if rcv.Callee, err = fromExpressionTable(fb.Callee, fb.CalleeType()); err != nil {
		return errors.Wrap(err, codes.Inherit, "CallExpression.Callee")
	}
	if rcv.Arguments, err = objectExprFromProperties(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "CallExpression.Arguments")
	}
	if rcv.Pipe, err = fromExpressionTableOptional(fb.Pipe, fb.PipeType()); err != nil {
		return errors.Wrap(err, codes.Inherit, "CallExpression.Pipe")
	}
	if rcv.typ, err = getMonoType(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "CallExpression.typ")
	}
	return nil
}

func (rcv *ConditionalExpression) FromBuf(fb *fbsemantic.ConditionalExpression) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "ConditionalExpression.loc")
		}
	}
	if rcv.Test, err = fromExpressionTable(fb.Test, fb.TestType()); err != nil {
		return errors.Wrap(err, codes.Inherit, "ConditionalExpression.Test")
	}
	if rcv.Alternate, err = fromExpressionTable(fb.Alternate, fb.AlternateType()); err != nil {
		return errors.Wrap(err, codes.Inherit, "ConditionalExpression.Alternate")
	}
	if rcv.Consequent, err = fromExpressionTable(fb.Consequent, fb.ConsequentType()); err != nil {
		return errors.Wrap(err, codes.Inherit, "ConditionalExpression.Consequent")
	}
	if rcv.typ, err = getMonoType(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "ConditionalExpression.typ")
	}
	return nil
}

func (rcv *DateTimeLiteral) FromBuf(fb *fbsemantic.DateTimeLiteral) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "DateTimeLiteral.loc")
		}
	}
	if fbValue := fb.Value(nil); fbValue != nil {
		rcv.Value = fromFBTime(fbValue)
	}
	if rcv.typ, err = getMonoType(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "DateTimeLiteral.typ")
	}
	return nil
}

func (rcv *DurationLiteral) FromBuf(fb *fbsemantic.DurationLiteral) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "DurationLiteral.loc")
		}
	}
	if rcv.Values, err = fromFBDurationVector(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "DurationLiteral.Values")
	}
	if rcv.typ, err = getMonoType(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "DurationLiteral.typ")
	}
	return nil
}

func (rcv *ExpressionStatement) FromBuf(fb *fbsemantic.ExpressionStatement) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "ExpressionStatement.loc")
		}
	}
	if rcv.Expression, err = fromExpressionTable(fb.Expression, fb.ExpressionType()); err != nil {
		return errors.Wrap(err, codes.Inherit, "ExpressionStatement.Expression")
	}
	return nil
}

func (rcv *File) FromBuf(fb *fbsemantic.File) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "File.loc")
		}
	}
	if fbPackage := fb.Package(nil); fbPackage != nil {
		rcv.Package = new(PackageClause)
		if err = rcv.Package.FromBuf(fbPackage); err != nil {
			return errors.Wrap(err, codes.Inherit, "File.Package")
		}
	}
	if fb.ImportsLength() > 0 {
		rcv.Imports = make([]*ImportDeclaration, fb.ImportsLength())
		for i := 0; i < fb.ImportsLength(); i++ {
			fbImportDeclaration := new(fbsemantic.ImportDeclaration)
			if !fb.Imports(fbImportDeclaration, i) {
				return errors.New(codes.Internal, "could not deserialize File.Imports")
			}
			rcv.Imports[i] = new(ImportDeclaration)
			if err = rcv.Imports[i].FromBuf(fbImportDeclaration); err != nil {
				return errors.Wrap(err, codes.Inherit, "File.Imports")
			}
		}
	}
	if fb.BodyLength() > 0 {
		rcv.Body = make([]Statement, fb.BodyLength())
		for i := 0; i < fb.BodyLength(); i++ {
			fbWrappedStatement := new(fbsemantic.WrappedStatement)
			if !fb.Body(fbWrappedStatement, i) {
				return errors.New(codes.Internal, "could not deserialize File.Body")
			}
			if rcv.Body[i], err = fromWrappedStatement(fbWrappedStatement); err != nil {
				return errors.Wrap(err, codes.Inherit, "File.Body")
			}
		}
	}
	return nil
}

func (rcv *FloatLiteral) FromBuf(fb *fbsemantic.FloatLiteral) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "FloatLiteral.loc")
		}
	}
	rcv.Value = fb.Value()
	if rcv.typ, err = getMonoType(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "FloatLiteral.typ")
	}
	return nil
}

func (rcv *Identifier) FromBuf(fb *fbsemantic.Identifier) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "Identifier.loc")
		}
	}
	rcv.Name = string(fb.Name())
	return nil
}

func (rcv *IdentifierExpression) FromBuf(fb *fbsemantic.IdentifierExpression) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "IdentifierExpression.loc")
		}
	}
	rcv.Name = string(fb.Name())
	if rcv.typ, err = getMonoType(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "IdentifierExpression.typ")
	}
	return nil
}

func (rcv *ImportDeclaration) FromBuf(fb *fbsemantic.ImportDeclaration) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "ImportDeclaration.loc")
		}
	}
	if fbAs := fb.Alias(nil); fbAs != nil {
		rcv.As = new(Identifier)
		if err = rcv.As.FromBuf(fbAs); err != nil {
			return errors.Wrap(err, codes.Inherit, "ImportDeclaration.As")
		}
	}
	if fbPath := fb.Path(nil); fbPath != nil {
		rcv.Path = new(StringLiteral)
		if err = rcv.Path.FromBuf(fbPath); err != nil {
			return errors.Wrap(err, codes.Inherit, "ImportDeclaration.Path")
		}
	}
	return nil
}

func (rcv *IndexExpression) FromBuf(fb *fbsemantic.IndexExpression) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "IndexExpression.loc")
		}
	}
	if rcv.Array, err = fromExpressionTable(fb.Array, fb.ArrayType()); err != nil {
		return errors.Wrap(err, codes.Inherit, "IndexExpression.Array")
	}
	if rcv.Index, err = fromExpressionTable(fb.Index, fb.IndexType()); err != nil {
		return errors.Wrap(err, codes.Inherit, "IndexExpression.Index")
	}
	if rcv.typ, err = getMonoType(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "IndexExpression.typ")
	}
	return nil
}

func (rcv *IntegerLiteral) FromBuf(fb *fbsemantic.IntegerLiteral) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "IntegerLiteral.loc")
		}
	}
	rcv.Value = fb.Value()
	if rcv.typ, err = getMonoType(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "IntegerLiteral.typ")
	}
	return nil
}

func (rcv *LogicalExpression) FromBuf(fb *fbsemantic.LogicalExpression) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "LogicalExpression.loc")
		}
	}
	if rcv.Operator, err = fromFBLogicalOperator(fb.Operator()); err != nil {
		return errors.Wrap(err, codes.Inherit, "LogicalExpression.Operator")
	}
	if rcv.Left, err = fromExpressionTable(fb.Left, fb.LeftType()); err != nil {
		return errors.Wrap(err, codes.Inherit, "LogicalExpression.Left")
	}
	if rcv.Right, err = fromExpressionTable(fb.Right, fb.RightType()); err != nil {
		return errors.Wrap(err, codes.Inherit, "LogicalExpression.Right")
	}
	if rcv.typ, err = getMonoType(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "LogicalExpression.typ")
	}
	return nil
}

func (rcv *MemberAssignment) FromBuf(fb *fbsemantic.MemberAssignment) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "MemberAssignment.loc")
		}
	}
	if fbMember := fb.Member(nil); fbMember != nil {
		rcv.Member = new(MemberExpression)
		if err = rcv.Member.FromBuf(fbMember); err != nil {
			return errors.Wrap(err, codes.Inherit, "MemberAssignment.Member")
		}
	}
	if rcv.Init, err = fromExpressionTable(fb.Init_, fb.Init_type()); err != nil {
		return errors.Wrap(err, codes.Inherit, "MemberAssignment.Init")
	}
	return nil
}

func (rcv *MemberExpression) FromBuf(fb *fbsemantic.MemberExpression) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "MemberExpression.loc")
		}
	}
	if rcv.Object, err = fromExpressionTable(fb.Object, fb.ObjectType()); err != nil {
		return errors.Wrap(err, codes.Inherit, "MemberExpression.Object")
	}
	rcv.Property = string(fb.Property())
	if rcv.typ, err = getMonoType(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "MemberExpression.typ")
	}
	return nil
}

func (rcv *NativeVariableAssignment) FromBuf(fb *fbsemantic.NativeVariableAssignment) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "NativeVariableAssignment.loc")
		}
	}
	if fbIdentifier := fb.Identifier(nil); fbIdentifier != nil {
		rcv.Identifier = new(Identifier)
		if err = rcv.Identifier.FromBuf(fbIdentifier); err != nil {
			return errors.Wrap(err, codes.Inherit, "NativeVariableAssignment.Identifier")
		}
	}
	if rcv.Init, err = fromExpressionTable(fb.Init_, fb.Init_type()); err != nil {
		return errors.Wrap(err, codes.Inherit, "NativeVariableAssignment.Init")
	}
	if rcv.Typ, err = getPolyType(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "NativeVariableAssignment.Typ")
	}
	return nil
}

func (rcv *ObjectExpression) FromBuf(fb *fbsemantic.ObjectExpression) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "ObjectExpression.loc")
		}
	}
	if fbWith := fb.With(nil); fbWith != nil {
		rcv.With = new(IdentifierExpression)
		if err = rcv.With.FromBuf(fbWith); err != nil {
			return errors.Wrap(err, codes.Inherit, "ObjectExpression.With")
		}
	}
	if fb.PropertiesLength() > 0 {
		rcv.Properties = make([]*Property, fb.PropertiesLength())
		for i := 0; i < fb.PropertiesLength(); i++ {
			fbProperty := new(fbsemantic.Property)
			if !fb.Properties(fbProperty, i) {
				return errors.New(codes.Internal, "could not deserialize ObjectExpression.Properties")
			}
			rcv.Properties[i] = new(Property)
			if err = rcv.Properties[i].FromBuf(fbProperty); err != nil {
				return errors.Wrap(err, codes.Inherit, "ObjectExpression.Properties")
			}
		}
	}
	if rcv.typ, err = getMonoType(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "ObjectExpression.typ")
	}
	return nil
}

func (rcv *OptionStatement) FromBuf(fb *fbsemantic.OptionStatement) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "OptionStatement.loc")
		}
	}
	if rcv.Assignment, err = fromAssignmentTable(fb.Assignment, fb.AssignmentType()); err != nil {
		return errors.Wrap(err, codes.Inherit, "OptionStatement.Assignment")
	}
	return nil
}

func (p *Package) FromBuf(fb *fbsemantic.Package) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = p.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "Package.loc")
		}
	}
	p.Package = string(fb.Package())
	if fb.FilesLength() > 0 {
		p.Files = make([]*File, fb.FilesLength())
		for i := 0; i < fb.FilesLength(); i++ {
			fbFile := new(fbsemantic.File)
			if !fb.Files(fbFile, i) {
				return errors.New(codes.Internal, "could not deserialize Package.Files")
			}
			p.Files[i] = new(File)
			if err = p.Files[i].FromBuf(fbFile); err != nil {
				return errors.Wrap(err, codes.Inherit, "Package.Files")
			}
		}
	}
	return nil
}

func (rcv *PackageClause) FromBuf(fb *fbsemantic.PackageClause) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "PackageClause.loc")
		}
	}
	if fbName := fb.Name(nil); fbName != nil {
		rcv.Name = new(Identifier)
		if err = rcv.Name.FromBuf(fbName); err != nil {
			return errors.Wrap(err, codes.Inherit, "PackageClause.Name")
		}
	}
	return nil
}

func (rcv *Property) FromBuf(fb *fbsemantic.Property) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "Property.loc")
		}
	}
	if rcv.Key, err = propertyKeyFromFBIdentifier(fb.Key(nil)); err != nil {
		return errors.Wrap(err, codes.Inherit, "Property.Key")
	}
	if rcv.Value, err = fromExpressionTable(fb.Value, fb.ValueType()); err != nil {
		return errors.Wrap(err, codes.Inherit, "Property.Value")
	}
	return nil
}

func (rcv *RegexpLiteral) FromBuf(fb *fbsemantic.RegexpLiteral) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "RegexpLiteral.loc")
		}
	}
	if rcv.Value, err = fromFBRegexpLiteral(fb.Value()); err != nil {
		return errors.Wrap(err, codes.Inherit, "RegexpLiteral.Value")
	}
	if rcv.typ, err = getMonoType(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "RegexpLiteral.typ")
	}
	return nil
}

func (rcv *ReturnStatement) FromBuf(fb *fbsemantic.ReturnStatement) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "ReturnStatement.loc")
		}
	}
	if rcv.Argument, err = fromExpressionTable(fb.Argument, fb.ArgumentType()); err != nil {
		return errors.Wrap(err, codes.Inherit, "ReturnStatement.Argument")
	}
	return nil
}

func (rcv *StringExpression) FromBuf(fb *fbsemantic.StringExpression) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "StringExpression.loc")
		}
	}
	if rcv.Parts, err = fromFBStringExpressionPartVector(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "StringExpression.Parts")
	}
	if rcv.typ, err = getMonoType(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "StringExpression.typ")
	}
	return nil
}

func (rcv *StringLiteral) FromBuf(fb *fbsemantic.StringLiteral) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "StringLiteral.loc")
		}
	}
	rcv.Value = string(fb.Value())
	if rcv.typ, err = getMonoType(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "StringLiteral.typ")
	}
	return nil
}

func (rcv *TestStatement) FromBuf(fb *fbsemantic.TestStatement) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "TestStatement.loc")
		}
	}
	if fbAssignment := fb.Assignment(nil); fbAssignment != nil {
		rcv.Assignment = new(NativeVariableAssignment)
		if err = rcv.Assignment.FromBuf(fbAssignment); err != nil {
			return errors.Wrap(err, codes.Inherit, "TestStatement.Assignment")
		}
	}
	return nil
}

func (rcv *UnaryExpression) FromBuf(fb *fbsemantic.UnaryExpression) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "UnaryExpression.loc")
		}
	}
	if rcv.Operator, err = fromFBOperator(fb.Operator()); err != nil {
		return errors.Wrap(err, codes.Inherit, "UnaryExpression.Operator")
	}
	if rcv.Argument, err = fromExpressionTable(fb.Argument, fb.ArgumentType()); err != nil {
		return errors.Wrap(err, codes.Inherit, "UnaryExpression.Argument")
	}
	if rcv.typ, err = getMonoType(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "UnaryExpression.typ")
	}
	return nil
}

func (rcv *UnsignedIntegerLiteral) FromBuf(fb *fbsemantic.UnsignedIntegerLiteral) error {
	var err error
	if fb == nil {
		return nil
	}
	if fbLoc := fb.Loc(nil); fbLoc != nil {
		if err = rcv.loc.FromBuf(fbLoc); err != nil {
			return errors.Wrap(err, codes.Inherit, "UnsignedIntegerLiteral.loc")
		}
	}
	rcv.Value = fb.Value()
	if rcv.typ, err = getMonoType(fb); err != nil {
		return errors.Wrap(err, codes.Inherit, "UnsignedIntegerLiteral.typ")
	}
	return nil
}
