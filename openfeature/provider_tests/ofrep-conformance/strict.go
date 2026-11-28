package ofrepconformance

import (
	"errors"
	"fmt"

	"gopkg.in/yaml.v3"
)

const pairWidth = 2

var errUnsupportedKeyword = errors.New("schema keyword the validator does not implement")

// implementedKeywords are the schema keywords that [Spec.ValidateResponse] evaluates.
func implementedKeywords() map[string]bool {
	return map[string]bool{
		"$ref": true, "type": true, "format": true, "pattern": true, "allOf": true, "oneOf": true, "not": true,
		"required": true, "enum": true, "properties": true, "items": true, "additionalProperties": true,
		"minimum": true,
	}
}

// annotationKeywords are the schema keywords that describe and never reject a value.
func annotationKeywords() map[string]bool {
	return map[string]bool{
		"description": true, "example": true, "examples": true, "title": true, "default": true,
		"deprecated": true, "contentMediaType": true, "contentSchema": true,
	}
}

// checkKeywords fails when a schema of the document uses a keyword that the validator neither implements nor
// takes as an annotation: a newer document that relies on `anyOf`, `maxLength`, `nullable`... must not pass
// as if those were checked.
func checkKeywords(raw []byte) error {
	var root yaml.Node

	err := yaml.Unmarshal(raw, &root)
	if err != nil {
		return fmt.Errorf("unmarshal OpenAPI spec: %w", err)
	}

	if len(root.Content) == 0 {
		return nil
	}

	doc := root.Content[0]

	for _, spot := range schemaSpots(doc) {
		err = walkSchema(spot.node, spot.path)
		if err != nil {
			return err
		}
	}

	return nil
}

type schemaSpot struct {
	node *yaml.Node
	path string
}

// child returns the value of a key of a mapping node.
func child(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}

	for idx := 0; idx+1 < len(node.Content); idx += 2 {
		if node.Content[idx].Value == key {
			return node.Content[idx+1]
		}
	}

	return nil
}

func entries(node *yaml.Node) [][2]*yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}

	out := make([][2]*yaml.Node, 0, len(node.Content)/pairWidth)
	for idx := 0; idx+1 < len(node.Content); idx += 2 {
		out = append(out, [2]*yaml.Node{node.Content[idx], node.Content[idx+1]})
	}

	return out
}

// schemaSpots lists where the document holds schemas: the components, the parameters, the bodies and the responses.
func schemaSpots(doc *yaml.Node) []schemaSpot {
	var spots []schemaSpot

	add := func(node *yaml.Node, path string) {
		if node != nil {
			spots = append(spots, schemaSpot{node: node, path: path})
		}
	}

	for _, kv := range entries(child(child(doc, "components"), "schemas")) {
		add(kv[1], "components.schemas."+kv[0].Value)
	}

	for _, kv := range entries(child(child(doc, "components"), "parameters")) {
		add(child(kv[1], "schema"), "components.parameters."+kv[0].Value)
	}

	for _, path := range entries(child(doc, "paths")) {
		for _, method := range entries(path[1]) {
			where := path[0].Value + " " + method[0].Value
			spots = append(spots, operationSpots(method[1], where)...)
		}
	}

	return spots
}

func operationSpots(oper *yaml.Node, where string) []schemaSpot {
	var spots []schemaSpot

	add := func(node *yaml.Node, path string) {
		if node != nil {
			spots = append(spots, schemaSpot{node: node, path: path})
		}
	}

	if params := child(oper, "parameters"); params != nil {
		for idx, param := range params.Content {
			add(child(param, "schema"), fmt.Sprintf("%s parameter %d", where, idx))
		}
	}

	for _, media := range entries(child(child(oper, "requestBody"), "content")) {
		add(child(media[1], "schema"), where+" request "+media[0].Value)
	}

	for _, resp := range entries(child(oper, "responses")) {
		for _, media := range entries(child(resp[1], "content")) {
			add(child(media[1], "schema"), where+" "+resp[0].Value+" "+media[0].Value)
		}
	}

	return spots
}

func walkSchema(node *yaml.Node, path string) error {
	implemented, annotations := implementedKeywords(), annotationKeywords()

	for _, entry := range entries(node) {
		key := entry[0].Value

		switch {
		case annotations[key]:
			continue
		case !implemented[key]:
			return fmt.Errorf("%w: %q in %s", errUnsupportedKeyword, key, path)
		}

		err := walkChildren(key, entry[1], path)
		if err != nil {
			return err
		}
	}

	return nil
}

func walkChildren(key string, value *yaml.Node, path string) error {
	var subs []*yaml.Node

	switch key {
	case "items", "not", "additionalProperties":
		subs = []*yaml.Node{value}
	case "allOf", "oneOf":
		subs = value.Content
	case "properties":
		for _, prop := range entries(value) {
			subs = append(subs, prop[1])
		}
	}

	for _, sub := range subs {
		if sub.Kind != yaml.MappingNode {
			continue
		}

		err := walkSchema(sub, path+"."+key)
		if err != nil {
			return err
		}
	}

	return nil
}
