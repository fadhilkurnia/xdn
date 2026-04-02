package raftnode

import "encoding/json"

type CommandType uint8

const (
	CmdCreateBook CommandType = iota
	CmdUpdateBook
	CmdDeleteBook
)

type Command struct {
	Type   CommandType `json:"type"`
	BookID uint32      `json:"book_id,omitempty"`
	Title  string      `json:"title,omitempty"`
	Author string      `json:"author,omitempty"`
}

func Encode(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}

func Decode(data []byte) (Command, error) {
	var cmd Command
	err := json.Unmarshal(data, &cmd)
	return cmd, err
}
