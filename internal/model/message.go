package model

type Message struct {
	Type    string      `json:"type"`
	Payload interface{} `json:"payload"`
	From    string      `json:"from,omitempty"`
	To      string      `json:"to,omitempty"`
} 