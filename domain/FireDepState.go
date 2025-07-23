package domain

type FireDepState struct {
	Units  []Unit  `json:"units"`
	Events []Event `json:"events"`
}
