package domain

import "time"

type Event struct {
	Eid               *int     `json:"eid"`
	Num1              *string  `json:"num_1"`
	Location          *string  `json:"location"`
	LocationInfo      *string  `json:"location_info"`
	LocationInvolved  *string  `json:"location_involved"`
	Category          *string  `json:"category"`
	TypEng            *string  `json:"typ_eng"`
	SubEng            *string  `json:"sub_eng"`
	AlarmLev          *uint    `json:"alarm_lev"`
	EventAlarmtext    *string  `json:"event_alarmtext"`
	CreateTime        *string  `json:"create_time"`
	FirstdispatchTime *string  `json:"firstdispatch_time"`
	Latitude          *float64 `json:"latitude"`
	Longitude         *float64 `json:"longitude"`
	CallerName        *string  `json:"caller_name"`
	CallerNumber      *string  `json:"caller_number"`
	Destinations      []struct {
		Id   uint   `json:"id"`
		Name string `json:"name"`
	} `json:"destinations"`
	UserResponses struct {
		Accepted []string `json:"accepted"`
		Declined []string `json:"declined"`
	} `json:"user_responses"`
	FullChain             *bool                  `json:"fullChain"`
	WaterExtractionPoints []WaterExtractionPoint `json:"waterExtractionPoints"`
}

type WaterExtractionPoint struct {
	Id       int     `json:"id"`
	Title    *string `json:"title"`
	Geometry *struct {
		Type        *string   `json:"type"`
		Coordinates []float64 `json:"coordinates"`
	}
	ObjectType       *string    `json:"objtype"`
	ObjectSubType    *string    `json:"objsubtype"`
	AdditionalInfo   *string    `json:"additional_info"`
	FireDepartmentId *int       `json:"firedepartment_id"`
	MunicipalityId   *int       `json:"municipality_id"`
	SectorId         *int       `json:"sector_id"`
	DistrictId       *int       `json:"district_id"`
	CreatedAt        *time.Time `json:"created_at"`
	UpdatedAt        *time.Time `json:"updated_at"`
	HashCode         *string    `json:"hashcode"`

	DistanceToEventLocationKm *float64 `json:"distanceToEventLocationKm,omitempty"`
}
