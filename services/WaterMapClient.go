package services

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/uoul/go-common/collections"
	"github.com/uoul/go-common/log"

	"github.com/fireops-software/fireops-edge-core-gateway/dal"
	"github.com/fireops-software/fireops-edge-core-gateway/domain"
	appError "github.com/fireops-software/fireops-edge-core-gateway/error"
)

const (
	last_update_key = "last_update_unix"
	geo_data_key    = "water_extraction_points"
)

type WaterMapClient struct {
	logger     log.ILogger
	fireOpsApi dal.IFireOpsCoreApi
	db         redis.UniversalClient

	retryInterval time.Duration
	updateTimeout time.Duration
	pollRate      time.Duration
	radius        float32
}

func (w *WaterMapClient) run(ctx context.Context) error {
	ticker := time.NewTicker(w.pollRate)
	for {
		select {
		case <-ctx.Done():
			return nil // Shutdown
		case <-ticker.C:
			if err := func() error {
				// Create Timout context for update
				uCtx, cancel := context.WithTimeout(ctx, w.updateTimeout)
				defer cancel()
				// Check, if already initialized
				initialized, err := w.db.Exists(uCtx, last_update_key).Result()
				if err != nil {
					return appError.NewErrDataAccess("failed to check if already initialized - %v", err)
				}
				lastUpdate := time.Time{} // 1970-01-01
				if initialized == 1 {
					// Get Last update
					lastUpdateUnix, err := w.db.Get(uCtx, last_update_key).Int()
					if err != nil {
						return appError.NewErrDataAccess("failed to get last update time - %v", err)
					}
					lastUpdate = time.Unix(int64(lastUpdateUnix), 0)
				}
				// Get water extraction points from fireops
				r := <-w.fireOpsApi.GetWaterExtractionPoints(uCtx, w.radius, lastUpdate)
				if r.Error != nil {
					return r.Error
				}
				// Filter valid points
				wep := collections.FilterSlice(r.Result, func(e domain.WaterExtractionPoint) bool { return e.Title != nil && len(e.Geometry.Coordinates) == 2 })
				// Store water extraction points geo data
				if len(wep) > 0 {
					if err := w.db.GeoAdd(
						uCtx,
						geo_data_key,
						collections.MapSlice(wep, func(e domain.WaterExtractionPoint) *redis.GeoLocation {
							return &redis.GeoLocation{
								Name:      fmt.Sprintf("%d", e.Id),
								Longitude: e.Geometry.Coordinates[0],
								Latitude:  e.Geometry.Coordinates[1],
							}
						})...,
					).Err(); err != nil {
						return appError.NewErrDataAccess("failed to store water extraction points into database - %v", err)
					}
					// Store additional data
					for _, extractionPoint := range r.Result {
						w.db.JSONSet(uCtx, fmt.Sprintf("%s:%d", geo_data_key, extractionPoint.Id), "$", extractionPoint)
					}
				}
				// Store update time
				if err := w.db.Set(uCtx, last_update_key, time.Now().Unix(), 0).Err(); err != nil {
					return appError.NewErrDataAccess("failed to store last update time into database - %v", err)
				}
				return nil
			}(); err != nil {
				return err
			}
		}
	}
}

func (w *WaterMapClient) GetWaterExtractionPoints(ctx context.Context, lat float64, lon float64, radiusKm float64) ([]domain.WaterExtractionPoint, error) {
	// Get water extraction points from database
	r, err := w.db.GeoSearchLocation(ctx, geo_data_key, &redis.GeoSearchLocationQuery{
		GeoSearchQuery: redis.GeoSearchQuery{
			Longitude:  lon,
			Latitude:   lat,
			Radius:     radiusKm,
			RadiusUnit: "km",
			Sort:       "ASC",
		},
		WithDist:  true,
		WithCoord: true,
	}).Result()
	if err != nil {
		return nil, appError.NewErrDataAccess("failed to get nearby locations - %v", err)
	}
	// Get location data
	extractionPoints := []domain.WaterExtractionPoint{}
	for _, location := range r {
		// Create data key
		dataKey := fmt.Sprintf("%s:%s", geo_data_key, location.Name)
		// Get Data from kv store
		data, err := w.db.JSONGet(ctx, dataKey, "$").Result()
		if err != nil {
			continue
		}
		// Parse json
		epd := []domain.WaterExtractionPoint{}
		if err := json.Unmarshal([]byte(data), &epd); err != nil {
			w.logger.Errorf("failed to get jsondata for %s - %v", dataKey, err)
			continue
		}
		// Add distance to location
		epd = collections.MapSlice(epd, func(e domain.WaterExtractionPoint) domain.WaterExtractionPoint {
			e.DistanceToEventLocationKm = &location.Dist
			return e
		})
		// Add to result set
		extractionPoints = append(extractionPoints, epd...)
	}
	// Return result
	return extractionPoints, nil
}

func WithWaterMapClientCacheRadius(r uint) func(*WaterMapClient) {
	return func(wmc *WaterMapClient) {
		wmc.radius = float32(r) / 1000.0 // r in meters, convert to km
	}
}

func NewWaterMapClient(ctx context.Context, logger log.ILogger, fireOpsApi dal.IFireOpsCoreApi, db redis.UniversalClient, opts ...func(*WaterMapClient)) *WaterMapClient {
	c := &WaterMapClient{
		logger:     logger,
		fireOpsApi: fireOpsApi,
		db:         db,

		retryInterval: 300 * time.Second,
		pollRate:      60 * time.Second,
		updateTimeout: 30 * time.Second,
		radius:        10.0,
	}
	for _, o := range opts {
		o(c)
	}
	go func() {
		for {
			err := c.run(ctx)
			if err == nil {
				return // Gracefull shutdown
			}
			logger.Errorf("%v", err)
			time.Sleep(c.retryInterval)
		}
	}()
	return c
}
