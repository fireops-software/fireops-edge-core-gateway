package dal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/fireops-software/fireops-edge-core-gateway/domain"
	appError "github.com/fireops-software/fireops-edge-core-gateway/error"
	"github.com/uoul/go-common/async"
)

//--------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------

type FireOpsCoreApi struct {
	baseUrl  string
	apiToken string

	timeout time.Duration
}

//--------------------------------------------------------------------------------------------
// Public
//--------------------------------------------------------------------------------------------

// GetFireDepState implements IFireOpsCoreApi.
func (f *FireOpsCoreApi) GetFireDepState() chan async.ActionResult[domain.FireDepState] {
	r := make(chan async.ActionResult[domain.FireDepState])
	go func() {
		// Create Request context
		ctx, cancel := context.WithTimeout(context.Background(), f.timeout)
		defer cancel()
		// Create http request
		req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/api/v1/edge/user/firedepartment/unitsAndEvents", f.baseUrl), nil)
		if err != nil {
			r <- async.ActionResult[domain.FireDepState]{
				Error: appError.NewErrInternal("failed to create http request - %v", err),
			}
			return
		}
		// Do Request
		resp, err := f.doRequest(req)
		if err != nil {
			r <- async.ActionResult[domain.FireDepState]{
				Error: appError.NewErrFireOpsApi("failed to perform http request - %v", err),
			}
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			r <- async.ActionResult[domain.FireDepState]{
				Error: appError.NewErrFireOpsApi("failed to get current firedepartment state [StatusCode: %d]", resp.StatusCode),
			}
			return
		}
		// Parse Response
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			r <- async.ActionResult[domain.FireDepState]{
				Error: appError.NewErrFireOpsApi("failed read response body - %v", err),
			}
			return
		}
		fireDepState := domain.FireDepState{}
		err = json.Unmarshal(body, &fireDepState)
		if err != nil {
			r <- async.ActionResult[domain.FireDepState]{
				Error: appError.NewErrDataParsing("failed to parse FireDepState from fireops-core-api - %v", err),
			}
			return
		}
		// Success
		r <- async.ActionResult[domain.FireDepState]{
			Result: fireDepState,
			Error:  nil,
		}
	}()
	return r
}

// SendEvents implements IFireOpsCoreApi.
func (f *FireOpsCoreApi) SendEvents(events []domain.Event) chan async.ActionResult[any] {
	r := make(chan async.ActionResult[any])
	go func() {
		// Marshal events
		body, err := json.Marshal(events)
		if err != nil {
			r <- async.ActionResult[any]{
				Result: nil,
				Error:  appError.NewErrDataParsing("failed to marshal events to json - %v", err),
			}
			return
		}
		// Create Request context
		ctx, cancel := context.WithTimeout(context.Background(), f.timeout)
		defer cancel()
		// Create HTTP request
		req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/api/v1/edge/new-operation", f.baseUrl), bytes.NewBuffer(body))
		if err != nil {
			r <- async.ActionResult[any]{
				Result: nil,
				Error:  appError.NewErrInternal("failed to create http request - %v", err),
			}
			return
		}
		// Do Request
		resp, err := f.doRequest(req)
		if err != nil {
			r <- async.ActionResult[any]{
				Result: nil,
				Error:  appError.NewErrFireOpsApi("failed to perform http request - %v", err),
			}
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			r <- async.ActionResult[any]{
				Result: nil,
				Error:  appError.NewErrFireOpsApi("failed to send events to fireops-core [StatusCode: %d]", resp.StatusCode),
			}
			return
		}
		// Success
		r <- async.ActionResult[any]{
			Result: nil,
			Error:  nil,
		}
	}()
	return r
}

//--------------------------------------------------------------------------------------------
// Private
//--------------------------------------------------------------------------------------------

func (f *FireOpsCoreApi) doRequest(req *http.Request) (*http.Response, error) {
	// Add Headers
	req.Header.Add("Accept", `application/json`)
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", f.apiToken))
	// Do request
	return http.DefaultClient.Do(req)
}

//--------------------------------------------------------------------------------------------
// Constructor
//--------------------------------------------------------------------------------------------

func NewFireOpsCoreApi(baseUrl string, apiToken string) IFireOpsCoreApi {
	return &FireOpsCoreApi{
		baseUrl:  baseUrl,
		apiToken: apiToken,

		timeout: 10 * time.Second,
	}
}
