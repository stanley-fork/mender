// Copyright 2021 Northern.tech AS
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

package app

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/mendersoftware/mender/dbus"
	"github.com/mendersoftware/mender/dbus/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// TestControlMap tests the ControlMap structure, and verifies the thread-safety
// of the data access, and writes.
func TestControlMap(t *testing.T) {
	cm := NewControlMap()
	cm.Set(&UpdateControlMap{
		ID:       "foo",
		Priority: 0,
	})
	cm.Set(&UpdateControlMap{
		ID:       "foo",
		Priority: 1,
	})
	cm.Set(&UpdateControlMap{
		ID:       "foo",
		Priority: 1,
	})
	cm.Set(&UpdateControlMap{
		ID:       "foo",
		Priority: 0,
	})
	assert.EqualValues(t, cm.Get("foo"), []*UpdateControlMap{
		&UpdateControlMap{ID: "foo", Priority: 0},
		&UpdateControlMap{ID: "foo", Priority: 1},
	}, cm.controlMap)
	assert.Equal(t, len(cm.Get("foo")), 2, "The map has a duplicate")
}

func TestUpdateControlMapStateValidation(t *testing.T) {
	// Empty values, shall validate
	stateEmpty := UpdateControlMapState{}
	assert.NoError(t, stateEmpty.Validate())

	// Legal values, shall validate
	for _, value := range []string{"continue", "force_continue", "pause", "fail"} {
		stateAction := UpdateControlMapState{
			Action: value,
		}
		assert.NoError(t, stateAction.Validate())

		stateOnMapExpire := UpdateControlMapState{
			OnMapExpire: value,
		}
		if value == "pause" {
			// Except for "OnMapExpire": "pause", which is not allowed
			assert.Error(t, stateOnMapExpire.Validate())
		} else {
			assert.NoError(t, stateOnMapExpire.Validate())
		}

		stateOnActionExecuted := UpdateControlMapState{
			OnActionExecuted: value,
		}
		assert.NoError(t, stateOnActionExecuted.Validate())
	}

	// Any other string, shall invalidate
	stateActionFoo := UpdateControlMapState{
		Action: "foo",
	}
	assert.Error(t, stateActionFoo.Validate())
	stateOnMapExpireFoo := UpdateControlMapState{
		OnMapExpire: "bar",
	}
	assert.Error(t, stateOnMapExpireFoo.Validate())
	stateOnActionExecutedFoo := UpdateControlMapState{
		OnActionExecuted: "baz",
	}
	assert.Error(t, stateOnActionExecutedFoo.Validate())
}

func TestUpdateControlMapValidation(t *testing.T) {
	// Empty, shall invalidate
	mapEmpty := UpdateControlMap{}
	assert.Error(t, mapEmpty.Validate())

	// Only ID, shall validate
	mapOnlyID := UpdateControlMap{
		ID: "whatever",
	}
	assert.NoError(t, mapOnlyID.Validate())

	// Legal values, shall validate
	for _, value := range []string{
		"ArtifactInstall_Enter",
		"ArtifactReboot_Enter",
		"ArtifactCommit_Enter",
	} {
		mapValid := UpdateControlMap{
			ID:     "whatever",
			States: map[string]UpdateControlMapState{value: {}},
		}
		assert.NoError(t, mapValid.Validate())
	}
}

func TestUpdateControlMapValidationFromJSON(t *testing.T) {
	jsonString := `{
	"priority": 0,
	"states": {
		"ArtifactInstall_Enter": {
			"action": "continue",
			"on_map_expire": "force_continue",
			"on_action_executed": "pause"
		},
		"ArtifactReboot_Enter": {
			"action": "pause",
			"on_map_expire": "fail",
			"on_action_executed": "continue"
		}
	},
	"id": "01234567-89ab-cdef-0123-456789abcdef"
}`

	controlMap := UpdateControlMap{}
	err := json.Unmarshal([]byte(jsonString), &controlMap)
	assert.NoError(t, err)
	assert.NoError(t, controlMap.Validate())

	assert.Equal(t, 2, len(controlMap.States))
	state1 := controlMap.States["ArtifactInstall_Enter"]
	assert.Equal(t, "continue", state1.Action)
	state2 := controlMap.States["ArtifactReboot_Enter"]
	assert.Equal(t, "fail", state2.OnMapExpire)
}

func TestUpdateControlMapStateSanitize(t *testing.T) {

	tc := []struct {
		controlMapState          UpdateControlMapState
		controlMapStateSanitized UpdateControlMapState
	}{
		{
			controlMapState: UpdateControlMapState{
				Action:           "pause",
				OnMapExpire:      "force_continue",
				OnActionExecuted: "fail",
			},
			controlMapStateSanitized: UpdateControlMapState{
				Action:           "pause",
				OnMapExpire:      "force_continue",
				OnActionExecuted: "fail",
			},
		},
		{
			controlMapState: UpdateControlMapState{},
			controlMapStateSanitized: UpdateControlMapState{
				Action:           "continue",
				OnMapExpire:      "continue",
				OnActionExecuted: "continue",
			},
		},
		{
			controlMapState: UpdateControlMapState{
				Action: "force_continue",
			},
			controlMapStateSanitized: UpdateControlMapState{
				Action:           "force_continue",
				OnMapExpire:      "force_continue",
				OnActionExecuted: "continue",
			},
		},
		{
			controlMapState: UpdateControlMapState{
				OnMapExpire: "force_continue",
			},
			controlMapStateSanitized: UpdateControlMapState{
				Action:           "continue",
				OnMapExpire:      "force_continue",
				OnActionExecuted: "continue",
			},
		},
		{
			controlMapState: UpdateControlMapState{
				OnActionExecuted: "force_continue",
			},
			controlMapStateSanitized: UpdateControlMapState{
				Action:           "continue",
				OnMapExpire:      "continue",
				OnActionExecuted: "force_continue",
			},
		},
		{
			controlMapState: UpdateControlMapState{
				Action: "fail",
			},
			controlMapStateSanitized: UpdateControlMapState{
				Action:           "fail",
				OnMapExpire:      "fail",
				OnActionExecuted: "continue",
			},
		},
		{
			controlMapState: UpdateControlMapState{
				Action: "pause",
			},
			controlMapStateSanitized: UpdateControlMapState{
				Action:           "pause",
				OnMapExpire:      "fail",
				OnActionExecuted: "continue",
			},
		},
		{
			controlMapState: UpdateControlMapState{
				OnMapExpire:      "fail",
				OnActionExecuted: "fail",
			},
			controlMapStateSanitized: UpdateControlMapState{
				Action:           "continue",
				OnMapExpire:      "fail",
				OnActionExecuted: "fail",
			},
		},
	}

	for n, tt := range tc {
		caseName := strconv.Itoa(n)
		t.Run(caseName, func(t *testing.T) {
			tt.controlMapState.Sanitize()
			assert.Equal(t, tt.controlMapStateSanitized, tt.controlMapState)

		})
	}
}

func TestUpdateControlMapSanitize(t *testing.T) {
	mapDefault := UpdateControlMap{
		ID:       "whatever",
		Priority: 100,
		States: map[string]UpdateControlMapState{
			"ArtifactInstall_Enter": {
				Action:           "continue",
				OnMapExpire:      "continue",
				OnActionExecuted: "continue",
			},
			"ArtifactReboot_Enter": {
				Action:           "continue",
				OnMapExpire:      "continue",
				OnActionExecuted: "continue",
			},
			"ArtifactCommit_Enter": {
				Action:           "continue",
				OnMapExpire:      "continue",
				OnActionExecuted: "continue",
			},
		},
	}
	mapDefault.Sanitize()
	assert.Equal(t, 0, len(mapDefault.States))

	mapOneState := UpdateControlMap{
		ID:       "whatever",
		Priority: 100,
		States: map[string]UpdateControlMapState{
			"ArtifactInstall_Enter": {
				Action:           "continue",
				OnMapExpire:      "continue",
				OnActionExecuted: "continue",
			},
			"ArtifactReboot_Enter": {
				Action:           "fail",
				OnMapExpire:      "continue",
				OnActionExecuted: "continue",
			},
			"ArtifactCommit_Enter": {
				Action:           "continue",
				OnMapExpire:      "continue",
				OnActionExecuted: "continue",
			},
		},
	}
	mapOneState.Sanitize()
	assert.Equal(t, 1, len(mapOneState.States))
	_, ok := mapOneState.States["ArtifactReboot_Enter"]
	assert.True(t, ok)
}

func setupTestUpdateManager() dbus.DBusAPI {
	dbusAPI := &mocks.DBusAPI{}

	dbusConn := dbus.Handle(nil)

	dbusAPI.On("BusGet",
		mock.AnythingOfType("uint"),
	).Return(dbusConn, nil)

	dbusAPI.On("BusOwnNameOnConnection",
		dbusConn,
		UpdateManagerDBusObjectName,
		mock.AnythingOfType("uint"),
	).Return(uint(1), nil)

	dbusAPI.On("BusRegisterInterface",
		dbusConn,
		UpdateManagerDBusPath,
		UpdateManagerDBusInterface,
	).Return(uint(2), nil)

	dbusAPI.On("RegisterMethodCallCallback",
		UpdateManagerDBusPath,
		UpdateManagerDBusInterfaceName,
		updateManagerSetUpdateControlMap,
		mock.Anything,
	)

	dbusAPI.On("UnregisterMethodCallCallback",
		UpdateManagerDBusPath,
		UpdateManagerDBusInterfaceName,
		updateManagerSetUpdateControlMap,
	)

	dbusAPI.On("BusUnregisterInterface",
		dbusConn,
		uint(2),
	).Return(true)

	dbusAPI.On("BusUnownName",
		uint(1),
	)
	return dbusAPI

}

func TestUpdateManager(t *testing.T) {

	api := setupTestUpdateManager()
	defer api.(*mocks.DBusAPI).AssertExpectations(t)
	um := NewUpdateManager(6)
	um.EnableDBus(api)
	ctx, cancel := context.WithCancel(context.Background())
	go um.run(ctx)
	time.Sleep(3 * time.Second)
	cancel()
	// Give the defered functions some time to run
	time.Sleep(3 * time.Second)

}