// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tdbg

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pborman/uuid"
	"github.com/urfave/cli/v2"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/payloads"
	"google.golang.org/protobuf/types/known/durationpb"
)

func newTopActivityCommands(
	clientFactory ClientFactory,
) []*cli.Command {
	return []*cli.Command{
		{
			Name:  "create",
			Usage: "Create a new top level activity",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagID,
					Usage: "ID",
				},
				&cli.StringFlag{
					Name:    FlagActivityType,
					Aliases: FlagActivityTypeAlias,
					Usage:   "ActivityType",
				},
				&cli.StringFlag{
					Name:  FlagTaskQueue,
					Usage: "TaskQueue",
				},
				&cli.StringSliceFlag{
					Name:  FlagPayloads,
					Usage: "Input payloads.",
				},
			},
			Action: func(ctx *cli.Context) error {
				return StartTopActivity(ctx, clientFactory)
			},
		},
		{
			Name:  "describe",
			Usage: "Describe a new top level activity",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagID,
					Usage: "ID",
				},
				&cli.StringFlag{
					Name:    FlagRunID,
					Aliases: FlagRunIDAlias,
					Usage:   "Run ID",
				},
			},
			Action: func(ctx *cli.Context) error {
				return DescribeTopActivity(ctx, clientFactory)
			},
		},
		{
			Name:  "get-task",
			Usage: "Get task from top level activity",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagID,
					Usage: "ID",
				},
				&cli.StringFlag{
					Name:    FlagRunID,
					Aliases: FlagRunIDAlias,
					Usage:   "Run ID",
				},
			},
			Action: func(ctx *cli.Context) error {
				return GetTopActivityTask(ctx, clientFactory)
			},
		},
		{
			Name:  "complete-task",
			Usage: "Respond completed to top level activity task.",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagID,
					Usage: "ID",
				},
				&cli.StringFlag{
					Name:    FlagRunID,
					Aliases: FlagRunIDAlias,
					Usage:   "Run ID",
				},
				&cli.StringFlag{
					Name:  FlagTaskToken,
					Usage: "Task token.",
				},
				&cli.StringSliceFlag{
					Name:  FlagPayloads,
					Usage: "Result payloads.",
				},
			},
			Action: func(ctx *cli.Context) error {
				return CompleteTopActivityTask(ctx, clientFactory)
			},
		},
		{
			Name:  "fail-task",
			Usage: "Respond failure to top level activity task.",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagID,
					Usage: "ID",
				},
				&cli.StringFlag{
					Name:    FlagRunID,
					Aliases: FlagRunIDAlias,
					Usage:   "Run ID",
				},
				&cli.StringFlag{
					Name:  FlagTaskToken,
					Usage: "Task token.",
				},
				&cli.StringFlag{
					Name:  FlagMessage,
					Usage: "Failure message.",
				},
			},
			Action: func(ctx *cli.Context) error {
				return FailTopActivityTask(ctx, clientFactory)
			},
		},
		{
			Name:  "show",
			Usage: "Show top level activity history.",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagID,
					Usage: "ID",
				},
				&cli.StringFlag{
					Name:    FlagRunID,
					Aliases: FlagRunIDAlias,
					Usage:   "Run ID",
				},
			},
			Action: func(ctx *cli.Context) error {
				return ShowTopActivityHistory(ctx, clientFactory)
			},
		},
	}
}

func StartTopActivity(
	c *cli.Context,
	clientFactory ClientFactory,
) error {
	ctx, cancel := newContext(c)
	defer cancel()

	nsName, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	id, err := getRequiredOption(c, FlagID)
	if err != nil {
		return err
	}

	tq, err := getRequiredOption(c, FlagTaskQueue)
	if err != nil {
		return err
	}

	at, err := getRequiredOption(c, FlagActivityType)
	if err != nil {
		return err
	}

	input, err := unmarshalPayloadsFromCLI(c)
	if err != nil {
		return err
	}

	payloads, err := payloads.Encode(input...)
	if err != nil {
		return err
	}

	client := clientFactory.WorkflowClient(c)
	resp, err := client.CreateTopActivity(
		ctx,
		&workflowservice.CreateTopActivityRequest{
			Namespace: nsName,
			Id:        id,
			ActivityType: &common.ActivityType{
				Name: at,
			},
			TaskQueue:           tq,
			Input:               payloads,
			RequestId:           uuid.New(),
			Identity:            "tdbg",
			StartToCloseTimeout: durationpb.New(time.Minute),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create top activity: %w", err)
	}

	fmt.Printf("Top activity created. Run ID: %v\n", resp.GetRunId())
	return nil
}

func DescribeTopActivity(
	c *cli.Context,
	clientFactory ClientFactory,
) error {
	ctx, cancel := newContext(c)
	defer cancel()

	nsName, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	id, err := getRequiredOption(c, FlagID)
	if err != nil {
		return err
	}

	rid := c.String(FlagRunID)

	client := clientFactory.WorkflowClient(c)
	resp, err := client.DescribeTopActivity(
		ctx,
		&workflowservice.DescribeTopActivityRequest{
			Namespace: nsName,
			Id:        id,
			RunId:     rid,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to describe top activity: %w", err)
	}

	prettyPrintJSONObject(resp)

	return nil
}

var lastTaskToken []byte

func GetTopActivityTask(
	c *cli.Context,
	clientFactory ClientFactory,
) error {
	ctx, cancel := newContext(c)
	defer cancel()

	nsName, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	id, err := getRequiredOption(c, FlagID)
	if err != nil {
		return err
	}

	rid := c.String(FlagRunID)

	client := clientFactory.WorkflowClient(c)
	resp, err := client.GetTopActivityTask(
		ctx,
		&workflowservice.GetTopActivityTaskRequest{
			Namespace: nsName,
			Id:        id,
			RunId:     rid,
			Identity:  "tdbg",
		},
	)
	if err != nil {
		return fmt.Errorf("failed to get top activity task: %w", err)
	}

	prettyPrintJSONObject(resp)

	var input map[string]interface{}
	if err := payloads.Decode(resp.Input, &input); err != nil {
		return err
	}
	fmt.Println("Inputs", input)

	return nil
}

func CompleteTopActivityTask(
	c *cli.Context,
	clientFactory ClientFactory,
) error {
	ctx, cancel := newContext(c)
	defer cancel()

	nsName, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	id, err := getRequiredOption(c, FlagID)
	if err != nil {
		return err
	}

	rid := c.String(FlagRunID)

	taskToken, err := getRequiredOption(c, FlagTaskToken)
	if err != nil {
		return err
	}
	taskTokenBytes, err := base64.StdEncoding.DecodeString(taskToken)
	if err != nil {
		return err
	}

	input, err := unmarshalPayloadsFromCLI(c)
	if err != nil {
		return err
	}

	payloads, err := payloads.Encode(input...)
	if err != nil {
		return err
	}

	client := clientFactory.WorkflowClient(c)
	_, err = client.RespondTopActivityCompleted(
		ctx,
		&workflowservice.RespondTopActivityCompletedRequest{
			Namespace: nsName,
			Id:        id,
			RunId:     rid,
			Token:     taskTokenBytes,
			Result:    payloads,
		},
	)
	return err
}

func FailTopActivityTask(
	c *cli.Context,
	clientFactory ClientFactory,
) error {
	ctx, cancel := newContext(c)
	defer cancel()

	nsName, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	id, err := getRequiredOption(c, FlagID)
	if err != nil {
		return err
	}

	rid := c.String(FlagRunID)

	taskToken, err := getRequiredOption(c, FlagTaskToken)
	if err != nil {
		return err
	}
	taskTokenBytes, err := base64.StdEncoding.DecodeString(taskToken)
	if err != nil {
		return err
	}

	failureMsg, err := getRequiredOption(c, FlagMessage)
	if err != nil {
		return err
	}

	client := clientFactory.WorkflowClient(c)
	_, err = client.RespondTopActivityFailed(
		ctx,
		&workflowservice.RespondTopActivityFailedRequest{
			Namespace: nsName,
			Id:        id,
			RunId:     rid,
			Token:     taskTokenBytes,
			Failure: &failure.Failure{
				Message: failureMsg,
			},
		},
	)
	return err
}

func ShowTopActivityHistory(
	c *cli.Context,
	clientFactory ClientFactory,
) error {
	ctx, cancel := newContext(c)
	defer cancel()

	nsName, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	id, err := getRequiredOption(c, FlagID)
	if err != nil {
		return err
	}

	rid := c.String(FlagRunID)

	client := clientFactory.WorkflowClient(c)
	resp, err := client.GetTopActivityHistory(
		ctx,
		&workflowservice.GetTopActivityHistoryRequest{
			Namespace: nsName,
			Id:        id,
			RunId:     rid,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to get top activity show: %w", err)
	}

	prettyPrintJSONObject(resp)

	return nil
}

func unmarshalPayloadsFromCLI(c *cli.Context) ([]interface{}, error) {
	jsonsRaw := readJSONPayloads(c)

	var result []interface{}
	for _, jsonRaw := range jsonsRaw {
		if jsonRaw == nil {
			result = append(result, nil)
		} else {
			var j interface{}
			if err := json.Unmarshal(jsonRaw, &j); err != nil {
				return nil, fmt.Errorf("input is not valid JSON: %w", err)
			}
			result = append(result, j)
		}

	}

	return result, nil
}

func readJSONPayloads(c *cli.Context) [][]byte {
	inputsG := c.Generic(FlagPayloads)

	var inputs *cli.StringSlice
	var ok bool
	if inputs, ok = inputsG.(*cli.StringSlice); !ok {
		// input could be provided as StringFlag instead of StringSliceFlag
		ss := cli.StringSlice{}
		ss.Set(fmt.Sprintf("%v", inputsG))
		inputs = &ss
	}

	var inputsRaw [][]byte
	for _, i := range inputs.Value() {
		if strings.EqualFold(i, "null") {
			inputsRaw = append(inputsRaw, []byte(nil))
		} else {
			inputsRaw = append(inputsRaw, []byte(i))
		}
	}

	return inputsRaw

}
