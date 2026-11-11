package messagix

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

func (c *Client) ExecuteTasks(ctx context.Context, tasks ...socket.Task) (*table.LSTable, error) {
	tbl, _, err := c.ExecuteTasksWithIDs(ctx, tasks...)
	return tbl, err
}

// RequestIDs identifies one ExecuteTasks request: the request_id it was sent with, and its task ids.
type RequestIDs struct {
	PacketID int64
	TaskIDs  []int64
}

// ExecuteTasksWithIDs is ExecuteTasks, and also returns the request's ids. It sends exactly the same request.
func (c *Client) ExecuteTasksWithIDs(ctx context.Context, tasks ...socket.Task) (*table.LSTable, RequestIDs, error) {
	var ids RequestIDs
	if c == nil {
		return nil, ids, ErrClientIsNil
	} else if ctx.Err() != nil {
		return nil, ids, ctx.Err()
	}
	tskm := c.newTaskManager()
	for _, task := range tasks {
		tskm.AddNewTask(task)
	}
	//tskm.setTraceId(methods.GenerateTraceID())
	for _, task := range tskm.currTasks {
		ids.TaskIDs = append(ids.TaskIDs, task.TaskID)
	}

	payload, err := tskm.FinalizePayload()
	if err != nil {
		return nil, ids, fmt.Errorf("failed to finalize payload: %w", err)
	}

	resp, err := c.makeLSRequestWithID(ctx, payload, 3, &ids.PacketID)
	if err != nil {
		return nil, ids, err
	}

	tbl, err := resp.Parse(ctx)
	if err != nil {
		return nil, ids, fmt.Errorf("failed to parse response: %w", err)
	}

	return tbl, ids, nil
}

func (c *Client) ExecuteStatelessTask(ctx context.Context, task socket.Task) error {
	if c == nil {
		return ErrClientIsNil
	} else if ctx.Err() != nil {
		return ctx.Err()
	}
	innerPayload, queueName := task.Create()
	label := task.GetLabel()
	if queueName != "" {
		return fmt.Errorf("tried to execute stateful task %s as stateless", label)
	}
	innerPayloadMarshalled, err := json.Marshal(innerPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal inner task %s payload: %w", label, err)
	}
	outerPayload := socket.StatelessTaskData{
		Label:   label,
		Payload: string(innerPayloadMarshalled),
		Version: strconv.FormatInt(c.configs.VersionID, 10),
	}
	outerPayloadMarshalled, err := json.Marshal(outerPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal outer task %s payload: %w", label, err)
	}
	_, err = c.makeLSRequest(ctx, outerPayloadMarshalled, 4)
	return err
}
