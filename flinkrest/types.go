package flinkrest

import "encoding/json"

// JobStatus remains open-ended for forward compatibility.
type JobStatus string

const (
	JobInitializing JobStatus = "INITIALIZING"
	JobCreated      JobStatus = "CREATED"
	JobRunning      JobStatus = "RUNNING"
	JobFailing      JobStatus = "FAILING"
	JobFailed       JobStatus = "FAILED"
	JobCancelling   JobStatus = "CANCELLING"
	JobCanceled     JobStatus = "CANCELED"
	JobFinished     JobStatus = "FINISHED"
	JobRestarting   JobStatus = "RESTARTING"
	JobSuspended    JobStatus = "SUSPENDED"
	JobReconciling  JobStatus = "RECONCILING"
)

// Job is the stable subset of GET /jobs/:jobid plus the complete raw payload.
type Job struct {
	JobID      string           `json:"jid"`
	Name       string           `json:"name"`
	State      JobStatus        `json:"state"`
	StartTime  int64            `json:"start-time"`
	EndTime    int64            `json:"end-time"`
	Duration   int64            `json:"duration"`
	Now        int64            `json:"now"`
	Timestamps map[string]int64 `json:"timestamps,omitempty"`
	Raw        json.RawMessage  `json:"-"`
}

func (j *Job) UnmarshalJSON(data []byte) error {
	type wire Job
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*j = Job(decoded)
	j.Raw = append(j.Raw[:0], data...)
	return nil
}

// SavepointFormat controls stop-with-savepoint state format.
type SavepointFormat string

const (
	SavepointCanonical SavepointFormat = "CANONICAL"
	SavepointNative    SavepointFormat = "NATIVE"
)

// StopOptions is the Flink 1.20 StopWithSavepoint request body.
type StopOptions struct {
	Drain           bool
	FormatType      SavepointFormat
	TargetDirectory string
	TriggerID       string
}

// TriggerResponse identifies an asynchronous stop operation.
type TriggerResponse struct {
	RequestID string          `json:"request-id"`
	Raw       json.RawMessage `json:"-"`
}

func (r *TriggerResponse) UnmarshalJSON(data []byte) error {
	type wire TriggerResponse
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*r = TriggerResponse(decoded)
	r.Raw = append(r.Raw[:0], data...)
	return nil
}

// JobExceptions retains the current exception-history object as raw JSON so
// newer nested fields remain available.
type JobExceptions struct {
	RootException    string          `json:"root-exception,omitempty"`
	Timestamp        int64           `json:"timestamp,omitempty"`
	Truncated        bool            `json:"truncated,omitempty"`
	ExceptionHistory json.RawMessage `json:"exceptionHistory,omitempty"`
	Raw              json.RawMessage `json:"-"`
}

func (e *JobExceptions) UnmarshalJSON(data []byte) error {
	type wire JobExceptions
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*e = JobExceptions(decoded)
	e.Raw = append(e.Raw[:0], data...)
	return nil
}

// CheckpointCounts is the stable counts subset of checkpoint statistics.
type CheckpointCounts struct {
	Restored   int64 `json:"restored"`
	Total      int64 `json:"total"`
	InProgress int64 `json:"in_progress"`
	Completed  int64 `json:"completed"`
	Failed     int64 `json:"failed"`
}

// Checkpoint is the stable subset of a checkpoint history item.
type Checkpoint struct {
	ID          int64  `json:"id"`
	Status      string `json:"status"`
	IsSavepoint bool   `json:"is_savepoint"`
	TriggerTime int64  `json:"trigger_timestamp"`
	Duration    int64  `json:"end_to_end_duration"`
}

// Checkpoints is returned by GET /jobs/:jobid/checkpoints.
type Checkpoints struct {
	Counts  CheckpointCounts `json:"counts"`
	History []Checkpoint     `json:"history"`
	Raw     json.RawMessage  `json:"-"`
}

func (c *Checkpoints) UnmarshalJSON(data []byte) error {
	type wire Checkpoints
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*c = Checkpoints(decoded)
	c.Raw = append(c.Raw[:0], data...)
	return nil
}

// JobPlan contains the raw plan object and complete response.
type JobPlan struct {
	Plan json.RawMessage `json:"plan"`
	Raw  json.RawMessage `json:"-"`
}

func (p *JobPlan) UnmarshalJSON(data []byte) error {
	type wire JobPlan
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*p = JobPlan(decoded)
	p.Raw = append(p.Raw[:0], data...)
	return nil
}
