package flinkrest

import "encoding/json"

// JobStatus는 이후 Flink 버전의 알 수 없는 상태도 보존할 수 있는 Job 상태 문자열이다.
type JobStatus string

const (
	// JobInitializing은 Job 초기화를 진행하는 상태이다.
	JobInitializing JobStatus = "INITIALIZING"
	// JobCreated는 Job을 만들었지만 아직 실행하지 않은 상태이다.
	JobCreated JobStatus = "CREATED"
	// JobRunning은 Job이 실행 중인 상태이다.
	JobRunning JobStatus = "RUNNING"
	// JobFailing은 Job 실패 처리 중인 상태이다.
	JobFailing JobStatus = "FAILING"
	// JobFailed는 Job이 실패로 종료된 상태이다.
	JobFailed JobStatus = "FAILED"
	// JobCancelling은 Job 취소 처리 중인 상태이다.
	JobCancelling JobStatus = "CANCELLING"
	// JobCanceled는 Job이 취소로 종료된 상태이다.
	JobCanceled JobStatus = "CANCELED"
	// JobFinished는 Job이 성공적으로 끝난 상태이다.
	JobFinished JobStatus = "FINISHED"
	// JobRestarting은 Job 재시작을 진행하는 상태이다.
	JobRestarting JobStatus = "RESTARTING"
	// JobSuspended는 Job 실행이 중단된 상태이다.
	JobSuspended JobStatus = "SUSPENDED"
	// JobReconciling은 Job 상태를 조정하는 상태이다.
	JobReconciling JobStatus = "RECONCILING"
)

// Job은 GET /jobs/:jobid 응답의 안정적인 필드와 전체 원본 payload를 보존한다.
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

// UnmarshalJSON은 알려진 Job 필드를 해석하면서 전체 원본 JSON도 보존한다.
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

// SavepointFormat은 stop-with-savepoint가 저장할 상태 형식을 지정한다.
type SavepointFormat string

const (
	// SavepointCanonical은 표준 canonical savepoint 형식이다.
	SavepointCanonical SavepointFormat = "CANONICAL"
	// SavepointNative는 backend 고유의 native savepoint 형식이다.
	SavepointNative SavepointFormat = "NATIVE"
)

// StopOptions는 Flink 1.20 StopWithSavepoint 요청 옵션이다.
type StopOptions struct {
	Drain           bool
	FormatType      SavepointFormat
	TargetDirectory string
	TriggerID       string
}

// TriggerResponse는 비동기 Job 중지 작업을 식별한다.
type TriggerResponse struct {
	RequestID string          `json:"request-id"`
	Raw       json.RawMessage `json:"-"`
}

// UnmarshalJSON은 요청 ID를 해석하면서 중지 작업의 전체 원본 JSON도 보존한다.
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

// JobExceptions는 새 nested field도 사용할 수 있도록 현재 예외 이력과 원본 JSON을 보존한다.
type JobExceptions struct {
	RootException    string          `json:"root-exception,omitempty"`
	Timestamp        int64           `json:"timestamp,omitempty"`
	Truncated        bool            `json:"truncated,omitempty"`
	ExceptionHistory json.RawMessage `json:"exceptionHistory,omitempty"`
	Raw              json.RawMessage `json:"-"`
}

// UnmarshalJSON은 알려진 예외 필드를 해석하면서 전체 원본 JSON도 보존한다.
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

// CheckpointCounts는 checkpoint 통계 중 안정적으로 제공되는 개수 필드이다.
type CheckpointCounts struct {
	Restored   int64 `json:"restored"`
	Total      int64 `json:"total"`
	InProgress int64 `json:"in_progress"`
	Completed  int64 `json:"completed"`
	Failed     int64 `json:"failed"`
}

// Checkpoint는 checkpoint 이력 항목 중 안정적으로 제공되는 필드이다.
type Checkpoint struct {
	ID          int64  `json:"id"`
	Status      string `json:"status"`
	IsSavepoint bool   `json:"is_savepoint"`
	TriggerTime int64  `json:"trigger_timestamp"`
	Duration    int64  `json:"end_to_end_duration"`
}

// Checkpoints는 GET /jobs/:jobid/checkpoints가 반환하는 통계, 이력과 원본 응답이다.
type Checkpoints struct {
	Counts  CheckpointCounts `json:"counts"`
	History []Checkpoint     `json:"history"`
	Raw     json.RawMessage  `json:"-"`
}

// UnmarshalJSON은 checkpoint 필드를 해석하면서 전체 원본 JSON도 보존한다.
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

// JobPlan은 Job plan 객체와 전체 원본 응답을 보존한다.
type JobPlan struct {
	Plan json.RawMessage `json:"plan"`
	Raw  json.RawMessage `json:"-"`
}

// UnmarshalJSON은 plan 필드를 해석하면서 전체 원본 JSON도 보존한다.
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
