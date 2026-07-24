package flinksqlgateway

import (
	"context"
	"sync"
)

// activityGroup은 wrapper가 시작한 실행과 stream을 추적하여 Close가 안전하게 종료를 기다리게 한다.
type activityGroup struct {
	mu        sync.Mutex
	closed    bool
	closedErr error
	active    int
	idle      chan struct{}
	streams   map[ResultStream]struct{}
}

// newActivityGroup은 활동이 없는 열린 추적기를 생성한다.
func newActivityGroup(closedErr error) *activityGroup {
	idle := make(chan struct{})
	close(idle)
	return &activityGroup{
		closedErr: closedErr,
		idle:      idle,
		streams:   make(map[ResultStream]struct{}),
	}
}

// begin은 Close와 원자적으로 새 활동을 등록한다.
func (g *activityGroup) begin() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return g.closedErr
	}
	if g.active == 0 {
		g.idle = make(chan struct{})
	}
	g.active++
	return nil
}

// end는 활동 하나를 종료하고 마지막 활동이면 idle 대기자를 깨운다.
func (g *activityGroup) end() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active == 0 {
		return
	}
	g.active--
	if g.active == 0 {
		close(g.idle)
	}
}

// registerStream은 이미 begin한 stream을 Close 시 정리할 목록에 추가한다.
func (g *activityGroup) registerStream(stream ResultStream) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return g.closedErr
	}
	g.streams[stream] = struct{}{}
	return nil
}

// unregisterStream은 종료된 stream을 정리 목록에서 제거한다.
func (g *activityGroup) unregisterStream(stream ResultStream) {
	g.mu.Lock()
	delete(g.streams, stream)
	g.mu.Unlock()
}

// stop은 새 활동을 거부하고 현재 열린 stream snapshot을 반환한다.
func (g *activityGroup) stop() []ResultStream {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	streams := make([]ResultStream, 0, len(g.streams))
	for stream := range g.streams {
		streams = append(streams, stream)
	}
	return streams
}

// wait는 모든 등록 활동이 끝나거나 context가 취소될 때까지 기다린다.
func (g *activityGroup) wait(ctx context.Context) error {
	g.mu.Lock()
	idle := g.idle
	g.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// isClosed는 새 활동이 거부되는 상태인지 반환한다.
func (g *activityGroup) isClosed() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closed
}
