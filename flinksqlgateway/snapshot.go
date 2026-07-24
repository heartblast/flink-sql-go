package flinksqlgateway

import "encoding/json"

// cloneOperation은 stream consumer가 내부 fetch와 cleanup handle을 변경하지 못하게 복사한다.
func cloneOperation(operation *Operation) *Operation {
	if operation == nil {
		return nil
	}
	cloned := *operation
	return &cloned
}

// cloneRow는 raw JSON field를 공유하지 않는 changelog row snapshot을 만든다.
func cloneRow(row Row) Row {
	row.Fields = cloneRawMessages(row.Fields)
	return row
}

// cloneResultPage는 nested column, row와 raw payload를 포함한 결과 page를 깊게 복사한다.
func cloneResultPage(page *ResultPage) *ResultPage {
	if page == nil {
		return nil
	}
	cloned := *page
	cloned.Raw = append(json.RawMessage(nil), page.Raw...)
	if page.Results != nil {
		info := *page.Results
		info.Columns = cloneColumns(page.Results.Columns)
		info.Data = make([]Row, len(page.Results.Data))
		for index := range page.Results.Data {
			info.Data[index] = cloneRow(page.Results.Data[index])
		}
		cloned.Results = &info
	}
	return &cloned
}

// cloneResultEvent는 consumer에게 전달하는 pointer와 nested slice를 내부 상태에서 분리한다.
func cloneResultEvent(event ResultEvent) ResultEvent {
	result := event
	result.Operation = cloneOperation(event.Operation)
	result.Page = cloneResultPage(event.Page)
	if event.Row != nil {
		row := cloneRow(*event.Row)
		result.Row = &row
	}
	return result
}
