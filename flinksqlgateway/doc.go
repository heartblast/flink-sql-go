// Package flinksqlgateway는 Apache Flink 1.20.x와 2.0.x부터 2.3.x까지의
// compatibility profile을 제공하는 context 지원 SQL Gateway REST client이다. JDBC,
// JVM, CGO 또는 database/sql 없이 상태가 있는 session, 비동기 operation, changelog
// row와 제한된 결과 paging을 모델링하며 Flink 2.3의 v3 Materialized Table refresh와 v4
// Script 배포를 별도 capability interface로 제공한다. Flink 1.20.4만 실제 Gateway 검증을
// 완료했으며 2.x profile은 experimental 상태이다.
package flinksqlgateway
