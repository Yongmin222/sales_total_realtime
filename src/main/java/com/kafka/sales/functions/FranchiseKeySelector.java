package com.kafka.sales.functions;

import com.kafka.sales.model.ReceiptData;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * =======================================================
 * 프랜차이즈 ID 기반 키 선택 클래스
 * =======================================================
 * 
 * 📋 기능:
 * - 영수증 데이터에서 프랜차이즈 ID를 추출하여 키로 사용
 * - Flink의 keyBy() 연산에서 사용되는 키 선택기
 * - 같은 프랜차이즈의 데이터들을 같은 파티션으로 그룹화
 * 
 * 🎯 목적:
 * - 프랜차이즈별 병렬 처리 지원
 * - 상태 기반 집계를 위한 데이터 분할
 * - 같은 키의 데이터는 항상 같은 태스크에서 처리
 * 
 * 💡 동작 방식:
 * - 입력: ReceiptData (franchise_id=101, ...)
 * - 출력: Integer (101)
 * - 결과: franchise_id가 101인 모든 데이터가 같은 파티션으로 이동
 * 
 * 🔄 데이터 플로우에서의 역할:
 * Filter → [KeyBy] → Process (상태 기반 집계)
 */
public class FranchiseKeySelector implements KeySelector<ReceiptData, Integer> {
    
    /**
     * =======================================================
     * 키 추출 메서드
     * =======================================================
     * 
     * 🔑 키 선택 로직:
     * - ReceiptData 객체에서 franchise_id 필드 추출
     * - 해당 값을 Integer로 반환
     * 
     * 📊 효과:
     * - 같은 프랜차이즈의 모든 영수증이 같은 파티션에서 처리됨
     * - 프랜차이즈별 누적 상태 관리 가능
     * - 병렬 처리 시에도 데이터 일관성 보장
     * 
     * @param receipt 키를 추출할 영수증 데이터
     * @return 프랜차이즈 ID (그룹화 키)
     * @throws Exception 키 추출 실패 시
     */
    @Override
    public Integer getKey(ReceiptData receipt) throws Exception {
        return receipt.getFranchise_id();
    }
}
