import json
import sqlite3

def execute_and_report_sql(json_file_path, db_file_path, output_file_path):
    """
    JSON 파일에서 SQL 쿼리를 읽어와 실행하고, 결과를 파일로 저장하는 함수.
    
    Args:
        json_file_path (str): SQL 쿼리가 포함된 JSON 파일의 경로.
        db_file_path (str): 연결할 SQLite 데이터베이스 파일의 경로.
        output_file_path (str): 결과를 저장할 텍스트 파일의 경로.
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"오류: {json_file_path} 파일을 찾을 수 없습니다.")
        return
    except json.JSONDecodeError:
        print(f"오류: {json_file_path} 파일이 유효한 JSON 형식이 아닙니다.")
        return

    try:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
    except sqlite3.Error as e:
        print(f"데이터베이스 연결 오류: {e}")
        return

    total_queries = 0
    success_count = 0
    failed_queries = []

    for i, item in enumerate(data):
        sql_query = item.get("sql")
        expected_rows = item.get("sql_result_rows_count")

        if not sql_query:
            continue
        
        total_queries += 1
        
        try:
            cursor.execute(sql_query)
            result = cursor.fetchall()
            actual_rows = len(result)

            if expected_rows == actual_rows:
                success_count += 1
            else:
                failed_queries.append({
                    "index": i + 1,
                    "sql": sql_query.strip(),
                    "reason": "행 개수 불일치",
                    "expected": expected_rows,
                    "actual": actual_rows
                })

        except sqlite3.OperationalError as e:
            failed_queries.append({
                "index": i + 1,
                "sql": sql_query.strip(),
                "reason": "SQL 실행 오류",
                "error": str(e)
            })
        except Exception as e:
            failed_queries.append({
                "index": i + 1,
                "sql": sql_query.strip(),
                "reason": "예기치 않은 오류",
                "error": str(e)
            })
    
    conn.close()

    # 결과 파일 작성
    with open(output_file_path, 'w', encoding='utf-8') as f:
        f.write("--- SQL 쿼리 실행 통계 ---\n")
        f.write(f"총 쿼리 수: {total_queries}개\n")
        f.write(f"성공한 쿼리 수: {success_count}개\n")
        f.write(f"실패한 쿼리 수: {len(failed_queries)}개\n")
        f.write(f"성공률: {(success_count / total_queries) * 100:.2f}%\n")
        
        if failed_queries:
            f.write("\n\n--- 실패한 SQL 쿼리 상세 내역 ---\n")
            for item in failed_queries:
                f.write(f"\n[{item['index']}] 실패 쿼리:\n")
                f.write(f"{item['sql']}\n")
                f.write(f"실패 원인: {item['reason']}\n")
                if 'expected' in item:
                    f.write(f"  - 예상 행 수: {item['expected']}개\n")
                    f.write(f"  - 실제 행 수: {item['actual']}개\n")
                if 'error' in item:
                    f.write(f"  - 오류 메시지: {item['error']}\n")
    
    print(f"쿼리 실행 결과가 '{output_file_path}' 파일에 저장되었습니다.")

if __name__ == "__main__":
    json_file_path = "question_and_sql_pairs.json"
    db_file_path = "./data/dev/dev_databases/chinook/chinook.sqlite"
    output_file_path = "results.txt"
    
    execute_and_report_sql(json_file_path, db_file_path, output_file_path)