'''사용자가 데이터에베이스에 접근하기 위한 서버'''
import os
import re
from datetime import datetime, timedelta
from functools import wraps

from dotenv import load_dotenv
from flask import Flask, request, jsonify
import pymysql

# ==============================
# DB 접속 정보
# ==============================
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

host = os.getenv('ROAD2_DB_HOST')
user = os.getenv('ROAD2_DB_USER')
password = os.getenv('ROAD2_DB_PASSWORD')
name = os.getenv('ROAD2_DB_NAME')

def _get_connection():
    '''MySQL 데이터베이스 연결 객체 반환'''
    return pymysql.connect(host=host, user=user, password=password, database=name)

# ==============================
# 헬퍼 함수
# ==============================
def _username_check(conn, username):
    '''username이 user테이블에 있는지 확인 True, False 반환'''
    with conn.cursor() as cursor:
        cursor.execute("SELECT username FROM user WHERE username = %s", (username,))
        return cursor.fetchone() is not None

def _table_check(conn, table):
    '''table 이름의 테이블이 있는지 확인 True, False 반환'''
    with conn.cursor() as cursor:
        cursor.execute("SHOW TABLES LIKE %s;", (table,))
        return cursor.fetchone() is not None

# ==============================
# Flask 설정 및 데코레이터
# ==============================
app = Flask(__name__)

def handle_server_errors(func):
    '''DB 관련 공통 예외 처리 데코레이터'''
    @wraps(func)
    def wrapper(*args, **kwargs):
        conn = None
        try:
            conn = _get_connection()
            result = func(conn, *args, **kwargs)  # 원래 함수 실행
            conn.commit()
            return result
        except Exception as e:
            print(e)
            return jsonify({'message': 'server error'}), 500
        finally:
            if conn:
                conn.close()
    return wrapper

# ==============================
# API 라우터
# ==============================
@app.route('/server_check', methods=['GET']) 
@handle_server_errors
def server_check(conn):
    '''프로그램 시작 시 서버가 잘 작동하는지 확인'''
    with conn.cursor() as cursor:
        # user테이블 유무 확인
        cursor.execute("SHOW TABLES LIKE 'user';")
        has_users = cursor.fetchone() is not None
        # main테이블 유무 확인
        cursor.execute("SHOW TABLES LIKE 'main';")
        has_main = cursor.fetchone() is not None
        # record테이블 유무 확인
        cursor.execute("SHOW TABLES LIKE 'record';")
        has_record = cursor.fetchone() is not None

        if not(has_users and has_main and has_record):
            return jsonify({'message': 'Missing user or main table'}), 503

    return jsonify({'message': 'OK'}), 200

@app.route('/login', methods=['POST']) 
@handle_server_errors
def login(conn):
    '''로그인 체크 및 언어, 단어개수, add여부 반환'''
    data = request.get_json()
    username = data.get('username')
    # 로그인 성공 시
    if _username_check(conn, username):
        with conn.cursor() as cursor:
            # user 테이블에서 language, dayword, category 취득
            cursor.execute(
                "SELECT language, dayword, category FROM user WHERE username = %s",
                (username,)
            )
            row = cursor.fetchone()
            language, dayword, category = row

            if category != 'add_yourself':
                # main 테이블에서 number 순으로 dayword 개수만큼, status가 '-1'인 단어 가져오기
                cursor.execute("""
                    SELECT number, word FROM main 
                    WHERE username = %s AND status = '-1'
                    ORDER BY number ASC LIMIT %s
                    """, (username, dayword)
                )
                today_word = [[number, word] for number, word in cursor.fetchall()]
            else:
                today_word = []

            # record 테이블에 로그인 시각 기록
            cursor.execute(
                "INSERT INTO record (username, start_time) VALUES (%s, NOW())",
                (username,)
            )

            # record 로부터 (연속로그인일수) 혹은 (최초가입) 혹은 (오늘과정 이미끝) 파악
            now = datetime.now()
            today_base = now.date() if now.hour >= 5 else (now - timedelta(days=1)).date()
            cursor.execute("""
                SELECT start_time, streak FROM record
                WHERE username = %s AND status = 'o'
                ORDER BY number DESC LIMIT 1
                """, (username,)
            )
            row = cursor.fetchone()

            if row is None:
                check_streak = -2  # 로그인 기록이 없을 경우(최초 로그인)
            else:
                start_time, streak = row
                day = start_time.date() if start_time.hour >= 5 else start_time.date() - timedelta(days=1)

                if day == today_base - timedelta(days=1):
                    check_streak = streak  # 마지막 로그인이 어제인 경우(streak 반환)
                elif day == today_base:
                    check_streak = -1  # 마지막 로그인이 오늘인 경우(오늘 이미 함)
                else:
                    check_streak = 0  # 마지막 로그인이 어제 이전 인 경우 (연속로그인 끊김)

            # language, dayword, category, words_list 반환
            return jsonify({
                'language': language,
                'dayword': dayword,
                'category': category,
                'today_word': today_word,
                'streak': check_streak
            }), 201
    # 로그인 실패 시
    else:
        return jsonify({'message': 'ID not found'}), 400

@app.route('/take_more_word', methods=['POST'])
@handle_server_errors
def take_more_word(conn):
    '''오늘의 단어가 부족할때 추가로 가져오는 함수'''
    data = request.get_json()
    username = data.get('username')
    n = data.get('n')

    # 유효성 검사
    if not _username_check(conn, username):
        return jsonify({'message': 'ID not found'}), 400
    try:
        n = int(n)
    except:
        return jsonify({'message': 'number error'}), 400
    
    # n만큼 main테이블에서 number 순으로 status가 '-1'인 단어 가져오기
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT number, word FROM main 
            WHERE username = %s AND status = '-1'
            ORDER BY number ASC LIMIT %s
            """, (username, n)
        )
        today_word = [[number, word] for number, word in cursor.fetchall()]

    return jsonify({'add_word': today_word}), 200

@app.route('/take_category', methods=['GET'])
@handle_server_errors
def take_category(conn):
    '''단어 카테고리명을 반환하는 함수'''
    # 모든 테이블명 취득 후 word_가 붙은것만 리턴
    with conn.cursor() as cursor:
        cursor.execute("SHOW TABLES;")
        tables = [row[0] for row in cursor.fetchall()]
    word_tables = [t for t in tables if t.startswith('word_')]

    return jsonify({'word_tables': word_tables}), 200

@app.route('/signup', methods=['POST'])
@handle_server_errors
def signup(conn):
    '''회원가입 함수'''
    # 입력받은 데이터 저장
    data = request.get_json()
    username = data.get('username')
    language = data.get('language')
    dayword = data.get('dayword')
    category = data.get('category')

    # category를 add yourself로 선택하면 False
    temp =  False if category == 'add_yourself' else True
    
    # 데이터 유효성 검사
    if not username or username == 'Username' or len(username) > 15:
        return jsonify({'message': 'username error'}), 400
    if language not in ['K', 'J']:
        return jsonify({'message': 'language error'}), 400
    try:
        dayword = int(dayword)
    except:
        return jsonify({'message': 'dayword error'}), 400
    if not 10 <= dayword <= 25:
        return jsonify({'message': 'dayword error'}), 400
    if not _table_check(conn, category) and category != 'add_yourself':
        return jsonify({'message': 'category error'}), 400

    # username 중복 검사
    if _username_check(conn, username):
        return jsonify({'message': 'The ID already exists.'}), 400

    with conn.cursor() as cursor:
        # user 테이블에 데이터 삽입
        cursor.execute("""
            INSERT INTO user
            (username, language, dayword, category, created)
            VALUES (%s, %s, %s, %s, NOW())
            """, (username, language, dayword, category)
        )

        if temp:
            # main 테이블에 데이터 삽입
            cursor.execute(f"""
                INSERT INTO main (username, number, word, status)
                SELECT 
                    %s AS username,
                    (@rownum := @rownum + 1) AS number,
                    w.word,
                    '-1' AS status
                FROM (
                    SELECT word FROM {category} ORDER BY RAND()
                ) AS w, (SELECT @rownum := 0) AS r;
                """, (username,)
            )

    return jsonify({'message': 'Sign up successfully!'}), 201

@app.route('/delete_username_check', methods=['POST'])
@handle_server_errors
def delete_username_check(conn):
    '''계정삭제 시 username이 존재하는지 확인하는 함수'''
    data = request.get_json()
    username = data.get('username')
    exists = _username_check(conn, username)
    return jsonify({'message': exists}), 200

@app.route('/delaccount', methods=['DELETE'])
@handle_server_errors
def delaccount(conn):
    '''계정 삭제 함수'''
    # 입력받은 데이터 저장
    data = request.get_json()
    username = data.get('username')

    with conn.cursor() as cursor:
        # main테이블에서 유저정보 삭세
        cursor.execute("DELETE FROM main WHERE username = %s", (username,))
        # users테이블에서 유저정보 삭세
        cursor.execute("DELETE FROM user WHERE username = %s", (username,))
    
    return jsonify({'message': 'Deleted account'}), 200

# ==============================
# API 라우터 (프로그램 외부)
# ==============================
@app.route('/create_word_category', methods=['POST'])
@handle_server_errors
def create_word_category(conn):
    '''단어 테이블을 생성하는 함수'''
    # 데이터 취득
    data = request.get_json()
    table_name = data.get('table_name')
    words = data.get('words', [])

    # table_name, words 유효성 검사 
    # (존재해야하며 테이블명은 word_로 시작하고 그 이후는 알파벳으로 구성, 20자이내)
    if not table_name or not isinstance(words, list):
        return jsonify({'message': '잘못된 요청 데이터'}), 400
    if not re.fullmatch(r'word_[a-z]*', table_name) or len(table_name) > 20:
        return jsonify({'message': '잘못된 테이블명'}), 400
    
    # 단어테이블 작성
    with conn.cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                number INT PRIMARY KEY AUTO_INCREMENT,
                word VARCHAR(20)
            );
        """)
        cursor.executemany(
            f"INSERT INTO {table_name} (word) VALUES (%s)", [(w,) for w in words]
        )

    return jsonify({'message': f'{table_name} 테이블 생성 및 {len(words)}개 단어 삽입 완료'}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)