import os
import re
import logging
from functools import wraps
from datetime import datetime, timedelta

from dotenv import load_dotenv
from flask import Flask, request, jsonify
import pymysql

# ==============================
# 로깅, 설정 함수
# ==============================
def _get_db_logger():
    logger = logging.getLogger('db_wrapper')
    logger.setLevel(logging.ERROR)
    log_path = os.path.join(os.path.dirname(__file__), 'error.log')
    file_handler = logging.FileHandler(log_path)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

def _load_db_config():
    global host, user, password, name

    host = os.getenv('ROAD2_DB_HOST')
    user = os.getenv('ROAD2_DB_USER')
    password = os.getenv('ROAD2_DB_PASSWORD')
    name = os.getenv('ROAD2_DB_NAME')

# ==============================
# DB 관련 함수
# ==============================
def _get_connection():
    return pymysql.connect(host=host, user=user, password=password, database=name)

def _db_request_wrapper(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        conn = None
        try:
            with _get_connection() as conn:
                result = func(conn, *args, **kwargs)  # 원래 함수 실행
                conn.commit()
                return result

        except Exception as e:
            logger.error(f"Exception in {func.__name__}: {e}", exc_info=True)
            return jsonify({'message': 'Instance error.'}), 500

    return wrapper

def _is_user_exist(conn, username):
    with conn.cursor() as cursor:
        cursor.execute("SELECT username FROM user WHERE username = %s", (username,))
        return cursor.fetchone() is not None

def _is_table_exist(conn, table):
    with conn.cursor() as cursor:
        cursor.execute("SHOW TABLES LIKE %s;", (table,))
        return cursor.fetchone() is not None

# ==============================
# Flask 앱 초기화
# ==============================
app = Flask(__name__)

# ==============================
# check server and db
# ==============================
@app.route('/check_server_and_db', methods=['GET']) 
@_db_request_wrapper
def check_server_and_db(conn):
    with conn.cursor() as cursor:
        cursor.execute("SHOW TABLES LIKE 'user';")
        has_user = cursor.fetchone() is not None
        cursor.execute("SHOW TABLES LIKE 'main';")
        has_main = cursor.fetchone() is not None
        cursor.execute("SHOW TABLES LIKE 'record';")
        has_record = cursor.fetchone() is not None

        if not(has_user and has_main and has_record):
            return jsonify({'message': 'Missing core db tables.'}), 503

    return jsonify(), 200

# ==============================
# take category
# ==============================
@app.route('/take_category', methods=['GET'])
@_db_request_wrapper
def take_category(conn):
    with conn.cursor() as cursor:
        cursor.execute("SHOW TABLES;")
        tables = [row[0] for row in cursor.fetchall()]
    word_tables = [t for t in tables if t.startswith('word_')]

    return jsonify({'word_tables': word_tables}), 200

# ==============================
# sign up
# ==============================
@app.route('/sign_up', methods=['POST'])
@_db_request_wrapper
def sign_up(conn):
    data = request.get_json()
    username = data.get('username')
    language = data.get('language')
    dayword = data.get('dayword')
    category = data.get('category')

    is_add_yourself = True if category == 'add yourself' else False
    
    # 데이터 유효성 검사
    if not username or username == 'Username' or len(username) > 15:
        return jsonify({'message': 'username error.'}), 400
    if language not in ['K', 'J']:
        return jsonify({'message': 'language error.'}), 400
    try:
        dayword = int(dayword)
    except:
        return jsonify({'message': 'dayword error.'}), 400
    if not 10 <= dayword <= 25:
        return jsonify({'message': 'dayword error.'}), 400
    if not _is_table_exist(conn, category) and category != 'add yourself':
        return jsonify({'message': 'category error.'}), 400

    if _is_user_exist(conn, username):
        return jsonify({'message': 'The ID already exists.'}), 400

    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO user
            (username, language, dayword, category, created)
            VALUES (%s, %s, %s, %s, NOW())
            """, (username, language, dayword, category)
        )

        if not is_add_yourself:
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

# ==============================
# check user before delete
# ==============================
@app.route('/check_user_before_delete', methods=['POST'])
@_db_request_wrapper
def check_user_before_delete(conn):
    data = request.get_json()
    username = data.get('username')
    is_user_exist = _is_user_exist(conn, username)

    return jsonify({'message': is_user_exist}), 200

# ==============================
# delete account
# ==============================
@app.route('/delete_account', methods=['DELETE'])
@_db_request_wrapper
def delete_account(conn):
    data = request.get_json()
    username = data.get('username')

    if not _is_user_exist(conn, username):
        return jsonify({'message': 'ID not found.'}), 400

    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM main WHERE username = %s", (username,))
        cursor.execute("DELETE FROM user WHERE username = %s", (username,))
    return jsonify({'message': 'Deleted account.'}), 200
    
# ==============================
# login
# ==============================
@app.route('/login', methods=['POST']) 
@_db_request_wrapper
def login(conn):
    data = request.get_json()
    username = data.get('username')
    
    if not _is_user_exist(conn, username):
        return jsonify({'message': 'ID not found'}), 400

    with conn.cursor() as cursor:
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

        cursor.execute(
            "INSERT INTO record (username, start_time) VALUES (%s, NOW())",
            (username,)
        )

        # record 로부터 (연속로그인일수) 혹은 (최초가입) 혹은 (오늘과정 이미끝) 파악
        cursor.execute("""
            SELECT start_time, streak FROM record
            WHERE username = %s AND status = 'o'
            ORDER BY number DESC LIMIT 1
            """, (username,)
        )
        row = cursor.fetchone()

        # -2 : 최초로그인(로그인 기록이 없는 경우)
        check_streak = -2

        if row is not None:
            start_time, streak = row

            def adjusted_date(dt):
                '''오전 5시를 기준으로 하루를 계산하는 함수'''
                return dt.date() if dt.hour >= 5 else (dt - timedelta(days=1)).date()

            today = adjusted_date(datetime.now())
            last_login = adjusted_date(start_time)

            if last_login == today:
                # -1 : 오늘 과정 이미 완료(로그인 기록이 오늘인 경우)
                check_streak = -1

            elif last_login == today - timedelta(days=1):
                # 1 <= : 연속 로그인 성공(로그인기록이 어제인 경우)
                check_streak = streak
            else:
                # 0 : 연속 로그인 실패(로그인 기록이 어제보다 이전인 경우)
                check_streak = 0

        return jsonify({
            'language': language,
            'dayword': dayword,
            'category': category,
            'today_word': today_word,
            'streak': check_streak
        }), 201
        
# ==============================
# take more word
# ==============================
@app.route('/take_more_word', methods=['POST'])
@_db_request_wrapper
def take_more_word(conn):
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

    return jsonify({'today_word': today_word}), 200

# ==============================
# 프로그램 외부 호출 API 라우터
# ==============================
@app.route('/create_word_category', methods=['POST'])
@_db_request_wrapper
def create_word_category(conn):
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
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))
    _load_db_config()
    logger = _get_db_logger()
    app.run(host='0.0.0.0', port=5000)