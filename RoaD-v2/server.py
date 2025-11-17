import os
import re
import json
import logging
from functools import wraps
from datetime import date, datetime, timedelta

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
# DB 관련, 유틸 함수
# ==============================
def _adjusted_date(dt):
    '''오전 5시를 기준으로 하루를 계산하는 함수'''
    return dt.date() if dt.hour >= 5 else (dt - timedelta(days=1)).date()

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

def _is_word_in_main(conn, username, word_list):
    if not word_list:
        # already_know 형식이면 True, today_confirm 형식이면 False
        is_already_know = all(not isinstance(i, list) for i in word_list)
        return True if is_already_know else False

    if isinstance(word_list[0], list):
        word = [item[0] for item in word_list]
    else:
        word = word_list

    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT word FROM main WHERE username = %s
            AND word IN ({','.join(['%s'] * len(word))})
            """, [username] + word
        )
        rows = cursor.fetchall()

    db_word = {row[0] for row in rows}
    if isinstance(word_list[0], list):
        local_word = {w[0] for w in word_list}  # today_confirm 형식
    else:
        local_word = set(word_list)             # already_know, retry_word 형식
    return local_word.issubset(db_word)

def _get_due_list(conn, username, field):
    today = _adjusted_date(datetime.now())
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT word, mean, {field} FROM main
            WHERE username = %s AND status = 'progress'
            """, (username,)
        )
        rows = cursor.fetchall()

    result = []
    date_groups = {}

    for word, mean, field_json in rows:
        data = json.loads(field_json)
        plan = data.get('plan')
        plan_date = date.fromisoformat(plan)

        if plan_date <= today and 'done' not in data:
            date_groups.setdefault(plan, []).append([word, mean])

    for plan_date in sorted(date_groups.keys(), reverse=True):
        result.append(plan_date)
        result.extend(date_groups[plan_date])

    return result

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
                INSERT INTO main (username, number, word)
                SELECT 
                    %s AS username,
                    (@rownum := @rownum + 1) AS number,
                    w.word
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
            "SELECT language, dayword, category FROM user WHERE username = %s", (username, )
        )
        row = cursor.fetchone()
        language, dayword, category = row

        if category != 'add yourself':
            # 모든 단어가 status == finish인지 확인
            cursor.execute(
                "SELECT 1 FROM main WHERE username = %s AND status != 'finish' LIMIT 1", (username, )
            )
            result = cursor.fetchone()
            if not result:
                return jsonify({'message': 'finish'}), 204

            # status == retry인 단어 가져오기
            cursor.execute(
                "SELECT word FROM main WHERE username = %s AND status = 'retry'", (username, )
            )
            retry_word = [row[0] for row in cursor.fetchall()]
            
            # number 순으로 calculated_dayword 개수만큼, status가 yet인 단어 가져오기
            calculated_dayword = dayword - len(retry_word)
            if calculated_dayword > 0:
                cursor.execute("""
                    SELECT word FROM main WHERE username = %s AND status = 'yet'
                    ORDER BY number ASC LIMIT %s
                    """, (username, calculated_dayword)
                )
                yet_word = [row[0] for row in cursor.fetchall()]
                today_word = retry_word + yet_word
            else:
                today_word = retry_word
        else:
            today_word = []

        cursor.execute(
            "INSERT INTO record (username, start_time) VALUES (%s, NOW())", (username, )
        )

        # record 로부터 (연속로그인일수) 혹은 (최초가입) 혹은 (오늘과정 이미끝) 파악
        cursor.execute("""
            SELECT start_time, streak FROM record
            WHERE username = %s AND status = 'o'
            ORDER BY number DESC LIMIT 1
            """, (username, )
        )
        row = cursor.fetchone()

        if not row:
            check_streak = False # False : 최초로그인(로그인 기록이 없는 경우)
        else:
            start_time, streak = row

            today = _adjusted_date(datetime.now())
            last_login = _adjusted_date(start_time)

            if last_login == today:
                check_streak = True # True : 오늘 과정 이미 완료(로그인 기록이 오늘인 경우)
            elif last_login == today - timedelta(days=1):
                check_streak = streak + 1 # 며칠연속으로 로그인 했는지
            else:
                check_streak = -((today - last_login).days - 1) # 며칠연속으로 로그인 못했는지

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
    today_word = data.get('today_word')
    necessary = data.get('necessary')

    # 유효성 검사
    if not _is_user_exist(conn, username):
        return jsonify({'message': 'ID not found'}), 400
    try:
        necessary = int(necessary)
    except:
        return jsonify({'message': 'number error'}), 400

    placeholders = ','.join(['%s'] * len(today_word))
    
    # n만큼 main테이블에서 number 순으로 status가 yet 인 단어 가져오기
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT word FROM main
            WHERE username = %s AND status = 'yet' AND word NOT IN ({placeholders})
            ORDER BY number ASC LIMIT %s
            """, [username] + today_word + [necessary]
        )
        added_word = [word[0] for word in cursor.fetchall()]

    return jsonify({'added_word': added_word}), 200

# ==============================
# write today word
# ==============================
@app.route('/write_today_word', methods=['POST'])
@_db_request_wrapper
def write_today_word(conn):
    data = request.get_json()
    username = data.get('username')
    today_confirm = data.get('today_confirm')
    already_know = data.get('already_know')
    is_add_yourself = data.get('is_add_yourself')

    # 유효성 검사
    if not _is_user_exist(conn, username):
        return jsonify({'message': 'ID not found'}), 400
    if not isinstance(is_add_yourself, bool):
        return jsonify({'message': 'is_add_yourself error'}), 400
    if len(today_confirm) > 25:
        return jsonify({'message': 'today_confirm size error'}), 400
    if not is_add_yourself and not _is_word_in_main(conn, username, today_confirm):
        return jsonify({'message': 'today_confirm error'}), 400
    if is_add_yourself and len(today_confirm) != len({w[0] for w in today_confirm}):
        return jsonify({'message': 'today_confirm(add) error'}), 400
    if not is_add_yourself and not _is_word_in_main(conn, username, already_know):
        return jsonify({'message': 'already_know error'}), 400
    if is_add_yourself and already_know:
        return  jsonify({'message': 'already_know(add) error'}), 400

    # 오늘 날짜 및 반복 계획 JSON 구성
    today = _adjusted_date(datetime.now())
    plans = {
        'first':  (today + timedelta(days=1)).strftime('%Y-%m-%d'),
        'second': (today + timedelta(days=3)).strftime('%Y-%m-%d'),
        'third':  (today + timedelta(days=7)).strftime('%Y-%m-%d'),
        'fourth': (today + timedelta(days=14)).strftime('%Y-%m-%d'),
        'fifth':  (today + timedelta(days=28)).strftime('%Y-%m-%d')
    }

    with conn.cursor() as cursor:
        if not is_add_yourself:
            # today_confirm 처리
            for word, mean in today_confirm:
                cursor.execute("""
                    UPDATE main
                    SET mean = %s,
                        status = 'progress',
                        date_added = %s,
                        first  = JSON_OBJECT('plan', %s),
                        second = JSON_OBJECT('plan', %s),
                        third  = JSON_OBJECT('plan', %s),
                        fourth = JSON_OBJECT('plan', %s),
                        fifth  = JSON_OBJECT('plan', %s)
                    WHERE username = %s AND word = %s
                    """, (
                        mean,
                        today.strftime('%Y-%m-%d'),
                        plans['first'], plans['second'], plans['third'],
                        plans['fourth'], plans['fifth'],
                        username, word
                    )
                )

            # already_know 처리
            if already_know:
                cursor.execute(f"""
                    UPDATE main SET status = 'finish' WHERE username = %s
                    AND word IN ({','.join(['%s'] * len(already_know))})
                    """, [username] + already_know
                )
        else:
            # 현재 username의 최대 number 값 조회
            cursor.execute("SELECT MAX(number) FROM main WHERE username = %s", (username, ))
            result = cursor.fetchone()
            max_number = result[0] if result[0] is not None else 0  # 기존이 없으면 0부터 시작

            # today_confirm 처리
            for i, (word, mean) in enumerate(today_confirm, start=1):
                number = max_number + i
                cursor.execute("""
                    INSERT INTO main
                    (username, number, word, mean, status, date_added,
                    first, second, third, fourth, fifth)
                    VALUES (%s, %s, %s, %s, 'progress', %s,
                            JSON_OBJECT('plan', %s),
                            JSON_OBJECT('plan', %s),
                            JSON_OBJECT('plan', %s),
                            JSON_OBJECT('plan', %s),
                            JSON_OBJECT('plan', %s))
                    """, (username, number, word, mean, today.strftime('%Y-%m-%d'),
                        plans['first'], plans['second'], plans['third'],
                        plans['fourth'], plans['fifth']
                    )
                )

    return jsonify({'message': 'Successfully'}), 201

# ==============================
# get test data
# ==============================
@app.route('/get_test_data', methods=['POST'])
@_db_request_wrapper
def get_test_data(conn):
    data = request.get_json()
    username = data.get('username')

    # 유효성 검사
    if not _is_user_exist(conn, username):
        return jsonify({'message': 'ID not found'}), 400

    fields = ['first', 'second', 'third', 'fourth', 'fifth']
    result = {f: _get_due_list(conn, username, f) for f in fields}

    return jsonify(result), 200

# ==============================
# set retry word
# ==============================
@app.route('/set_retry_word', methods=['POST'])
@_db_request_wrapper
def set_retry_word(conn):
    data = request.get_json()
    username = data.get('username')
    retry_word_list = data.get('retry_word_list')

    if not retry_word_list:
        return jsonify({'message': 'retry_word_list is None'}), 200

    # 유효성 검사
    if not _is_user_exist(conn, username):
        return jsonify({'message': 'ID not found'}), 400
    if not _is_word_in_main(conn, username, retry_word_list):
        return jsonify({'message': 'retry_word_list error'}), 400

    with conn.cursor() as cursor:
        sql = f"""
            UPDATE main SET status = 'retry',
                mean = NULL, date_added = NULL, first = NULL, second = NULL, third = NULL, fourth = NULL, fifth = NULL
            WHERE username = %s AND word IN ({','.join(['%s'] * len(retry_word_list))})
        """
        cursor.execute(sql, [username] + retry_word_list)
    
    return jsonify({'message': 'set retry word successfully'}), 200

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
                word VARCHAR(20) UNIQUE NOT NULL
            );
        """)
        cursor.executemany(
            f"INSERT INTO {table_name} (word) VALUES (%s)", [(w,) for w in words]
        )

    return jsonify({'message': f'{table_name} 테이블 생성 및 {len(words)}개 단어 삽입 완료'}), 200

# ==============================
# 서버 실행
# ==============================
if __name__ == '__main__':
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))
    _load_db_config()
    logger = _get_db_logger()
    app.run(host='0.0.0.0', port=5000)