import os
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
import json
import io
import uuid
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, flash, abort, jsonify, Response
from flask_login import LoginManager, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from botocore.exceptions import BotoCoreError, ClientError
from botocore.config import Config as BotoConfig
import sqlite3
from datetime import datetime
import boto3
from PIL import Image

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-change-in-production')
DATABASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'shopping.db')

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message = 'Войдите, чтобы продолжить'


# ---- S3 config (avatars & images) ----
# Ожидаемые переменные окружения (можно S3-совместимые, не только AWS):
# - S3_BUCKET_NAME
# - S3_REGION (необязательно для некоторых S3-совместимых хранилищ)
# - S3_ENDPOINT_URL (необязательно; например, для MinIO/совместимых)
# - AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (или S3_ACCESS_KEY_ID / S3_SECRET_ACCESS_KEY)
# - S3_AVATAR_PREFIX (по умолчанию: avatars)
# - S3_PRESIGN_EXPIRES (по умолчанию: 3600 секунд)
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME') or os.environ.get('S3_BUCKET') or ''
S3_REGION = os.environ.get('S3_REGION') or os.environ.get('AWS_REGION') or os.environ.get('AWS_DEFAULT_REGION')
S3_ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL') or os.environ.get('AWS_ENDPOINT_URL')
S3_AVATAR_PREFIX = os.environ.get('S3_AVATAR_PREFIX', 'avatars')
S3_PRESIGN_EXPIRES = int(os.environ.get('S3_PRESIGN_EXPIRES', '3600'))


def s3_enabled() -> bool:
    return bool(S3_BUCKET_NAME)


def s3_credentials_configured() -> bool:
    ak = os.environ.get('S3_ACCESS_KEY_ID') or os.environ.get('AWS_ACCESS_KEY_ID')
    sk = os.environ.get('S3_SECRET_ACCESS_KEY') or os.environ.get('AWS_SECRET_ACCESS_KEY')
    return bool(ak and sk)


def get_s3_client():
    # Без явных ключей boto3 не должен «угадывать» на Render — там нет ~/.aws/credentials.
    if not s3_enabled():
        return None
    if not s3_credentials_configured():
        return None
    kwargs = {}
    if S3_REGION:
        kwargs['region_name'] = S3_REGION
    if S3_ENDPOINT_URL:
        kwargs['endpoint_url'] = S3_ENDPOINT_URL
        # Yandex Object Storage: path-style + v4 — стабильнее для put/get/presign
        if 'yandexcloud.net' in S3_ENDPOINT_URL.lower():
            kwargs['config'] = BotoConfig(
                signature_version='s3v4',
                s3={'addressing_style': 'path'},
            )

    access_key = os.environ.get('S3_ACCESS_KEY_ID') or os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('S3_SECRET_ACCESS_KEY') or os.environ.get('AWS_SECRET_ACCESS_KEY')
    kwargs['aws_access_key_id'] = access_key
    kwargs['aws_secret_access_key'] = secret_key

    return boto3.client('s3', **kwargs)


def avatar_url(avatar_key: str):
    if not avatar_key or not s3_enabled():
        return None
    client = get_s3_client()
    if not client:
        return None
    try:
        return client.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': avatar_key},
            ExpiresIn=S3_PRESIGN_EXPIRES,
        )
    except (BotoCoreError, ClientError):
        return None


@app.context_processor
def inject_template_helpers():
    return {'avatar_url': avatar_url}


def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys = ON')
    return conn


def log_pantry_activity(user_id, product_id, action, from_state=None, to_state=None, note=None):
    conn = get_db()
    conn.execute(
        'INSERT INTO pantry_activity (user_id, product_id, action, from_state, to_state, note) VALUES (?, ?, ?, ?, ?, ?)',
        (user_id, product_id, action, from_state, to_state, note)
    )
    conn.commit()
    conn.close()


def init_db():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            avatar_key TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS lists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            owner_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (owner_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS list_shares (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            list_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role TEXT DEFAULT 'editor',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(list_id, user_id),
            FOREIGN KEY (list_id) REFERENCES lists(id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            color TEXT DEFAULT '#6c757d'
        );
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            list_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            category_id INTEGER,
            quantity INTEGER DEFAULT 1,
            notes TEXT,
            bought INTEGER DEFAULT 0,
            bought_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (list_id) REFERENCES lists(id),
            FOREIGN KEY (category_id) REFERENCES categories(id)
        );
        CREATE TABLE IF NOT EXISTS list_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            items_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            category_id INTEGER,
            FOREIGN KEY (category_id) REFERENCES categories(id)
        );
        CREATE TABLE IF NOT EXISTS user_products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            state INTEGER NOT NULL DEFAULT 1,
            notes TEXT,
            UNIQUE(user_id, product_id),
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        );
        CREATE TABLE IF NOT EXISTS user_product_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_product_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            text TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_product_id) REFERENCES user_products(id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS pantry_activity (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            from_state INTEGER,
            to_state INTEGER,
            note TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        );
        CREATE TABLE IF NOT EXISTS user_settings (
            user_id INTEGER PRIMARY KEY,
            show_activity INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        INSERT OR IGNORE INTO categories (id, name, color) VALUES
            (1, 'Продукты', '#28a745'),
            (2, 'Бытовая химия', '#17a2b8'),
            (3, 'Молочное', '#ffc107'),
            (4, 'Овощи/Фрукты', '#fd7e14'),
            (5, 'Другое', '#6c757d');
        INSERT OR IGNORE INTO list_templates (id, name, items_json) VALUES
            (1, 'Продукты на неделю', '[{"name":"Молоко","cat":3,"qty":2},{"name":"Хлеб","cat":1,"qty":2},{"name":"Яйца","cat":1,"qty":1},{"name":"Сыр","cat":3,"qty":1},{"name":"Масло","cat":1,"qty":1},{"name":"Картофель","cat":4,"qty":1},{"name":"Морковь","cat":4,"qty":1},{"name":"Лук","cat":4,"qty":1},{"name":"Средство для мытья посуды","cat":2,"qty":1}]'),
            (2, 'Пикник', '[{"name":"Хлеб","cat":1,"qty":2},{"name":"Колбаса","cat":1,"qty":1},{"name":"Сыр","cat":3,"qty":1},{"name":"Огурцы","cat":4,"qty":2},{"name":"Помидоры","cat":4,"qty":2},{"name":"Напитки","cat":5,"qty":4},{"name":"Пакеты для мусора","cat":2,"qty":1}]'),
            (3, 'Домашняя уборка', '[{"name":"Средство для пола","cat":2,"qty":1},{"name":"Средство для окон","cat":2,"qty":1},{"name":"Губки","cat":2,"qty":2},{"name":"Мешки для мусора","cat":2,"qty":1},{"name":"Бумажные полотенца","cat":2,"qty":1}]');
        INSERT OR IGNORE INTO products (name, category_id) VALUES
            ('Молоко 3,2%', 3),
            ('Молоко 2,5%', 3),
            ('Кефир', 3),
            ('Ряженка', 3),
            ('Сметана 20%', 3),
            ('Сметана 15%', 3),
            ('Творог 5%', 3),
            ('Творог 9%', 3),
            ('Йогурт питьевой', 3),
            ('Йогурт густой', 3),
            ('Сыр российский', 3),
            ('Сыр голландский', 3),
            ('Сыр моцарелла', 3),
            ('Сыр плавленый', 3),
            ('Масло сливочное', 3),
            ('Маргарин', 1),
            ('Яйца куриные', 1),
            ('Хлеб белый', 1),
            ('Хлеб чёрный', 1),
            ('Батон нарезной', 1),
            ('Булочки', 1),
            ('Лаваш', 1),
            ('Макароны', 1),
            ('Спагетти', 1),
            ('Рис длиннозёрный', 1),
            ('Рис круглозёрный', 1),
            ('Гречка', 1),
            ('Овсяные хлопья', 1),
            ('Перловка', 1),
            ('Пшено', 1),
            ('Манка', 1),
            ('Сахар-песок', 1),
            ('Соль поваренная', 1),
            ('Перец чёрный молотый', 1),
            ('Мука пшеничная', 1),
            ('Мука ржаная', 1),
            ('Сода пищевая', 1),
            ('Разрыхлитель теста', 1),
            ('Дрожжи сухие', 1),
            ('Майонез', 1),
            ('Кетчуп', 1),
            ('Растительное масло подсолнечное', 1),
            ('Оливковое масло', 1),
            ('Уксус 9%', 1),
            ('Соевый соус', 1),
            ('Томатная паста', 1),
            ('Консервированная кукуруза', 1),
            ('Консервированный горошек', 1),
            ('Консервированные огурцы', 1),
            ('Консервированные помидоры', 1),
            ('Тушёнка говяжья', 1),
            ('Тушёнка свиная', 1),
            ('Сгущённое молоко', 3),
            ('Шоколад плиточный', 5),
            ('Конфеты шоколадные', 5),
            ('Печенье сахарное', 5),
            ('Печенье овсяное', 5),
            ('Вафли', 5),
            ('Пряники', 5),
            ('Зефир', 5),
            ('Мармелад', 5),
            ('Мёд', 5),
            ('Джем', 5),
            ('Варенье клубничное', 5),
            ('Варенье малиновое', 5),
            ('Чай чёрный', 5),
            ('Чай зелёный', 5),
            ('Кофе молотый', 5),
            ('Кофе растворимый', 5),
            ('Какао', 5),
            ('Минеральная вода', 5),
            ('Вода питьевая в бутылке', 5),
            ('Сок апельсиновый', 5),
            ('Сок яблочный', 5),
            ('Сок томатный', 5),
            ('Компот в бутылке', 5),
            ('Газированный напиток кола', 5),
            ('Газированный напиток лимонный', 5),
            ('Колбаса варёная', 1),
            ('Колбаса копчёная', 1),
            ('Сосиски', 1),
            ('Сардельки', 1),
            ('Ветчина', 1),
            ('Бекон', 1),
            ('Фарш говяжий', 1),
            ('Фарш свино-говяжий', 1),
            ('Курица целая', 1),
            ('Куриные бёдра', 1),
            ('Куриные крылья', 1),
            ('Куриное филе', 1),
            ('Индейка филе', 1),
            ('Свинина на кости', 1),
            ('Свинина мякоть', 1),
            ('Говядина мякоть', 1),
            ('Филе рыбы белой', 1),
            ('Филе лосося', 1),
            ('Сельдь солёная', 1),
            ('Шпроты в масле', 1),
            ('Крабовые палочки', 1),
            ('Креветки замороженные', 1),
            ('Пельмени', 1),
            ('Вареники с картошкой', 1),
            ('Вареники с творогом', 1),
            ('Пицца замороженная', 1),
            ('Овощная смесь замороженная', 4),
            ('Брокколи замороженная', 4),
            ('Цветная капуста замороженная', 4),
            ('Картофель', 4),
            ('Морковь', 4),
            ('Лук репчатый', 4),
            ('Чеснок', 4),
            ('Свёкла', 4),
            ('Капуста белокочанная', 4),
            ('Капуста пекинская', 4),
            ('Огурцы свежие', 4),
            ('Помидоры свежие', 4),
            ('Перец болгарский', 4),
            ('Кабачки', 4),
            ('Баклажаны', 4),
            ('Редис', 4),
            ('Зелёный лук', 4),
            ('Укроп свежий', 4),
            ('Петрушка свежая', 4),
            ('Салат листовой', 4),
            ('Шпинат', 4),
            ('Яблоки', 4),
            ('Груши', 4),
            ('Апельсины', 4),
            ('Мандарины', 4),
            ('Бананы', 4),
            ('Лимоны', 4),
            ('Лаймы', 4),
            ('Виноград', 4),
            ('Персики', 4),
            ('Нектарины', 4),
            ('Сливы', 4),
            ('Арбуз', 4),
            ('Дыня', 4),
            ('Киви', 4),
            ('Грейпфрут', 4),
            ('Замороженная клубника', 4),
            ('Замороженная малина', 4),
            ('Замороженная вишня', 4),
            ('Сахарная пудра', 5),
            ('Крахмал картофельный', 1),
            ('Крахмал кукурузный', 1),
            ('Майонез оливковый', 1),
            ('Соус барбекю', 1),
            ('Горчица', 1),
            ('Хрен столовый', 1),
            ('Аджика', 1),
            ('Хмели-сунели', 1),
            ('Приправа для курицы', 1),
            ('Приправа для рыбы', 1),
            ('Приправа для мяса', 1),
            ('Лавровый лист', 1),
            ('Гвоздика пряность', 1),
            ('Корица молотая', 1),
            ('Ванильный сахар', 1),
            ('Желе в порошке', 5),
            ('Желатин', 1),
            ('Кукурузные хлопья', 5),
            ('Мюсли', 5),
            ('Семечки подсолнечные', 5),
            ('Арахис жареный', 5),
            ('Фисташки солёные', 5),
            ('Чипсы картофельные', 5),
            ('Сухарики', 5),
            ('Попкорн для микроволновки', 5),
            ('Мороженое пломбир', 5),
            ('Мороженое эскимо', 5),
            ('Йогурт питьевой детский', 3),
            ('Детское пюре фруктовое', 5),
            ('Детское пюре овощное', 5),
            ('Смесь молочная детская', 3),
            ('Сок детский в коробке', 5),
            ('Пюре картофельное быстрого приготовления', 1),
            ('Лапша быстрого приготовления', 1),
            ('Крабовые чипсы', 5),
            ('Сырный соус', 1),
            ('Сметанный соус', 1),
            ('Хлебцы ржаные', 1),
            ('Хлебцы хрустящие', 1),
            ('Рулет бисквитный', 5),
            ('Торт бисквитный', 5),
            ('Пирожное картошка', 5),
            ('Круассаны упаковка', 5),
            ('Сухое печенье к чаю', 5),
            ('Сырок глазированный', 3),
            ('Ряженка детская', 3),
            ('Кефир детский', 3),
            ('Творожок детский', 3),
            ('Вода детская', 5),
            ('Средство для мытья посуды', 2),
            ('Губки кухонные', 2),
            ('Салфетки бумажные', 2),
            ('Полотенца бумажные', 2),
            ('Туалетная бумага', 2),
            ('Стиральный порошок', 2),
            ('Гель для стирки', 2),
            ('Кондиционер для белья', 2),
            ('Средство для мытья пола', 2),
            ('Средство для ванной', 2),
            ('Средство для туалета', 2),
            ('Средство для стёкол', 2),
            ('Чистящее средство универсальное', 2),
            ('Порошок чистящий', 2),
            ('Щётка для унитаза', 2),
            ('Перчатки резиновые', 2),
            ('Пакеты для мусора', 2),
            ('Мешки для мусора большие', 2),
            ('Фольга пищевая', 2),
            ('Пергамент для выпечки', 2),
            ('Плёнка пищевая', 2),
            ('Пакеты для заморозки', 2),
            ('Губка металлическая', 2),
            ('Освежитель воздуха', 2),
            ('Средство от накипи', 2),
            ('Салфетки влажные', 2),
            ('Салфетки для уборки микрофибра', 2),
            ('Щётка для пола', 2),
            ('Швабра', 2),
            ('Моющее средство для посудомоечной машины', 2),
            ('Соль для посудомоечной машины', 2),
            ('Ополаскиватель для посудомоечной машины', 2),
            ('Таблетки для ПММ', 2),
            ('Мыло жидкое', 2),
            ('Мыло кусковое', 2),
            ('Шампунь', 2),
            ('Бальзам для волос', 2),
            ('Гель для душа', 2),
            ('Пена для бритья', 2),
            ('Крем для бритья', 2),
            ('Лезвия для бритвы', 2),
            ('Зубная паста', 2),
            ('Зубные щётки', 2),
            ('Зубная нить', 2),
            ('Ополаскиватель для рта', 2),
            ('Дезодорант мужской', 2),
            ('Дезодорант женский', 2),
            ('Крем для рук', 2),
            ('Крем для лица', 2),
            ('Ватные палочки', 2),
            ('Ватные диски', 2),
            ('Средство для снятия макияжа', 2),
            ('Прокладки женские', 2),
            ('Тампоны', 2),
            ('Подгузники детские', 2),
            ('Салфетки детские влажные', 2),
            ('Мусорные пакеты с завязками', 2),
            ('Освежитель для холодильника', 2),
            ('Средство для мытья кухни', 2),
            ('Средство от плесени', 2),
            ('Средство от насекомых', 2),
            ('Батарейки AA', 5),
            ('Батарейки AAA', 5),
            ('Лампочка светодиодная', 5),
            ('Спички', 5),
            ('Зажигалка', 5),
            ('Фильтры для воды', 5),
            ('Пакеты фасовочные', 2),
            ('Одноразовые тарелки', 2),
            ('Одноразовые стаканчики', 2),
            ('Одноразовые вилки и ложки', 2),
            ('Скрепки канцелярские', 5),
            ('Пакетики для завтрака', 2),
            ('Полоски для чистки унитаза', 2),
            ('Освежитель для обуви', 2);
    ''')
    # Migration for existing DBs
    cols = [r['name'] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
    if 'avatar_key' not in cols:
        conn.execute('ALTER TABLE users ADD COLUMN avatar_key TEXT')
    conn.commit()
    conn.close()


# Ensure schema exists even when running under gunicorn (Render)
init_db()


def ensure_default_admin():
    """Тестовый пользователь admin / admin (создаётся один раз, если нет записи с таким логином)."""
    conn = get_db()
    exists = conn.execute('SELECT 1 FROM users WHERE username = ?', ('admin',)).fetchone()
    if not exists:
        conn.execute(
            'INSERT INTO users (username, email, password_hash) VALUES (?, ?, ?)',
            ('admin', 'admin@localhost', generate_password_hash('admin')),
        )
        conn.commit()
    conn.close()


ensure_default_admin()


class User:
    def __init__(self, id, username, email, password_hash, avatar_key=None):
        self.id = id
        self.username = username
        self.email = email
        self.password_hash = password_hash
        self.avatar_key = avatar_key
        self.is_authenticated = True
        self.is_active = True
        self.is_anonymous = False

    def get_id(self):
        return str(self.id)


@login_manager.user_loader
def load_user(user_id):
    conn = get_db()
    row = conn.execute('SELECT * FROM users WHERE id = ?', (user_id,)).fetchone()
    conn.close()
    if row:
        return User(row['id'], row['username'], row['email'], row['password_hash'], row['avatar_key'])
    return None


def list_access(role='viewer'):
    def decorator(f):
        @wraps(f)
        def inner(*args, **kwargs):
            if not current_user.is_authenticated:
                return redirect(url_for('login'))
            list_id = kwargs.get('list_id')
            conn = get_db()
            lst = conn.execute('SELECT * FROM lists WHERE id = ?', (list_id,)).fetchone()
            if not lst:
                conn.close()
                abort(404)
            is_owner = lst['owner_id'] == current_user.id
            share = conn.execute(
                'SELECT * FROM list_shares WHERE list_id = ? AND user_id = ?',
                (list_id, current_user.id)
            ).fetchone()
            conn.close()
            if not is_owner and not share:
                abort(403)
            if role == 'editor' and not is_owner and (not share or share['role'] != 'editor'):
                abort(403)
            return f(*args, **kwargs)
        return inner
    return decorator


# === Auth ===
@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('my_profile'))
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        email = request.form.get('email', '').strip()
        password = request.form.get('password', '')
        if not username or not email or not password:
            flash('Заполните все поля', 'danger')
            return render_template('register.html')
        if len(password) < 4:
            flash('Пароль минимум 4 символа', 'danger')
            return render_template('register.html')
        conn = get_db()
        try:
            conn.execute(
                'INSERT INTO users (username, email, password_hash) VALUES (?, ?, ?)',
                (username, email, generate_password_hash(password))
            )
            conn.commit()
            flash('Регистрация успешна. Войдите.', 'success')
            return redirect(url_for('login'))
        except sqlite3.IntegrityError:
            flash('Пользователь или email уже существует', 'danger')
        finally:
            conn.close()
    return render_template('register.html')


@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('my_profile'))
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        conn = get_db()
        user_row = conn.execute(
            'SELECT * FROM users WHERE username = ? OR email = ?',
            (username, username)
        ).fetchone()
        conn.close()
        if user_row and check_password_hash(user_row['password_hash'], password):
            user = User(user_row['id'], user_row['username'], user_row['email'], user_row['password_hash'], user_row['avatar_key'])
            login_user(user)
            flash(f'Вход выполнен', 'success')
            return redirect(url_for('my_profile'))
        flash('Неверный логин или пароль', 'danger')
    return render_template('login.html')


@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('Вы вышли', 'info')
    return redirect(url_for('login'))


# === Dashboard ===
@app.route('/')
@login_required
def index():
    conn = get_db()
    my_lists = conn.execute('''
        SELECT l.*, u.username as owner_name,
               (SELECT COUNT(*) FROM items WHERE list_id = l.id AND bought = 0) as pending
        FROM lists l
        JOIN users u ON l.owner_id = u.id
        WHERE l.owner_id = ?
        ORDER BY l.created_at DESC
    ''', (current_user.id,)).fetchall()
    shared_lists = conn.execute('''
        SELECT l.*, u.username as owner_name, ls.role,
               (SELECT COUNT(*) FROM items WHERE list_id = l.id AND bought = 0) as pending
        FROM lists l
        JOIN list_shares ls ON l.id = ls.list_id
        JOIN users u ON l.owner_id = u.id
        WHERE ls.user_id = ?
        ORDER BY l.created_at DESC
    ''', (current_user.id,)).fetchall()
    conn.close()
    return render_template('index.html', my_lists=my_lists, shared_lists=shared_lists)


# === Lists ===
@app.route('/list/new', methods=['GET', 'POST'])
@login_required
def list_new():
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        if name:
            conn = get_db()
            conn.execute('INSERT INTO lists (name, owner_id) VALUES (?, ?)', (name, current_user.id))
            conn.commit()
            list_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
            conn.close()
            flash('Список создан', 'success')
            return redirect(url_for('list_view', list_id=list_id))
        flash('Введите название', 'danger')
    return render_template('list_new.html')


@app.route('/list/<int:list_id>')
@login_required
@list_access('viewer')
def list_view(list_id):
    conn = get_db()
    lst = conn.execute('SELECT l.*, u.username as owner_name FROM lists l JOIN users u ON l.owner_id = u.id WHERE l.id = ?', (list_id,)).fetchone()
    items = conn.execute('''
        SELECT i.*, c.name as category_name, c.color as category_color
        FROM items i
        LEFT JOIN categories c ON i.category_id = c.id
        WHERE i.list_id = ?
        ORDER BY i.bought, c.name, i.name
    ''', (list_id,)).fetchall()
    categories = conn.execute('SELECT * FROM categories ORDER BY name').fetchall()
    shares = conn.execute('''
        SELECT ls.*, u.username FROM list_shares ls
        JOIN users u ON ls.user_id = u.id
        WHERE ls.list_id = ?
    ''', (list_id,)).fetchall()
    is_owner = lst['owner_id'] == current_user.id
    my_share = conn.execute('SELECT role FROM list_shares WHERE list_id = ? AND user_id = ?', (list_id, current_user.id)).fetchone()
    can_edit = is_owner or (my_share and my_share['role'] == 'editor')
    conn.close()
    return render_template('list_view.html', lst=lst, items=items, categories=categories, shares=shares, is_owner=is_owner, can_edit=can_edit)


@app.route('/list/<int:list_id>/edit', methods=['GET', 'POST'])
@login_required
@list_access('editor')
def list_edit(list_id):
    conn = get_db()
    lst = conn.execute('SELECT * FROM lists WHERE id = ?', (list_id,)).fetchone()
    conn.close()
    if not lst:
        abort(404)
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        if name:
            conn = get_db()
            conn.execute('UPDATE lists SET name = ? WHERE id = ?', (name, list_id))
            conn.commit()
            conn.close()
            flash('Список обновлён', 'success')
            return redirect(url_for('list_view', list_id=list_id))
        flash('Введите название', 'danger')
    return render_template('list_edit.html', lst=lst)


@app.route('/list/<int:list_id>/delete', methods=['POST'])
@login_required
@list_access('editor')
def list_delete(list_id):
    conn = get_db()
    conn.execute('DELETE FROM items WHERE list_id = ?', (list_id,))
    conn.execute('DELETE FROM list_shares WHERE list_id = ?', (list_id,))
    conn.execute('DELETE FROM lists WHERE id = ?', (list_id,))
    conn.commit()
    conn.close()
    flash('Список удалён', 'info')
    return redirect(url_for('index'))


@app.route('/list/<int:list_id>/share', methods=['GET', 'POST'])
@login_required
@list_access('editor')
def list_share(list_id):
    conn = get_db()
    lst = conn.execute('SELECT * FROM lists WHERE id = ?', (list_id,)).fetchone()
    shares = conn.execute('''
        SELECT ls.*, u.username FROM list_shares ls
        JOIN users u ON ls.user_id = u.id
        WHERE ls.list_id = ?
    ''', (list_id,)).fetchall()
    conn.close()
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        if username:
            conn = get_db()
            target = conn.execute('SELECT id FROM users WHERE username = ? OR email = ?', (username, username)).fetchone()
            if not target:
                conn.close()
                flash('Пользователь не найден', 'danger')
                return render_template('list_share.html', lst=lst, shares=shares)
            if target['id'] == current_user.id:
                conn.close()
                flash('Нельзя поделиться с собой', 'danger')
                return render_template('list_share.html', lst=lst, shares=shares)
            try:
                conn.execute('INSERT INTO list_shares (list_id, user_id, role) VALUES (?, ?, ?)', (list_id, target['id'], 'editor'))
                conn.commit()
                flash('Доступ предоставлен', 'success')
                return redirect(url_for('list_view', list_id=list_id))
            except sqlite3.IntegrityError:
                flash('Пользователь уже имеет доступ', 'warning')
            finally:
                conn.close()
        else:
            flash('Введите имя пользователя', 'danger')
    return render_template('list_share.html', lst=lst, shares=shares)


@app.route('/list/<int:list_id>/unshare/<int:user_id>', methods=['POST'])
@login_required
@list_access('editor')
def list_unshare(list_id, user_id):
    conn = get_db()
    conn.execute('DELETE FROM list_shares WHERE list_id = ? AND user_id = ?', (list_id, user_id))
    conn.commit()
    conn.close()
    flash('Доступ отозван', 'info')
    return redirect(url_for('list_share', list_id=list_id))


# === Items ===
@app.route('/list/<int:list_id>/add', methods=['POST'])
@login_required
@list_access('editor')
def item_add(list_id):
    name = request.form.get('name', '').strip()
    category_id = request.form.get('category_id') or None
    quantity = request.form.get('quantity', '1')
    notes = request.form.get('notes', '').strip()
    if name:
        try:
            qty = int(quantity) if quantity else 1
        except ValueError:
            qty = 1
        conn = get_db()
        conn.execute(
            'INSERT INTO items (list_id, name, category_id, quantity, notes) VALUES (?, ?, ?, ?, ?)',
            (list_id, name, category_id, qty, notes)
        )
        conn.commit()
        conn.close()
        flash('Товар добавлен', 'success')
    return redirect(url_for('list_view', list_id=list_id))


@app.route('/list/<int:list_id>/toggle/<int:item_id>')
@login_required
@list_access('editor')
def item_toggle(list_id, item_id):
    conn = get_db()
    row = conn.execute('SELECT bought FROM items WHERE id = ? AND list_id = ?', (item_id, list_id)).fetchone()
    if row:
        new_val = 0 if row['bought'] else 1
        bought_at = datetime.utcnow().isoformat() if new_val else None
        conn.execute('UPDATE items SET bought = ?, bought_at = ? WHERE id = ?', (new_val, bought_at, item_id))
        conn.commit()
    conn.close()
    return redirect(url_for('list_view', list_id=list_id))


@app.route('/list/<int:list_id>/delete/<int:item_id>', methods=['POST'])
@login_required
@list_access('editor')
def item_delete(list_id, item_id):
    conn = get_db()
    conn.execute('DELETE FROM items WHERE id = ? AND list_id = ?', (item_id, list_id))
    conn.commit()
    conn.close()
    return redirect(url_for('list_view', list_id=list_id))


@app.route('/list/<int:list_id>/clear-bought', methods=['POST'])
@login_required
@list_access('editor')
def clear_bought(list_id):
    conn = get_db()
    conn.execute('DELETE FROM items WHERE list_id = ? AND bought = 1', (list_id,))
    conn.commit()
    conn.close()
    flash('Купленные товары удалены', 'info')
    return redirect(url_for('list_view', list_id=list_id))


@app.route('/list/<int:list_id>/duplicate', methods=['POST'])
@login_required
@list_access('viewer')
def list_duplicate(list_id):
    conn = get_db()
    lst = conn.execute('SELECT * FROM lists WHERE id = ?', (list_id,)).fetchone()
    items = conn.execute('SELECT name, category_id, quantity, notes FROM items WHERE list_id = ? AND bought = 0', (list_id,)).fetchall()
    conn.execute('INSERT INTO lists (name, owner_id) VALUES (?, ?)', (lst['name'] + ' (копия)', current_user.id))
    new_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    for it in items:
        conn.execute('INSERT INTO items (list_id, name, category_id, quantity, notes) VALUES (?, ?, ?, ?, ?)',
                     (new_id, it['name'], it['category_id'], it['quantity'], it['notes']))
    conn.commit()
    conn.close()
    flash('Список скопирован', 'success')
    return redirect(url_for('list_view', list_id=new_id))


@app.route('/list/<int:list_id>/save-template', methods=['POST'])
@login_required
@list_access('viewer')
def list_save_template(list_id):
    name = request.form.get('template_name', '').strip()
    if not name:
        name = 'Шаблон списка'
    conn = get_db()
    items = conn.execute('SELECT name, category_id as cat, quantity as qty, notes FROM items WHERE list_id = ? AND bought = 0',
                         (list_id,)).fetchall()
    if not items:
        conn.close()
        flash('Нечего сохранять в шаблон (нет активных товаров)', 'warning')
        return redirect(url_for('list_view', list_id=list_id))
    items_payload = []
    for it in items:
        items_payload.append({
            'name': it['name'],
            'cat': it['cat'],
            'qty': it['qty'],
            'notes': it['notes'] or ''
        })
    conn.execute(
        'INSERT INTO list_templates (name, items_json) VALUES (?, ?)',
        (name, json.dumps(items_payload, ensure_ascii=False))
    )
    conn.commit()
    conn.close()
    flash('Шаблон списка сохранён', 'success')
    return redirect(url_for('list_view', list_id=list_id))


@app.route('/list/new/from-template/<int:template_id>', methods=['GET', 'POST'])
@login_required
def list_from_template(template_id):
    conn = get_db()
    tpl = conn.execute('SELECT * FROM list_templates WHERE id = ?', (template_id,)).fetchone()
    conn.close()
    if not tpl:
        abort(404)
    if request.method == 'POST':
        name = request.form.get('name', tpl['name']).strip() or tpl['name']
        conn = get_db()
        conn.execute('INSERT INTO lists (name, owner_id) VALUES (?, ?)', (name, current_user.id))
        new_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        items = json.loads(tpl['items_json'])
        for it in items:
            conn.execute('INSERT INTO items (list_id, name, category_id, quantity, notes) VALUES (?, ?, ?, ?, ?)',
                         (new_id, it['name'], it.get('cat'), it.get('qty', 1), it.get('notes', '')))
        conn.commit()
        conn.close()
        flash('Список создан из шаблона', 'success')
        return redirect(url_for('list_view', list_id=new_id))
    template_items = json.loads(tpl['items_json'])
    return render_template('list_from_template.html', template=tpl, template_items=template_items)


@app.route('/search')
@login_required
def search():
    q = request.args.get('q', '').strip()
    conn = get_db()
    suggestions = conn.execute('''
        SELECT DISTINCT i.name
        FROM items i
        JOIN lists l ON i.list_id = l.id
        WHERE (l.owner_id = ? OR EXISTS (SELECT 1 FROM list_shares WHERE list_id = l.id AND user_id = ?))
        ORDER BY i.name
        LIMIT 200
    ''', (current_user.id, current_user.id)).fetchall()
    results = []
    if q:
        q_pattern = f'%{q}%'
        results = conn.execute('''
            SELECT i.*, l.name as list_name, l.id as list_id, c.name as category_name
            FROM items i
            JOIN lists l ON i.list_id = l.id
            LEFT JOIN categories c ON i.category_id = c.id
            WHERE (i.name LIKE ? OR i.notes LIKE ?)
            AND (l.owner_id = ? OR EXISTS (SELECT 1 FROM list_shares WHERE list_id = l.id AND user_id = ?))
            ORDER BY l.name, i.name
        ''', (q_pattern, q_pattern, current_user.id, current_user.id)).fetchall()
    conn.close()
    return render_template('search.html', results=results, query=q, suggestions=suggestions)


@app.route('/list/<int:list_id>/export')
@login_required
@list_access('viewer')
def list_export(list_id):
    fmt = request.args.get('format', 'txt')
    conn = get_db()
    lst = conn.execute('SELECT * FROM lists WHERE id = ?', (list_id,)).fetchone()
    items = conn.execute('''
        SELECT i.*, c.name as cat_name FROM items i
        LEFT JOIN categories c ON i.category_id = c.id
        WHERE i.list_id = ? ORDER BY i.bought, c.name, i.name
    ''', (list_id,)).fetchall()
    conn.close()
    if fmt == 'json':
        data = {'name': lst['name'], 'items': [{'name': i['name'], 'quantity': i['quantity'], 'notes': i['notes'], 'category': i['cat_name'], 'bought': bool(i['bought'])} for i in items]}
        return Response(json.dumps(data, ensure_ascii=False, indent=2), mimetype='application/json',
                        headers={'Content-Disposition': f'attachment; filename="{lst["name"]}.json"'})
    lines = [lst['name'], '=' * 40]
    for i in items:
        line = f"- {i['name']}"
        if i['quantity'] > 1:
            line += f" x{i['quantity']}"
        if i['notes']:
            line += f" ({i['notes']})"
        if i['bought']:
            line += " [куплено]"
        lines.append(line)
    return Response('\n'.join(lines), mimetype='text/plain; charset=utf-8',
                   headers={'Content-Disposition': f'attachment; filename="{lst["name"]}.txt"'})


@app.route('/list/<int:list_id>/print')
@login_required
@list_access('viewer')
def list_print(list_id):
    conn = get_db()
    lst = conn.execute('SELECT l.*, u.username as owner_name FROM lists l JOIN users u ON l.owner_id = u.id WHERE l.id = ?', (list_id,)).fetchone()
    items = conn.execute('''
        SELECT i.*, c.name as category_name FROM items i
        LEFT JOIN categories c ON i.category_id = c.id
        WHERE i.list_id = ? AND bought = 0
        ORDER BY c.name, i.name
    ''', (list_id,)).fetchall()
    conn.close()
    return render_template('list_print.html', lst=lst, items=items, now=datetime.now())




@app.route('/settings', methods=['GET', 'POST'])
@login_required
def settings():
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'password':
            current = request.form.get('current_password', '')
            new_pass = request.form.get('new_password', '')
            if len(new_pass) < 4:
                flash('Новый пароль минимум 4 символа', 'danger')
                return redirect(url_for('settings'))
            conn = get_db()
            user = conn.execute('SELECT password_hash FROM users WHERE id = ?', (current_user.id,)).fetchone()
            if not user or not check_password_hash(user['password_hash'], current):
                conn.close()
                flash('Неверный текущий пароль', 'danger')
                return redirect(url_for('settings'))
            conn.execute('UPDATE users SET password_hash = ? WHERE id = ?',
                         (generate_password_hash(new_pass), current_user.id))
            conn.commit()
            conn.close()
            flash('Пароль изменён', 'success')
        elif action == 'email':
            email = request.form.get('email', '').strip()
            if email:
                conn = get_db()
                try:
                    conn.execute('UPDATE users SET email = ? WHERE id = ?', (email, current_user.id))
                    conn.commit()
                    flash('Email обновлён', 'success')
                except sqlite3.IntegrityError:
                    flash('Email уже занят', 'danger')
                conn.close()
        elif action == 'avatar':
            f = request.files.get('avatar')
            if not f or not f.filename:
                flash('Выберите файл', 'danger')
                return redirect(url_for('settings'))
            if not f.mimetype or not f.mimetype.startswith('image/'):
                flash('Загрузите изображение', 'danger')
                return redirect(url_for('settings'))
            if not s3_enabled():
                flash('S3 не настроен. Добавьте переменные окружения для подключения.', 'danger')
                return redirect(url_for('settings'))
            if not s3_credentials_configured():
                flash(
                    'Не заданы ключи S3. На Render: Environment → Environment Variables → '
                    'добавьте S3_ACCESS_KEY_ID и S3_SECRET_ACCESS_KEY (или AWS_ACCESS_KEY_ID и AWS_SECRET_ACCESS_KEY). '
                    'Файл .env локально на сервер не попадает.',
                    'danger',
                )
                return redirect(url_for('settings'))

            client = get_s3_client()
            if not client:
                flash('Не удалось инициализировать S3 клиент', 'danger')
                return redirect(url_for('settings'))

            try:
                # Сжимаем до 256x256 и сохраняем как JPEG (универсальный формат)
                img = Image.open(f.stream)
                img = img.convert('RGB')
                # Достаточно пикселей для шапки/профиля на Retina без «мыла»
                try:
                    _resample = Image.Resampling.LANCZOS
                except AttributeError:
                    _resample = Image.LANCZOS
                img.thumbnail((768, 768), _resample)
                buf = io.BytesIO()
                img.save(buf, format='JPEG', quality=92, optimize=True, subsampling=0)
                buf.seek(0)
            except Exception:
                flash('Не удалось обработать изображение', 'danger')
                return redirect(url_for('settings'))

            key = f"{S3_AVATAR_PREFIX}/{current_user.id}/{uuid.uuid4().hex}.jpg"
            try:
                client.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=key,
                    Body=buf,
                    ContentType='image/jpeg',
                )
            except (BotoCoreError, ClientError) as e:
                flash(f'Ошибка загрузки аватара в S3: {e}', 'danger')
                return redirect(url_for('settings'))

            # Удаляем старый аватар, чтобы не копились объекты
            old_key = getattr(current_user, 'avatar_key', None)
            if old_key:
                try:
                    client.delete_object(Bucket=S3_BUCKET_NAME, Key=old_key)
                except Exception:
                    pass

            conn = get_db()
            conn.execute('UPDATE users SET avatar_key = ? WHERE id = ?', (key, current_user.id))
            conn.commit()
            conn.close()
            flash('Аватар обновлён', 'success')
        return redirect(url_for('settings'))
    conn = get_db()
    user = conn.execute('SELECT * FROM users WHERE id = ?', (current_user.id,)).fetchone()
    conn.close()
    return render_template('settings.html', user=user)


def _avatar_response_for_user(user_id: int):
    """Отдаёт файл аватара из S3 для указанного пользователя."""
    conn = get_db()
    row = conn.execute('SELECT avatar_key FROM users WHERE id = ?', (user_id,)).fetchone()
    conn.close()
    key = row['avatar_key'] if row else None
    if not key or not s3_enabled():
        abort(404)
    client = get_s3_client()
    if not client:
        abort(503)
    try:
        obj = client.get_object(Bucket=S3_BUCKET_NAME, Key=key)
        body = obj['Body'].read()
        ct = obj.get('ContentType') or 'image/jpeg'
        resp = Response(body, mimetype=ct)
        resp.headers['Cache-Control'] = 'private, max-age=300'
        return resp
    except (BotoCoreError, ClientError):
        abort(404)


@app.route('/avatar/me')
@login_required
def avatar_me():
    """Аватар текущего пользователя (для шапки и настроек)."""
    return _avatar_response_for_user(current_user.id)


@app.route('/avatar/<int:user_id>')
@login_required
def avatar_user(user_id):
    """Аватар пользователя по id (профиль и просмотр)."""
    return _avatar_response_for_user(user_id)


@app.route('/me/profile', methods=['GET', 'POST'])
@login_required
def my_profile():
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'privacy':
            show = 1 if request.form.get('show_activity') == '1' else 0
            conn = get_db()
            conn.execute(
                'INSERT INTO user_settings (user_id, show_activity) VALUES (?, ?) '
                'ON CONFLICT(user_id) DO UPDATE SET show_activity = excluded.show_activity',
                (current_user.id, show)
            )
            conn.commit()
            conn.close()
            flash('Настройки приватности обновлены', 'success')
        return redirect(url_for('my_profile'))
    conn = get_db()
    user = conn.execute('SELECT * FROM users WHERE id = ?', (current_user.id,)).fetchone()
    settings_row = conn.execute('SELECT show_activity FROM user_settings WHERE user_id = ?', (current_user.id,)).fetchone()
    show_activity = settings_row['show_activity'] == 1 if settings_row else False
    activity = conn.execute('''
        SELECT a.*, p.name as product_name
        FROM pantry_activity a
        JOIN products p ON a.product_id = p.id
        WHERE a.user_id = ?
        ORDER BY a.created_at DESC
        LIMIT 200
    ''', (current_user.id,)).fetchall()
    total_items = conn.execute('''
        SELECT COUNT(*) as c FROM items i
        JOIN lists l ON i.list_id = l.id
        WHERE (l.owner_id = ? OR EXISTS (SELECT 1 FROM list_shares WHERE list_id = l.id AND user_id = ?))
    ''', (current_user.id, current_user.id)).fetchone()['c']
    bought_count = conn.execute('''
        SELECT COUNT(*) as c FROM items i
        JOIN lists l ON i.list_id = l.id
        WHERE i.bought = 1 AND (l.owner_id = ? OR EXISTS (SELECT 1 FROM list_shares WHERE list_id = l.id AND user_id = ?))
    ''', (current_user.id, current_user.id)).fetchone()['c']
    by_category = conn.execute('''
        SELECT c.name, COUNT(*) as cnt FROM items i
        JOIN lists l ON i.list_id = l.id
        LEFT JOIN categories c ON i.category_id = c.id
        WHERE (l.owner_id = ? OR EXISTS (SELECT 1 FROM list_shares WHERE list_id = l.id AND user_id = ?))
        GROUP BY c.name
        ORDER BY cnt DESC
    ''', (current_user.id, current_user.id)).fetchall()
    pantry_totals = conn.execute('''
        SELECT
            SUM(CASE WHEN up.state = 1 THEN 1 ELSE 0 END) as in_stock,
            SUM(CASE WHEN up.state = 2 THEN 1 ELSE 0 END) as low,
            SUM(CASE WHEN up.state = 3 THEN 1 ELSE 0 END) as none
        FROM user_products up
        WHERE up.user_id = ?
    ''', (current_user.id,)).fetchone()
    history_items = conn.execute('''
        SELECT i.name, i.quantity, i.bought_at, l.name as list_name, l.id as list_id
        FROM items i
        JOIN lists l ON i.list_id = l.id
        WHERE i.bought = 1 AND (l.owner_id = ? OR EXISTS (SELECT 1 FROM list_shares WHERE list_id = l.id AND user_id = ?))
        ORDER BY i.bought_at DESC
        LIMIT 100
    ''', (current_user.id, current_user.id)).fetchall()
    conn.close()
    return render_template('profile.html', user=user, is_owner=True, show_activity=show_activity,
                           can_view_activity=True, activity=activity,
                           total_items=total_items, bought_count=bought_count, by_category=by_category,
                           pantry_totals=pantry_totals, history_items=history_items)


@app.route('/user/<username>/profile')
@login_required
def user_profile(username):
    conn = get_db()
    user = conn.execute('SELECT * FROM users WHERE username = ?', (username,)).fetchone()
    if not user:
        conn.close()
        abort(404)
    settings_row = conn.execute('SELECT show_activity FROM user_settings WHERE user_id = ?', (user['id'],)).fetchone()
    show_activity = settings_row['show_activity'] == 1 if settings_row else False
    can_view = (user['id'] == current_user.id) or show_activity
    activity = []
    if can_view:
        activity = conn.execute('''
            SELECT a.*, p.name as product_name
            FROM pantry_activity a
            JOIN products p ON a.product_id = p.id
            WHERE a.user_id = ?
            ORDER BY a.created_at DESC
            LIMIT 200
        ''', (user['id'],)).fetchall()
    total_items = conn.execute('''
        SELECT COUNT(*) as c FROM items i
        JOIN lists l ON i.list_id = l.id
        WHERE (l.owner_id = ? OR EXISTS (SELECT 1 FROM list_shares WHERE list_id = l.id AND user_id = ?))
    ''', (user['id'], user['id'])).fetchone()['c']
    bought_count = conn.execute('''
        SELECT COUNT(*) as c FROM items i
        JOIN lists l ON i.list_id = l.id
        WHERE i.bought = 1 AND (l.owner_id = ? OR EXISTS (SELECT 1 FROM list_shares WHERE list_id = l.id AND user_id = ?))
    ''', (user['id'], user['id'])).fetchone()['c']
    by_category = conn.execute('''
        SELECT c.name, COUNT(*) as cnt FROM items i
        JOIN lists l ON i.list_id = l.id
        LEFT JOIN categories c ON i.category_id = c.id
        WHERE (l.owner_id = ? OR EXISTS (SELECT 1 FROM list_shares WHERE list_id = l.id AND user_id = ?))
        GROUP BY c.name
        ORDER BY cnt DESC
    ''', (user['id'], user['id'])).fetchall()
    pantry_totals = conn.execute('''
        SELECT
            SUM(CASE WHEN up.state = 1 THEN 1 ELSE 0 END) as in_stock,
            SUM(CASE WHEN up.state = 2 THEN 1 ELSE 0 END) as low,
            SUM(CASE WHEN up.state = 3 THEN 1 ELSE 0 END) as none
        FROM user_products up
        WHERE up.user_id = ?
    ''', (user['id'],)).fetchone()
    history_items = conn.execute('''
        SELECT i.name, i.quantity, i.bought_at, l.name as list_name, l.id as list_id
        FROM items i
        JOIN lists l ON i.list_id = l.id
        WHERE i.bought = 1 AND (l.owner_id = ? OR EXISTS (SELECT 1 FROM list_shares WHERE list_id = l.id AND user_id = ?))
        ORDER BY i.bought_at DESC
        LIMIT 100
    ''', (user['id'], user['id'])).fetchall()
    conn.close()
    return render_template('profile.html', user=user, is_owner=(user['id'] == current_user.id),
                           show_activity=show_activity, can_view_activity=can_view, activity=activity,
                           total_items=total_items, bought_count=bought_count, by_category=by_category,
                           pantry_totals=pantry_totals, history_items=history_items)


@app.route('/me/pantry')
@login_required
def my_pantry():
    query = request.args.get('q', '').strip()
    conn = get_db()
    owner = conn.execute('SELECT * FROM users WHERE id = ?', (current_user.id,)).fetchone()
    user_products = conn.execute('''
        SELECT up.id, up.state, up.notes, p.name, c.name as category_name
        FROM user_products up
        JOIN products p ON up.product_id = p.id
        LEFT JOIN categories c ON p.category_id = c.id
        WHERE up.user_id = ?
        ORDER BY p.name
    ''', (current_user.id,)).fetchall()
    has_products_to_buy = any(up['state'] in (2, 3) for up in user_products)
    params = [current_user.id]
    catalog_query = '''
        SELECT p.id, p.name, c.name as category_name
        FROM products p
        LEFT JOIN categories c ON p.category_id = c.id
        WHERE p.id NOT IN (SELECT product_id FROM user_products WHERE user_id = ?)
    '''
    if query:
        catalog_query += ' AND p.name LIKE ?'
        params.append(f'%{query}%')
    catalog_query += ' ORDER BY p.name LIMIT 100'
    catalog = conn.execute(catalog_query, params).fetchall()
    conn.close()
    return render_template(
        'user_pantry.html',
        owner=owner,
        can_edit=True,
        user_products=user_products,
        catalog=catalog,
        query=query,
        has_products_to_buy=has_products_to_buy
    )


@app.route('/user/<username>/pantry')
@login_required
def user_pantry(username):
    conn = get_db()
    owner = conn.execute('SELECT * FROM users WHERE username = ?', (username,)).fetchone()
    if not owner:
        conn.close()
        abort(404)
    user_products = conn.execute('''
        SELECT up.id, up.state, up.notes, p.name, c.name as category_name
        FROM user_products up
        JOIN products p ON up.product_id = p.id
        LEFT JOIN categories c ON p.category_id = c.id
        WHERE up.user_id = ?
        ORDER BY p.name
    ''', (owner['id'],)).fetchall()
    catalog = []
    conn.close()
    return render_template(
        'user_pantry.html',
        owner=owner,
        can_edit=(owner['id'] == current_user.id),
        user_products=user_products,
        catalog=catalog,
        query='',
        has_products_to_buy=any(up['state'] in (2, 3) for up in user_products)
    )


@app.route('/me/pantry/board')
@login_required
def my_pantry_board():
    conn = get_db()
    owner = conn.execute('SELECT * FROM users WHERE id = ?', (current_user.id,)).fetchone()
    user_products = conn.execute('''
        SELECT up.id, up.state, up.notes, p.name, c.name as category_name
        FROM user_products up
        JOIN products p ON up.product_id = p.id
        LEFT JOIN categories c ON p.category_id = c.id
        WHERE up.user_id = ?
        ORDER BY p.name
    ''', (current_user.id,)).fetchall()
    categories = conn.execute('SELECT * FROM categories ORDER BY name').fetchall()
    products = conn.execute('SELECT id, name FROM products ORDER BY name').fetchall()
    active_list = conn.execute('SELECT id, name FROM lists WHERE owner_id = ? ORDER BY created_at DESC LIMIT 1',
                               (current_user.id,)).fetchone()
    conn.close()
    groups = {
        'in_stock': [up for up in user_products if up['state'] == 1],
        'low': [up for up in user_products if up['state'] == 2],
        'none': [up for up in user_products if up['state'] == 3],
    }
    return render_template(
        'user_pantry_board.html',
        owner=owner,
        can_edit=True,
        groups=groups,
        categories=categories,
        products=products,
        active_list=active_list
    )


@app.route('/user/<username>/pantry/board')
@login_required
def user_pantry_board(username):
    conn = get_db()
    owner = conn.execute('SELECT * FROM users WHERE username = ?', (username,)).fetchone()
    if not owner:
        conn.close()
        abort(404)
    user_products = conn.execute('''
        SELECT up.id, up.state, up.notes, p.name, c.name as category_name
        FROM user_products up
        JOIN products p ON up.product_id = p.id
        LEFT JOIN categories c ON p.category_id = c.id
        WHERE up.user_id = ?
        ORDER BY p.name
    ''', (owner['id'],)).fetchall()
    categories = conn.execute('SELECT * FROM categories ORDER BY name').fetchall()
    products = conn.execute('SELECT id, name FROM products ORDER BY name').fetchall()
    active_list = conn.execute('SELECT id, name FROM lists WHERE owner_id = ? ORDER BY created_at DESC LIMIT 1',
                               (owner['id'],)).fetchone()
    conn.close()
    groups = {
        'in_stock': [up for up in user_products if up['state'] == 1],
        'low': [up for up in user_products if up['state'] == 2],
        'none': [up for up in user_products if up['state'] == 3],
    }
    return render_template(
        'user_pantry_board.html',
        owner=owner,
        can_edit=(owner['id'] == current_user.id),
        groups=groups,
        categories=categories,
        products=products,
        active_list=active_list
    )


@app.route('/me/pantry/board/add', methods=['POST'])
@login_required
def pantry_board_add():
    name = (request.form.get('name') or '').strip()
    if not name:
        flash('Введите название продукта', 'danger')
        return redirect(url_for('my_pantry_board'))
    category_id = request.form.get('category_id', type=int)
    state = request.form.get('state', type=int) or 1
    if state not in (1, 2, 3):
        state = 1
    conn = get_db()
    product = conn.execute('SELECT id, category_id FROM products WHERE name = ?', (name,)).fetchone()
    if product:
        product_id = product['id']
        if category_id and (product['category_id'] is None):
            conn.execute('UPDATE products SET category_id = ? WHERE id = ?', (category_id, product_id))
    else:
        conn.execute('INSERT INTO products (name, category_id) VALUES (?, ?)', (name, category_id))
        product_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    conn.execute(
        'INSERT OR IGNORE INTO user_products (user_id, product_id, state) VALUES (?, ?, ?)',
        (current_user.id, product_id, state)
    )
    conn.execute(
        'UPDATE user_products SET state = ? WHERE user_id = ? AND product_id = ?',
        (state, current_user.id, product_id)
    )
    conn.commit()
    conn.close()
    log_pantry_activity(current_user.id, product_id, 'add', None, state, 'added_to_board')
    flash('Продукт добавлен на доску', 'success')
    return redirect(url_for('my_pantry_board'))


@app.route('/me/pantry/add', methods=['POST'])
@login_required
def pantry_add():
    product_id = request.form.get('product_id', type=int)
    if not product_id:
        flash('Не выбран продукт', 'danger')
        return redirect(url_for('my_pantry'))
    conn = get_db()
    try:
        conn.execute(
            'INSERT OR IGNORE INTO user_products (user_id, product_id, state) VALUES (?, ?, ?)',
            (current_user.id, product_id, 1)
        )
        conn.commit()
        flash('Продукт добавлен в шаблон', 'success')
    finally:
        conn.close()
    log_pantry_activity(current_user.id, product_id, 'add', None, 1, 'added_from_table')
    return redirect(url_for('my_pantry'))


@app.route('/me/pantry/<int:user_product_id>/state', methods=['POST'])
@login_required
def pantry_set_state(user_product_id):
    state = request.form.get('state', type=int)
    if state not in (1, 2, 3):
        flash('Неверное состояние', 'danger')
        return redirect(url_for('my_pantry'))
    conn = get_db()
    try:
        row = conn.execute(
            'SELECT state, product_id FROM user_products WHERE id = ? AND user_id = ?',
            (user_product_id, current_user.id)
        ).fetchone()
        if not row:
            conn.close()
            flash('Продукт не найден', 'danger')
            return redirect(url_for('my_pantry'))
        old_state = row['state']
        conn.execute(
            'UPDATE user_products SET state = ? WHERE id = ? AND user_id = ?',
            (state, user_product_id, current_user.id)
        )
        conn.commit()
        if old_state != state:
            log_pantry_activity(current_user.id, row['product_id'], 'move', old_state, state, 'state_changed')
    except sqlite3.OperationalError:
        conn.rollback()
        flash('База данных временно занята, попробуйте ещё раз', 'danger')
    finally:
        conn.close()
    return redirect(url_for('my_pantry'))


@app.route('/me/pantry/<int:user_product_id>/remove', methods=['POST'])
@login_required
def pantry_remove(user_product_id):
    conn = get_db()
    try:
        row = conn.execute(
            'SELECT product_id, state FROM user_products WHERE id = ? AND user_id = ?',
            (user_product_id, current_user.id)
        ).fetchone()
        if not row:
            conn.close()
            flash('Продукт не найден', 'danger')
            return redirect(url_for('my_pantry'))
        conn.execute('DELETE FROM user_product_comments WHERE user_product_id = ?', (user_product_id,))
        conn.execute('DELETE FROM user_products WHERE id = ? AND user_id = ?', (user_product_id, current_user.id))
        conn.commit()
        flash('Продукт удалён из шаблона', 'info')
        log_pantry_activity(current_user.id, row['product_id'], 'remove', row['state'], None, 'removed_from_template')
    except sqlite3.IntegrityError:
        conn.rollback()
        flash('Не удалось удалить продукт из-за связей в базе данных', 'danger')
    finally:
        conn.close()
    return redirect(url_for('my_pantry'))


@app.route('/me/pantry/create-list', methods=['POST'])
@login_required
def pantry_create_list():
    conn = get_db()
    to_buy = conn.execute('''
        SELECT p.name
        FROM user_products up
        JOIN products p ON up.product_id = p.id
        WHERE up.user_id = ? AND up.state IN (2, 3)
        ORDER BY p.name
    ''', (current_user.id,)).fetchall()
    if not to_buy:
        conn.close()
        flash('Нет товаров, которые нужно купить', 'info')
        return redirect(url_for('my_pantry'))
    list_name = 'Список из шаблона'
    conn.execute('INSERT INTO lists (name, owner_id) VALUES (?, ?)', (list_name, current_user.id))
    new_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    for row in to_buy:
        conn.execute(
            'INSERT INTO items (list_id, name, quantity) VALUES (?, ?, ?)',
            (new_id, row['name'], 1)
        )
    conn.commit()
    conn.close()
    flash('Создан новый список из шаблона', 'success')
    return redirect(url_for('list_view', list_id=new_id))


@app.route('/me/pantry/item/<int:user_product_id>/add-to-list', methods=['POST'])
@login_required
def pantry_item_add_to_list(user_product_id):
    list_id = request.form.get('list_id', type=int)
    conn = get_db()
    up = conn.execute('''
        SELECT up.state, up.product_id, p.name
        FROM user_products up
        JOIN products p ON up.product_id = p.id
        WHERE up.id = ? AND up.user_id = ?
    ''', (user_product_id, current_user.id)).fetchone()
    if not up:
        conn.close()
        flash('Продукт не найден', 'danger')
        return redirect(url_for('my_pantry_board'))
    if up['state'] not in (2, 3):
        conn.close()
        flash('Добавлять в список можно только товары с состоянием «Осталось мало» или «Отсутствует»', 'warning')
        return redirect(url_for('pantry_item', user_product_id=user_product_id))
    target_list = None
    if list_id:
        target_list = conn.execute('SELECT id, owner_id FROM lists WHERE id = ?', (list_id,)).fetchone()
        if not target_list or target_list['owner_id'] != current_user.id:
            conn.close()
            flash('Список не найден или не принадлежит вам', 'danger')
            return redirect(url_for('pantry_item', user_product_id=user_product_id))
    else:
        conn.execute('INSERT INTO lists (name, owner_id) VALUES (?, ?)', ('Список из профиля', current_user.id))
        list_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    conn.execute(
        'INSERT INTO items (list_id, name, quantity) VALUES (?, ?, ?)',
        (list_id, up['name'], 1)
    )
    conn.commit()
    conn.close()
    flash('Товар добавлен в список покупок', 'success')
    return redirect(url_for('list_view', list_id=list_id))


@app.route('/me/pantry/item/<int:user_product_id>', methods=['GET', 'POST'])
@login_required
def pantry_item(user_product_id):
    conn = get_db()
    up = conn.execute('SELECT * FROM user_products WHERE id = ? AND user_id = ?', (user_product_id, current_user.id)).fetchone()
    if not up:
        conn.close()
        abort(404)
    if request.method == 'POST':
        text = (request.form.get('text') or '').strip()
        if text:
            conn.execute(
                'INSERT INTO user_product_comments (user_product_id, user_id, text) VALUES (?, ?, ?)',
                (user_product_id, current_user.id, text)
            )
            conn.commit()
        conn.close()
        return redirect(url_for('pantry_item', user_product_id=user_product_id))
    product = conn.execute('SELECT name, category_id FROM products WHERE id = ?', (up['product_id'],)).fetchone()
    category_name_row = None
    if product and product['category_id']:
        category_name_row = conn.execute('SELECT name FROM categories WHERE id = ?', (product['category_id'],)).fetchone()
    comments = conn.execute('''
        SELECT c.text, c.created_at, u.username
        FROM user_product_comments c
        JOIN users u ON c.user_id = u.id
        WHERE c.user_product_id = ?
        ORDER BY c.created_at DESC
    ''', (user_product_id,)).fetchall()
    active_list = conn.execute('SELECT id, name FROM lists WHERE owner_id = ? ORDER BY created_at DESC LIMIT 1',
                               (current_user.id,)).fetchone()
    conn.close()
    return render_template(
        'user_pantry_item.html',
        up=up,
        product=product,
        category_name=category_name_row['name'] if category_name_row else None,
        comments=comments,
        active_list=active_list
    )

# === REST API ===
def api_login_required(f):
    @wraps(f)
    def inner(*args, **kwargs):
        if not current_user.is_authenticated:
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return inner


def api_list_access(role='viewer'):
    def decorator(f):
        @wraps(f)
        def inner(*args, **kwargs):
            if not current_user.is_authenticated:
                return jsonify({'error': 'Unauthorized'}), 401
            list_id = kwargs.get('list_id')
            conn = get_db()
            lst = conn.execute('SELECT * FROM lists WHERE id = ?', (list_id,)).fetchone()
            if not lst:
                conn.close()
                return jsonify({'error': 'Not found'}), 404
            is_owner = lst['owner_id'] == current_user.id
            share = conn.execute('SELECT * FROM list_shares WHERE list_id = ? AND user_id = ?', (list_id, current_user.id)).fetchone()
            conn.close()
            if not is_owner and not share:
                return jsonify({'error': 'Forbidden'}), 403
            if role == 'editor' and not is_owner and (not share or share['role'] != 'editor'):
                return jsonify({'error': 'Forbidden'}), 403
            return f(*args, **kwargs)
        return inner
    return decorator


@app.route('/api/lists')
@api_login_required
def api_lists():
    conn = get_db()
    rows = conn.execute('''
        SELECT l.id, l.name, l.created_at,
               (SELECT COUNT(*) FROM items WHERE list_id = l.id AND bought = 0) as pending
        FROM lists l
        WHERE l.owner_id = ? OR EXISTS (SELECT 1 FROM list_shares WHERE list_id = l.id AND user_id = ?)
        ORDER BY l.created_at DESC
    ''', (current_user.id, current_user.id)).fetchall()
    conn.close()
    return jsonify({'lists': [dict(r) for r in rows]})


@app.route('/api/lists/<int:list_id>')
@api_login_required
@api_list_access('viewer')
def api_list_get(list_id):
    conn = get_db()
    lst = conn.execute('SELECT id, name, created_at FROM lists WHERE id = ?', (list_id,)).fetchone()
    items = conn.execute('''
        SELECT i.id, i.name, i.quantity, i.notes, i.bought, i.bought_at, i.category_id, c.name as category_name
        FROM items i LEFT JOIN categories c ON i.category_id = c.id
        WHERE i.list_id = ? ORDER BY i.bought, i.name
    ''', (list_id,)).fetchall()
    conn.close()
    data = dict(lst)
    data['items'] = [dict(i) for i in items]
    data['bought'] = [k for k in data['items'] if k['bought']]
    data['pending'] = [k for k in data['items'] if not k['bought']]
    return jsonify(data)


@app.route('/api/lists', methods=['POST'])
@api_login_required
def api_list_create():
    data = request.get_json() or {}
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'error': 'name required'}), 400
    conn = get_db()
    conn.execute('INSERT INTO lists (name, owner_id) VALUES (?, ?)', (name, current_user.id))
    new_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    conn.commit()
    conn.close()
    return jsonify({'id': new_id, 'name': name}), 201


@app.route('/api/lists/<int:list_id>/items', methods=['POST'])
@api_login_required
@api_list_access('editor')
def api_item_add(list_id):
    data = request.get_json() or {}
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'error': 'name required'}), 400
    category_id = data.get('category_id')
    quantity = data.get('quantity', 1)
    notes = data.get('notes', '')
    conn = get_db()
    conn.execute('INSERT INTO items (list_id, name, category_id, quantity, notes) VALUES (?, ?, ?, ?, ?)',
                 (list_id, name, category_id, quantity, notes))
    item_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    conn.commit()
    conn.close()
    return jsonify({'id': item_id, 'name': name, 'quantity': quantity, 'notes': notes, 'bought': False}), 201


@app.route('/api/lists/<int:list_id>/items/<int:item_id>/toggle', methods=['POST'])
@api_login_required
@api_list_access('editor')
def api_item_toggle(list_id, item_id):
    conn = get_db()
    row = conn.execute('SELECT bought FROM items WHERE id = ? AND list_id = ?', (item_id, list_id)).fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'Not found'}), 404
    new_val = 0 if row['bought'] else 1
    bought_at = datetime.utcnow().isoformat() if new_val else None
    conn.execute('UPDATE items SET bought = ?, bought_at = ? WHERE id = ?', (new_val, bought_at, item_id))
    conn.commit()
    conn.close()
    return jsonify({'bought': bool(new_val), 'bought_at': bought_at})


@app.route('/api/lists/<int:list_id>/items/<int:item_id>', methods=['DELETE'])
@api_login_required
@api_list_access('editor')
def api_item_delete(list_id, item_id):
    conn = get_db()
    conn.execute('DELETE FROM items WHERE id = ? AND list_id = ?', (item_id, list_id))
    conn.commit()
    conn.close()
    return jsonify({'deleted': True})


@app.route('/api/templates')
@api_login_required
def api_templates():
    conn = get_db()
    rows = conn.execute('SELECT id, name FROM list_templates').fetchall()
    conn.close()
    return jsonify({'templates': [dict(r) for r in rows]})


@app.route('/api')
@login_required
def api_docs():
    return render_template('api_docs.html')


if __name__ == '__main__':
    # Локально: 127.0.0.1 + debug. На Render переменная RENDER задана — нужен 0.0.0.0 (и лучше gunicorn через Start Command).
    port = int(os.environ.get('PORT', 5001))
    if os.environ.get('RENDER'):
        app.run(host='0.0.0.0', port=port, debug=False)
    else:
        app.run(host='127.0.0.1', port=port, debug=True)
