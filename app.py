import os
import json
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, flash, abort, jsonify, Response
from flask_login import LoginManager, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
import sqlite3
from datetime import datetime

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-change-in-production')
DATABASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'shopping.db')

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message = 'Войдите, чтобы продолжить'


def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys = ON')
    return conn


def init_db():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
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
    ''')
    conn.commit()
    conn.close()


class User:
    def __init__(self, id, username, email, password_hash):
        self.id = id
        self.username = username
        self.email = email
        self.password_hash = password_hash
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
        return User(row['id'], row['username'], row['email'], row['password_hash'])
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
        return redirect(url_for('index'))
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
        return redirect(url_for('index'))
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
            user = User(user_row['id'], user_row['username'], user_row['email'], user_row['password_hash'])
            login_user(user)
            flash(f'Вход выполнен', 'success')
            return redirect(url_for('index'))
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
    if not q:
        return render_template('search.html', results=[], query='')
    conn = get_db()
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
    return render_template('search.html', results=results, query=q)


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


@app.route('/history')
@login_required
def history():
    conn = get_db()
    bought = conn.execute('''
        SELECT i.name, i.quantity, i.bought_at, l.name as list_name, l.id as list_id
        FROM items i
        JOIN lists l ON i.list_id = l.id
        WHERE i.bought = 1 AND (l.owner_id = ? OR EXISTS (SELECT 1 FROM list_shares WHERE list_id = l.id AND user_id = ?))
        ORDER BY i.bought_at DESC
        LIMIT 100
    ''', (current_user.id, current_user.id)).fetchall()
    conn.close()
    return render_template('history.html', items=bought)


@app.route('/stats')
@login_required
def stats():
    conn = get_db()
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
    conn.close()
    return render_template('stats.html', total_items=total_items, bought_count=bought_count, by_category=by_category)


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
        return redirect(url_for('settings'))
    conn = get_db()
    user = conn.execute('SELECT * FROM users WHERE id = ?', (current_user.id,)).fetchone()
    conn.close()
    return render_template('settings.html', user=user)


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
    init_db()
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
