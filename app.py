import os
from flask import Flask, render_template, request, redirect, url_for
import sqlite3

app = Flask(__name__)
DATABASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'shopping.db')


def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.execute('''CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        bought INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    conn.commit()
    conn.close()


@app.route('/')
def index():
    conn = get_db()
    items = conn.execute('SELECT * FROM items ORDER BY bought, id').fetchall()
    conn.close()
    return render_template('index.html', items=items)


@app.route('/add', methods=['POST'])
def add():
    name = request.form.get('name', '').strip()
    if name:
        conn = get_db()
        conn.execute('INSERT INTO items (name) VALUES (?)', (name,))
        conn.commit()
        conn.close()
    return redirect(url_for('index'))


@app.route('/toggle/<int:item_id>')
def toggle(item_id):
    conn = get_db()
    row = conn.execute('SELECT bought FROM items WHERE id = ?', (item_id,)).fetchone()
    if row:
        new_val = 0 if row['bought'] else 1
        conn.execute('UPDATE items SET bought = ? WHERE id = ?', (new_val, item_id))
        conn.commit()
    conn.close()
    return redirect(url_for('index'))


@app.route('/delete/<int:item_id>')
def delete(item_id):
    conn = get_db()
    conn.execute('DELETE FROM items WHERE id = ?', (item_id,))
    conn.commit()
    conn.close()
    return redirect(url_for('index'))


if __name__ == '__main__':
    init_db()
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
