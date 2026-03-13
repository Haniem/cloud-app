# Список покупок — Облачное приложение

Приложение для предмета «Основы облачных и туманных технологий».

## Локальный запуск

```bash
pip install -r requirements.txt
python app.py
```

Откройте http://localhost:5000

## Развёртывание на Render (бесплатно)

1. Зарегистрируйтесь на [render.com](https://render.com)
2. Нажмите **New** → **Web Service**
3. Подключите репозиторий GitHub (сначала загрузите проект в GitHub)
4. Настройки:
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `gunicorn app:app`
5. Нажмите **Create Web Service**
6. Через 2–3 минуты приложение будет доступно по ссылке вида `https://ваш-проект.onrender.com`

## Альтернатива: PythonAnywhere

1. Зарегистрируйтесь на [pythonanywhere.com](https://www.pythonanywhere.com)
2. Загрузите файлы через Files
3. Создайте веб-приложение (Flask), укажите путь к `app.py`
