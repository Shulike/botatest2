from flask import Flask, render_template, request, redirect, url_for
from jinja2 import DictLoader
import json
from common.db import db_conn
from common.models import ensure_schema

# ---------- шаблоны ---------------------------------------------------------
BASE = """{% macro nav() %}
<nav class='navbar navbar-expand-lg navbar-dark bg-dark mb-4'>
 <div class='container'>
  <a class='navbar-brand' href='/'>QuizBot Admin</a>
  <div class='navbar-nav'>
   <a class='nav-link' href='/students'>Students</a>
   <a class='nav-link' href='/results'>Results</a>
  </div>
 </div>
</nav>
{% endmacro %}
<!doctype html><html lang='en'><head>
 <meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
 <title>{{ title or 'Admin' }}</title>
 <link href='https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css' rel='stylesheet'>
</head><body>
 {{ nav() }}<div class='container'>{% block body %}{% endblock %}</div>
</body></html>"""

DASH = """{% extends 'base.html' %}{% block body %}
<h1 class='mb-4'>Dashboard</h1>
<div class='row g-4'>
 {% for c in cards %}
  <div class='col-6 col-md-3'>
   <div class='card text-bg-{{ c.color }} shadow-sm'>
    <div class='card-body'>
      <h6 class='card-title'>{{ c.title }}</h6>
      <h2>{{ c.value }}</h2>
    </div>
   </div>
  </div>
 {% endfor %}
</div>{% endblock %}"""

STUD = """{% extends 'base.html' %}{% block body %}
<h1 class='mb-4'>Students</h1>
<form class='row gx-2 gy-2' method='post'>
 <div class='col-auto'><input name='tgid' class='form-control' placeholder='Telegram ID' required></div>
 <div class='col-auto'><input name='name' class='form-control' placeholder='Display name'></div>
 <div class='col-auto'><button class='btn btn-primary'>Add</button></div>
</form><hr>
<table class='table table-striped'>
 <thead><tr><th>#</th><th>Telegram</th><th>Name</th><th>Active</th></tr></thead><tbody>
 {% for s in students %}
  <tr>
    <td>{{ s.Id }}</td>
    <td>{{ s.TelegramId }}</td>
    <td>{{ s.DisplayName or '' }}</td>
    <td>{{ '✔' if s.Active else '✖' }}</td>
  </tr>
 {% endfor %}
 </tbody></table>{% endblock %}"""

RES = """{% extends 'base.html' %}{% block body %}
<h1 class='mb-4'>Results</h1>
<table class='table table-bordered table-sm'>
<thead><tr><th>Student</th><th>QuizId</th><th>Chosen</th><th>Correct</th><th>When</th></tr></thead><tbody>
{% for r in rows %}
 <tr class='{% if not r.IsCorrect %}table-danger{% else %}table-success{% endif %}'>
  <td>{{ r.DisplayName or r.TelegramId }}</td>
  <td>{{ r.PendingQuizId }}</td>
  <td>{{ r.ChosenOption }}</td>
  <td>{{ '✔' if r.IsCorrect else '✖' }}</td>
  <td>{{ r.AnsweredAt }}</td>
 </tr>
{% endfor %}
</tbody></table>{% endblock %}"""
# ---------------------------------------------------------------------------

def dictrows(cur):
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

def scalar(sql: str):
    with db_conn() as c, c.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0] or 0

def accuracy():
    """Возвращает accuracy (0‒100) либо None, если результатов ещё нет."""
    with db_conn() as c, c.cursor() as cur:
        cur.execute("SELECT IsCorrect FROM dbo.QuizResults")
        vals = [r[0] for r in cur.fetchall()]
    return round(sum(vals) * 100 / len(vals), 1) if vals else None

# ---------- создание приложения -------------------------------------------
def create_app():
    ensure_schema()
    app = Flask(__name__)
    app.jinja_loader = DictLoader({
        'base.html': BASE,
        'dash.html': DASH,
        'stud.html': STUD,
        'res.html': RES
    })

    # --- Dashboard ---------------------------------------------------------
    @app.route('/')
    def dash():
        acc = accuracy()
        cards = [
            {
                'title': 'Students',
                'value': scalar('SELECT COUNT(*) FROM dbo.Students'),
                'color': 'primary'
            },
            {
                'title': 'Questions',
                'value': scalar('SELECT COUNT(*) FROM dbo.PendingQuizzes'),
                'color': 'success'
            },
            {
                'title': 'Answers',
                'value': scalar('SELECT COUNT(*) FROM dbo.QuizResults'),
                'color': 'info'
            },
            {
                'title': 'Accuracy',
                'value': f'{acc}%' if acc is not None else '—',
                'color': 'warning'
            }
        ]
        return render_template('dash.html', cards=cards)

    # --- Students ----------------------------------------------------------
    @app.route('/students', methods=['GET', 'POST'])
    def students():
        if request.method == 'POST':
            with db_conn() as c, c.cursor() as cur:
                cur.execute(
                    'INSERT INTO dbo.Students (TelegramId, DisplayName) VALUES (?, ?)',
                    int(request.form['tgid']),
                    request.form.get('name')
                )
                c.commit()
            return redirect(url_for('students'))

        with db_conn() as c, c.cursor() as cur:
            cur.execute(
                'SELECT Id, TelegramId, DisplayName, Active FROM dbo.Students ORDER BY Id'
            )
            st = dictrows(cur)
        return render_template('stud.html', students=st)

    # --- Results -----------------------------------------------------------
    @app.route('/results')
    def results():
        sql = """
            SELECT qr.PendingQuizId,
                qr.ChosenOption,
                qr.IsCorrect,
                qr.AnsweredAt,
                COALESCE(st.DisplayName,'') AS DisplayName,
                qr.StudentId               AS TelegramId
            FROM dbo.QuizResults qr
            LEFT JOIN dbo.Students st ON st.TelegramId = qr.StudentId
            ORDER BY qr.AnsweredAt DESC
        """
        with db_conn() as c, c.cursor() as cur:
            cur.execute(sql)
            rows = dictrows(cur)
        return render_template('res.html', rows=rows)

    return app

