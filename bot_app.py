# bot_app.py — финальная версия с динамическим тайм-аутом
# -------------------------------------------------------
import json, logging, os
from typing import List, Optional, Sequence, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    PollAnswerHandler,
)
from telegram.request import HTTPXRequest

from telegram_config import BOT_TOKEN, ADMIN_CHAT_ID
from common.db import db_conn
from common.models import ensure_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ─────────── helpers ───────────
def active_students() -> List[int]:
    with db_conn() as c, c.cursor() as cur:
        cur.execute("SELECT TelegramId FROM dbo.Students WHERE Active=1")
        return [r[0] for r in cur.fetchall()]


def get_recent_processedfiles() -> Sequence[Tuple[int, str, str]]:
    sql = """
        SELECT Id, FileName, QuizJson
        FROM dbo.ProcessedFiles
        WHERE QuizJson IS NOT NULL
          AND DownloadedAt >= DATEADD(day,-1,SYSUTCDATETIME())
          AND NOT EXISTS (
              SELECT 1 FROM dbo.PendingQuizzes pq
              WHERE pq.ProcessedFileId = Id
          )
        ORDER BY DownloadedAt, Id
    """
    with db_conn() as c, c.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def file_title(pf_id: int) -> str:
    with db_conn() as c, c.cursor() as cur:
        cur.execute("SELECT FileName FROM dbo.ProcessedFiles WHERE Id=?", pf_id)
        row = cur.fetchone()
    return os.path.splitext(row[0])[0] if row else f"Файл {pf_id}"


def student_name(tg_id: int) -> str:
    with db_conn() as c, c.cursor() as cur:
        cur.execute("SELECT DisplayName FROM dbo.Students WHERE TelegramId=?", tg_id)
        row = cur.fetchone()
    return row[0] if row and row[0] else str(tg_id)


# ─────────── FIXED: insert_pending ───────────
def insert_pending(pf_id: int, quiz_json: str) -> int:
    """Импортирует вопросы файла. Возвращает количество добавленных строк."""
    try:
        items = json.loads(quiz_json)
    except json.JSONDecodeError:
        logger.warning("Skip file %s: invalid JSON", pf_id)
        return 0

    if not isinstance(items, list):
        return 0

    rows = [
        (
            pf_id,
            q["question"],
            json.dumps(q["options"], ensure_ascii=False),
            q["answer"],
        )
        for q in items
        if {"question", "options", "answer"}.issubset(q)
    ]

    with db_conn() as c, c.cursor() as cur:
        # очищаем возможный «хвост» от предыдущих запусков
        cur.execute("DELETE FROM dbo.PendingQuizzes WHERE ProcessedFileId=?", pf_id)

        # если вопросов нет — просто завершаем без executemany
        if not rows:
            c.commit()
            return 0

        cur.executemany(
            "INSERT INTO dbo.PendingQuizzes "
            "(ProcessedFileId,Question,Options,Answer) VALUES (?,?,?,?)",
            rows,
        )
        c.commit()

    return len(rows)


def create_session(pf_id: int, student: int, total: int) -> None:
    with db_conn() as c, c.cursor() as cur:
        cur.execute(
            """
            MERGE dbo.QuizSessions WITH (HOLDLOCK) AS T
            USING (SELECT ? AS pf, ? AS st, ? AS tot) AS S
              ON (T.ProcessedFileId = S.pf AND T.StudentId = S.st)
            WHEN MATCHED AND T.Total <> S.tot THEN
                 UPDATE SET Total = S.tot
            WHEN NOT MATCHED THEN
                 INSERT (ProcessedFileId,StudentId,Total)
                 VALUES (S.pf,S.st,S.tot);
            """,
            pf_id,
            student,
            total,
        )
        c.commit()


# ─────────── handlers ───────────
async def cmd_sync(update: Optional[Update], ctx: ContextTypes.DEFAULT_TYPE):
    rows = get_recent_processedfiles()
    if not rows:
        await ctx.bot.send_message(ADMIN_CHAT_ID, "ℹ️ Новых викторин нет.")
        return

    grand_total = 0
    for pf_id, fname, quiz_json in rows:
        imported = insert_pending(pf_id, quiz_json)
        grand_total += imported
        await ctx.bot.send_message(
            ADMIN_CHAT_ID,
            f"Импортировано из «{os.path.splitext(fname)[0]}»: {imported} вопросов.",
        )

    if grand_total:
        await send_pending_questions(ctx)


async def send_pending_questions(ctx: ContextTypes.DEFAULT_TYPE):
    with db_conn() as c, c.cursor() as cur:
        cur.execute(
            "SELECT Id,ProcessedFileId,Question,Options,Answer "
            "FROM dbo.PendingQuizzes WHERE Approved IS NULL"
        )
        rows = cur.fetchall()

    if not rows:
        await maybe_prompt_send(ctx)
        return

    for qid, pf, qtext, opts_json, ans in rows:
        opts = json.loads(opts_json)
        fname = file_title(pf)
        txt = (
            f"<i>«{fname}»</i>\n<b>Вопрос:</b> {qtext}\n\n"
            + "\n".join(f"{i+1}. {o}" for i, o in enumerate(opts))
            + f"\n\n<b>Ответ:</b> {ans}"
        )
        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton("✅", callback_data=f"a:{qid}"),
              InlineKeyboardButton("❌", callback_data=f"r:{qid}")]]
        )
        await ctx.bot.send_message(
            ADMIN_CHAT_ID, txt, parse_mode="HTML", reply_markup=kb
        )


async def cb_approve(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    act, qid = q.data.split(":")

    with db_conn() as c, c.cursor() as cur:
        cur.execute(
            "UPDATE dbo.PendingQuizzes SET Approved=? WHERE Id=?",
            1 if act == "a" else 0,
            int(qid),
        )
        c.commit()

    await q.edit_message_reply_markup(None)
    await q.edit_message_text(
        q.message.text + f"\nСтатус: {'✅' if act == 'a' else '❌'}"
    )
    await maybe_prompt_send(ctx)


async def maybe_prompt_send(ctx: ContextTypes.DEFAULT_TYPE):
    sql = """
        SELECT ProcessedFileId,
               COUNT(*) total,
               SUM(CASE WHEN Approved=1 THEN 1 END) ok,
               SUM(CASE WHEN Approved IS NULL THEN 1 ELSE 0 END) pend,
               MIN(CAST(Prompted AS INT)) prm
        FROM dbo.PendingQuizzes
        GROUP BY ProcessedFileId
        HAVING SUM(CASE WHEN Approved IS NULL THEN 1 ELSE 0 END)=0
    """
    with db_conn() as c, c.cursor() as cur:
        cur.execute(sql)
        for pf, total, ok, _pend, prm in cur.fetchall():
            if ok == 0 or prm:
                continue
            fname = file_title(pf)
            kb = InlineKeyboardMarkup(
                [[InlineKeyboardButton("➡️ Разослать", callback_data=f"send:{pf}")]]
            )
            await ctx.bot.send_message(
                ADMIN_CHAT_ID,
                f"Все вопросы файла «{fname}» одобрены ({ok}/{total}). "
                "Разослать ученикам?",
                reply_markup=kb,
            )
            cur.execute(
                "UPDATE dbo.PendingQuizzes SET Prompted=1 WHERE ProcessedFileId=?", pf
            )
            c.commit()


async def cb_send_student(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    pf_id = int(q.data.split(":")[1])
    fname = file_title(pf_id)

    with db_conn() as c, c.cursor() as cur:
        cur.execute(
            "SELECT Id FROM dbo.PendingQuizzes "
            "WHERE ProcessedFileId=? AND Approved=1",
            pf_id,
        )
        pq_ids = [r[0] for r in cur.fetchall()]
        total_questions = len(pq_ids)

        students = active_students()
        deliveries = [(pid, sid) for pid in pq_ids for sid in students]
        cur.executemany(
            "INSERT INTO dbo.QuizDeliveries (PendingQuizId,StudentId) VALUES (?,?)",
            deliveries,
        )
        c.commit()

    for sid in students:
        await ctx.bot.send_message(
            sid,
            f"🔥 Новый тест «{fname}» на {total_questions} вопрос(а/ов). "
            f"У вас будет {total_questions} минут.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🚀 Я готов!", callback_data=f"start:{pf_id}")]]
            ),
        )

    await q.edit_message_reply_markup(None)
    await q.edit_message_text("Анонсы отправлены учащимся.")


async def cb_start_test(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    pf_id = int(q.data.split(":")[1])
    student = q.from_user.id
    fname = file_title(pf_id)

    with db_conn() as c, c.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM dbo.PendingQuizzes "
            "WHERE ProcessedFileId=? AND Approved=1",
            pf_id,
        )
        total = cur.fetchone()[0]
        create_session(pf_id, student, total)
        cur.execute(
            "UPDATE dbo.QuizSessions SET StartedAt=SYSUTCDATETIME() "
            "WHERE ProcessedFileId=? AND StudentId=?", pf_id, student
        )
        cur.execute(
            "UPDATE dbo.QuizDeliveries SET Started=1 "
            "WHERE StudentId=? AND PendingQuizId IN "
            "(SELECT Id FROM dbo.PendingQuizzes WHERE ProcessedFileId=?)",
            student, pf_id
        )
        c.commit()

    await q.edit_message_reply_markup(None)
    await q.edit_message_text(
        f"Начинаем тест «{fname}»! У вас {total} минут."
    )

    # отправляем Poll-ы
    with db_conn() as c, c.cursor() as cur:
        cur.execute(
            "SELECT Id,Question,Options,Answer FROM dbo.PendingQuizzes "
            "WHERE ProcessedFileId=? AND Approved=1",
            pf_id,
        )
        pending = cur.fetchall()

        for pid, qtext, opts_json, ans in pending:
            opts = json.loads(opts_json)
            try:
                correct_idx = opts.index(ans)
            except ValueError:
                logger.error(
                    "File #%s, question id %s: answer not found in options", pf_id, pid
                )
                continue

            poll = await ctx.bot.send_poll(
                student,
                qtext,
                opts,
                type="quiz",
                correct_option_id=correct_idx,
                is_anonymous=False,
            )
            cur.execute(
                "UPDATE dbo.QuizDeliveries SET PollId=? "
                "WHERE PendingQuizId=? AND StudentId=?",
                poll.poll.id, pid, student
            )
        c.commit()

    timeout_sec = total * 60
    ctx.job_queue.run_once(
        timeout_session,
        timeout_sec,
        data={"pf_id": pf_id, "student": student},
        name=f"to_{pf_id}_{student}",
    )


async def timeout_session(ctx: ContextTypes.DEFAULT_TYPE):
    pf_id = ctx.job.data["pf_id"]
    student = ctx.job.data["student"]
    fname = file_title(pf_id)

    with db_conn() as c, c.cursor() as cur:
        cur.execute(
            "SELECT Total,Correct,FinishedAt FROM dbo.QuizSessions "
            "WHERE ProcessedFileId=? AND StudentId=?", pf_id, student
        )
        total, correct, fin = cur.fetchone()
        if fin:
            return
        cur.execute(
            "UPDATE dbo.QuizSessions "
            "SET FinishedAt=SYSUTCDATETIME(), TimedOut=1 "
            "WHERE ProcessedFileId=? AND StudentId=?", pf_id, student
        )
        c.commit()

    await ctx.bot.send_message(
        student, f"⏰ Время вышло! Тест «{fname}» не завершён."
    )
    await ctx.bot.send_message(
        ADMIN_CHAT_ID,
        f"Ученик {student_name(student)} не успел пройти тест «{fname}». "
        f"Результат {correct}/{total}.",
    )


async def handle_poll(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ans = update.poll_answer
    sel = ans.option_ids[0] if ans.option_ids else -1

    with db_conn() as c, c.cursor() as cur:
        cur.execute(
            "SELECT qd.PendingQuizId, qd.StudentId, pq.Options, pq.Answer, pq.ProcessedFileId "
            "FROM dbo.QuizDeliveries qd "
            "JOIN dbo.PendingQuizzes pq ON pq.Id=qd.PendingQuizId "
            "WHERE qd.PollId=?", ans.poll_id
        )
        row = cur.fetchone()
        if not row:
            return

        pid, student, opts_json, right, pf_id = row
        opts = json.loads(opts_json)
        chosen = opts[sel] if 0 <= sel < len(opts) else "(none)"
        is_correct = int(chosen == right)

        cur.execute(
            "INSERT INTO dbo.QuizResults "
            "(PendingQuizId,StudentId,ChosenOption,IsCorrect) "
            "VALUES (?,?,?,?)",
            pid, student, chosen, is_correct
        )
        cur.execute(
            "UPDATE dbo.QuizSessions SET Correct=Correct+? "
            "WHERE ProcessedFileId=? AND StudentId=?", is_correct, pf_id, student
        )

        cur.execute(
            "SELECT Total,Correct FROM dbo.QuizSessions "
            "WHERE ProcessedFileId=? AND StudentId=?", pf_id, student
        )
        total, now_correct = cur.fetchone()

        cur.execute(
            "SELECT COUNT(*) FROM dbo.QuizResults "
            "WHERE StudentId=? "
            "  AND PendingQuizId IN "
            "      (SELECT Id FROM dbo.PendingQuizzes WHERE ProcessedFileId=?)",
            student, pf_id
        )
        answered = cur.fetchone()[0]

        fname = file_title(pf_id)

        if answered == total:
            cur.execute(
                "UPDATE dbo.QuizSessions "
                "SET FinishedAt=SYSUTCDATETIME() "
                "WHERE ProcessedFileId=? AND StudentId=?", pf_id, student
            )
            c.commit()
            await ctx.bot.send_message(
                student,
                f"✅ Вы завершили тест «{fname}»! Результат: {now_correct}/{total}.",
            )
            await ctx.bot.send_message(
                ADMIN_CHAT_ID,
                f"Ученик {student_name(student)}: {now_correct}/{total} "
                f"по тесту «{fname}».",
            )
        else:
            c.commit()


def run_bot():
    ensure_schema()
    req = HTTPXRequest(
        connect_timeout=20, read_timeout=40, write_timeout=20, pool_timeout=20
    )
    app = Application.builder().token(BOT_TOKEN).request(req).build()

    app.add_handler(CommandHandler("sync", cmd_sync, block=False))
    app.add_handler(CallbackQueryHandler(cb_approve, pattern="^[ar]:"))
    app.add_handler(CallbackQueryHandler(cb_send_student, pattern="^send:"))
    app.add_handler(CallbackQueryHandler(cb_start_test, pattern="^start:"))
    app.add_handler(PollAnswerHandler(handle_poll))

    app.job_queue.run_repeating(
        lambda ctx: ctx.application.create_task(cmd_sync(None, ctx)),
        interval=21600,
        first=21600,
    )

    logger.info("Bot started")
    app.run_polling()

