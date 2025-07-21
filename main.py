
from threading import Thread
from bot_app import run_bot
from web_app import create_app


def run_web():
    app=create_app()
    app.run(host='0.0.0.0', port=8000, threaded=True)


if __name__=='__main__':
    Thread(target=run_web, daemon=True).start()
    run_bot()

