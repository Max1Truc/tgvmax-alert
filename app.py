import asyncio, logging, os

from flask import Flask, render_template, send_from_directory

import db

logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.route("/")
async def index():
    return render_template(
        "index.html", listing=os.popen("cd ./.data/ && find public/ -type f | sort -r | xargs du -ch --").read()
    )


@app.route("/public/<path:filename>")
async def download_public_files(filename):
    return send_from_directory("./.data/public/", filename, as_attachment=True)