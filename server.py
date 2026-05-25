import os
import asyncio
import threading
from flask import Flask, Response, request, abort
from pyrogram import Client

API_ID = 36112539
API_HASH = "c9797b78de5e31facde739d92253b2a1"
BOT_TOKEN = "8975966534:AAE3gIBCyVmQKaVA0pVH6vqqS_AZEsV4kXs"
DB_CHANNEL = -1003911942501

app = Flask(__name__)
loop = asyncio.new_event_loop()

client = Client(
    "stream_server",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

def start_client():
    asyncio.set_event_loop(loop)
    loop.run_until_complete(client.start())

threading.Thread(target=start_client, daemon=True).start()
import time
time.sleep(5)

@app.route("/")
def index():
    return "🎬 CineFlix Stream Server Running!"

@app.route("/stream/<int:file_id>")
def stream(file_id):
    try:
        future = asyncio.run_coroutine_threadsafe(
            client.get_messages(DB_CHANNEL, file_id), loop
        )
        message = future.result(timeout=10)

        if not message or not message.media:
            abort(404)

        if message.video:
            file_size = message.video.file_size
            mime_type = message.video.mime_type
        elif message.document:
            file_size = message.document.file_size
            mime_type = message.document.mime_type
        else:
            abort(404)

        range_header = request.headers.get("Range", None)
        start = 0
        end = file_size - 1

        if range_header:
            parts = range_header.replace("bytes=", "").split("-")
            start = int(parts[0])
            end = int(parts[1]) if parts[1] else file_size - 1

        def generate():
            async def _gen():
                async for chunk in client.stream_media(message, offset=start):
                    yield chunk

            async def collect():
                chunks = []
                async for chunk in _gen():
                    chunks.append(chunk)
                return chunks

            future = asyncio.run_coroutine_threadsafe(collect(), loop)
            chunks = future.result(timeout=60)
            for chunk in chunks:
                yield chunk

        headers = {
            "Content-Type": mime_type,
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(end - start + 1),
        }

        return Response(
            generate(),
            status=206 if range_header else 200,
            headers=headers,
            direct_passthrough=True
        )
    except Exception as e:
        print(f"Error: {e}")
        abort(500)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, threaded=True)
