import os
from flask import Flask, Response, request, abort
from pyrogram import Client
from pyrogram.errors import FloodWait
import asyncio

API_ID = 36112539
API_HASH = "c9797b78de5e31facde739d92253b2a1"
BOT_TOKEN = "8975966534:AAE3gIBCyVmQKaVA0pVH6vqqS_AZEsV4kXs"
DB_CHANNEL = -1003911942501

app = Flask(__name__)

async def get_message(file_id):
    async with Client("stream", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN) as c:
        return await c.get_messages(DB_CHANNEL, file_id)

async def get_chunks(message, start=0):
    async with Client("stream", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN) as c:
        async for chunk in c.stream_media(message, offset=start):
            yield chunk

@app.route("/")
def index():
    return "CineFlix Stream Server Running!"

@app.route("/stream/<int:file_id>")
def stream(file_id):
    try:
        loop = asyncio.new_event_loop()
        message = loop.run_until_complete(get_message(file_id))
        loop.close()

        if not message or not message.media:
            abort(404)

        if message.video:
            file_size = message.video.file_size
            mime_type = message.video.mime_type or "video/mp4"
        else:
            file_size = message.document.file_size
            mime_type = message.document.mime_type or "video/mp4"

        range_header = request.headers.get("Range")
        start = 0
        if range_header:
            start = int(range_header.replace("bytes=", "").split("-")[0])

        end = file_size - 1

        def generate():
            loop2 = asyncio.new_event_loop()
            async def _stream():
                async with Client("s", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN) as c:
                    async for chunk in c.stream_media(message, offset=start):
                        yield chunk
            async def collect():
                data = b""
                async for chunk in _stream():
                    data += chunk
                return data
            data = loop2.run_until_complete(collect())
            loop2.close()
            yield data

        headers = {
            "Content-Type": mime_type,
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(end - start + 1),
        }
        return Response(generate(), status=206 if range_header else 200, headers=headers)

    except Exception as e:
        print(f"Error: {e}")
        abort(500)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
