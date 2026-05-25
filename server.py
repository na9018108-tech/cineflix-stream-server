import os
import asyncio
from flask import Flask, Response, request, abort
from pyrogram import Client
import aiohttp

API_ID = 36112539
API_HASH = "c9797b78de5e31facde739d92253b2a1"
BOT_TOKEN = "8975966534:AAE3gIBCyVmQKaVA0pVH6vqqS_AZEsV4kXs"
DB_CHANNEL = -1003911942501

app = Flask(__name__)
client = Client(
    "stream_server",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

@app.route("/stream/<int:file_id>")
async def stream(file_id):
    try:
        message = await client.get_messages(DB_CHANNEL, file_id)
        if not message or not message.media:
            abort(404)

        file_size = message.video.file_size if message.video else message.document.file_size
        mime_type = message.video.mime_type if message.video else message.document.mime_type

        range_header = request.headers.get("Range", None)
        start = 0
        end = file_size - 1

        if range_header:
            parts = range_header.replace("bytes=", "").split("-")
            start = int(parts[0])
            end = int(parts[1]) if parts[1] else file_size - 1

        chunk_size = 1024 * 1024

        async def generate():
            async for chunk in client.stream_media(message, offset=start, limit=chunk_size):
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
        abort(500)

@app.route("/")
def index():
    return "CineFlix Stream Server Running! 🎬"

async def main():
    await client.start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

if __name__ == "__main__":
    asyncio.run(main())
