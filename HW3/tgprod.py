from telethon import TelegramClient, events
from telethon.tl.functions.channels import JoinChannelRequest
from kafka import KafkaProducer
import json

api_id = 0
api_hash = ''
chats = [1050820672, 1149896996, 1101170442, 1036362176, 1310155678, 1001872252, 1054549314, 1073571855]

client = TelegramClient('session', api_id, api_hash)
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

@client.on(events.NewMessage(chats=chats))
async def handler(event):
    sender = await event.get_sender()
    name = sender.username if sender and sender.username else "anon"
    msg = {'username': name, 'ts': event.date.isoformat()}
    producer.send('tg_stream', msg)

async def main():
    await client.start()
    for chat in chats:
        try:
            await client(JoinChannelRequest(chat))
        except: pass
    await client.run_until_disconnected()

with client:
    client.loop.run_until_complete(main())
