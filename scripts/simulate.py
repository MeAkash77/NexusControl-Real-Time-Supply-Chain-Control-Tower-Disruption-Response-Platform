"""Generate synthetic supply-chain events. Usage: python simulate.py --events 100000 --token <jwt>"""
import argparse, json, random, uuid, urllib.request
from datetime import datetime, timezone
p = argparse.ArgumentParser(); p.add_argument("--events", type=int, default=1000)
p.add_argument("--url", default="http://localhost:5000/api/v1/events"); p.add_argument("--token", default="")
a = p.parse_args()
KINDS = [("shipment.delay", lambda: random.choice([1,2,30,80])), ("inventory.level", lambda: random.choice([0,12,90,150])),
         ("supplier.score", lambda: random.randint(35, 95))]
def ev():
    k, f = random.choice(KINDS)
    return {"eventId": str(uuid.uuid4()), "tenantId": 0, "kind": k, "entityKey": f"{k.split('.')[0]}-{random.randint(1,300)}",
            "value": f(), "baseline": 100, "at": datetime.now(timezone.utc).isoformat()}
for i in range(0, a.events, 500):
    body = json.dumps([ev() for _ in range(min(500, a.events - i))]).encode()
    r = urllib.request.Request(a.url, body, {"Content-Type": "application/json", "Authorization": f"Bearer {a.token}"})
    urllib.request.urlopen(r); print(f"sent {i+500}")
