import zmq
import json
import logging
import os
import uuid
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [table-service] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

PORT = int(os.getenv("SERVICE_PORT", "5555"))

# In-memory table state — keyed by table_number
tables = {
    "T1": {"table_number": "T1", "seats": 2, "available": True, "location": "Window"},
    "T2": {"table_number": "T2", "seats": 4, "available": True, "location": "Main Hall"},
    "T3": {"table_number": "T3", "seats": 4, "available": True, "location": "Main Hall"},
    "T4": {"table_number": "T4", "seats": 6, "available": True, "location": "Private"},
    "T5": {"table_number": "T5", "seats": 8, "available": True, "location": "Private"},
}


def envelope(event_type: str, data: dict) -> dict:
    return {
        "event_type": event_type,
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "1.0",
        "data": data,
    }


def handle(msg: dict) -> dict:
    event_type = msg.get("event_type", "")
    data = msg.get("data", {})

    if event_type == "CheckAvailability":
        party_size = data.get("party_size", 1)
        available = [t for t in tables.values() if t["available"] and t["seats"] >= party_size]
        return envelope("AvailabilityResult", {"available_tables": available})

    if event_type == "ReserveTable":
        table_number = data.get("table_number", "")
        if table_number not in tables:
            return envelope("ReserveTableFailed", {"reason": f"Table {table_number} not found"})
        if not tables[table_number]["available"]:
            return envelope("ReserveTableFailed", {"reason": f"Table {table_number} is already reserved"})
        tables[table_number]["available"] = False
        log.info("Reserved table %s", table_number)
        return envelope("TableReserved", {"table_number": table_number})

    if event_type == "ReleaseTable":
        table_number = data.get("table_number", "")
        if table_number not in tables:
            return envelope("ReleaseTableFailed", {"reason": f"Table {table_number} not found"})
        tables[table_number]["available"] = True
        log.info("Released table %s", table_number)
        return envelope("TableReleased", {"table_number": table_number})

    if event_type == "ListTables":
        return envelope("TableList", {"tables": list(tables.values())})

    return envelope("Error", {"reason": f"Unknown event_type: {event_type}"})


def main():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://*:{PORT}")
    log.info("Table Service listening on port %d", PORT)

    while True:
        raw = socket.recv_string()
        log.info("Request  → %s", raw)
        try:
            response = handle(json.loads(raw))
        except json.JSONDecodeError:
            response = envelope("Error", {"reason": "Invalid JSON"})
        out = json.dumps(response)
        socket.send_string(out)
        log.info("Response ← %s", out)


if __name__ == "__main__":
    main()
