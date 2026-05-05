"""
Test client for the Reservation Service.
Run after: docker compose up -d
Usage:     python3 test_client.py
"""

import zmq
import json
import time
import uuid
from datetime import datetime, timezone

RESERVATION_SVC_URL = "tcp://localhost:5556"


def make_envelope(event_type: str, data: dict) -> dict:
    """Standard event envelope."""
    return {
        "event_type": event_type,
        "event_id":   str(uuid.uuid4()),
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "version":    "1.0",
        "data":       data,
    }


def call(socket: zmq.Socket, event_type: str, data: dict) -> dict:
    envelope = make_envelope(event_type, data)
    print(f"\n→ {json.dumps(envelope, indent=2)}")
    socket.send_string(json.dumps(envelope))
    response = json.loads(socket.recv_string())
    print(f"← {json.dumps(response, indent=2)}")
    return response


def main():
    context = zmq.Context()
    socket  = context.socket(zmq.REQ)
    socket.connect(RESERVATION_SVC_URL)
    print(f"Connected to Reservation Service at {RESERVATION_SVC_URL}")
    print("=" * 60)

    # 1. Create reservation — Alice (party of 4, date far enough for 24h policy)
    print("\n[1] CreateReservation — Alice Johnson, party of 4")
    r1 = call(socket, "CreateReservation", {
        "customer_id":         "CUST-001",
        "customer_name":       "Alice Johnson",
        "customer_email":      "alice@example.com",
        "restaurant_location": "Downtown Branch",
        "party_size":          4,
        "reservation_date":    "2026-07-01",
        "reservation_time":    "19:00:00",
    })
    time.sleep(0.3)

    # 2. Create reservation — Bob (party of 2)
    print("\n[2] CreateReservation — Bob Smith, party of 2")
    r2 = call(socket, "CreateReservation", {
        "customer_id":         "CUST-002",
        "customer_name":       "Bob Smith",
        "customer_email":      "bob@example.com",
        "restaurant_location": "Downtown Branch",
        "party_size":          2,
        "reservation_date":    "2026-07-01",
        "reservation_time":    "20:00:00",
    })
    time.sleep(0.3)

    # 3. Staff action: CONFIRMED → ARRIVED → SEATED (Alice's reservation)
    if r1.get("event_type") == "ReservationConfirmed":
        res_id = r1["data"]["reservation_id"]

        print(f"\n[3] UpdateReservationStatus — {res_id}: CONFIRMED → ARRIVED")
        r_arrived = call(socket, "UpdateReservationStatus", {
            "reservation_id": res_id,
            "status": "ARRIVED",
        })
        time.sleep(0.3)

        if r_arrived.get("event_type") == "StatusUpdated":
            print(f"\n[4] UpdateReservationStatus — {res_id}: ARRIVED → SEATED")
            call(socket, "UpdateReservationStatus", {
                "reservation_id": res_id,
                "status": "SEATED",
            })
            time.sleep(0.3)

    # 4. Cancel Bob's reservation (within 24h policy — date is far ahead)
    if r2.get("event_type") == "ReservationConfirmed":
        res_id_2 = r2["data"]["reservation_id"]
        print(f"\n[5] CancelReservation — {res_id_2} (Bob)")
        call(socket, "CancelReservation", {"reservation_id": res_id_2})
        time.sleep(0.3)

    # 5. Try party too large for any table
    print("\n[6] CreateReservation — party of 10 (no table available — expect failure)")
    call(socket, "CreateReservation", {
        "customer_id":         "CUST-003",
        "customer_name":       "Large Group",
        "customer_email":      "group@example.com",
        "restaurant_location": "Downtown Branch",
        "party_size":          10,
        "reservation_date":    "2026-07-01",
        "reservation_time":    "18:00:00",
    })
    time.sleep(0.3)

    # 6. List all reservations
    print("\n[7] ListReservations")
    call(socket, "ListReservations", {})

    socket.close()
    context.term()


if __name__ == "__main__":
    main()
