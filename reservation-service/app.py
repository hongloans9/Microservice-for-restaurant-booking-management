"""
Reservation Service — core domain service.

Communication:
  REP on RESERVATION_SVC_PORT  : client-facing (simulates REST endpoint from design)
  REQ to TABLE_SVC_URL         : internal call to table-service (simulates gRPC from design)
  PUB on EVENT_PUB_PORT        : domain event bus (simulates RabbitMQ publish from design)

State machine:
  PENDING → CONFIRMED (table reserved successfully)
  CONFIRMED → ARRIVED → SEATED → COMPLETED  (staff-driven)
  CONFIRMED → CANCELLED  (24h cancellation policy)
"""

import zmq
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [reservation-service] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

TABLE_SVC_URL      = os.getenv("TABLE_SVC_URL",      "tcp://table-service:5555")
RESERVATION_SVC_PORT = int(os.getenv("RESERVATION_SVC_PORT", "5556"))
EVENT_PUB_PORT     = int(os.getenv("EVENT_PUB_PORT",  "5557"))

reservations: dict[str, dict] = {}
_counter = 0

# Valid staff-driven status progressions
STAFF_TRANSITIONS: dict[str, str] = {
    "CONFIRMED": "ARRIVED",
    "ARRIVED":   "SEATED",
    "SEATED":    "COMPLETED",
}


def make_envelope(event_type: str, data: dict) -> dict:
    """Standard event envelope."""
    return {
        "event_type": event_type,
        "event_id":   str(uuid.uuid4()),
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "version":    "1.0",
        "data":       data,
    }


class ReservationService:
    def __init__(self):
        self.context = zmq.Context()

        # Client-facing REP socket (simulates REST in design)
        self.rep = self.context.socket(zmq.REP)
        self.rep.bind(f"tcp://*:{RESERVATION_SVC_PORT}")

        # Internal REQ socket → table-service (simulates gRPC in design)
        self.req = self.context.socket(zmq.REQ)
        self.req.connect(TABLE_SVC_URL)

        # Event PUB socket (simulates RabbitMQ publisher in design)
        self.pub = self.context.socket(zmq.PUB)
        self.pub.bind(f"tcp://*:{EVENT_PUB_PORT}")

        log.info("Reservation Service REP on port %d", RESERVATION_SVC_PORT)
        log.info("Table Service (internal gRPC-like): %s", TABLE_SVC_URL)
        log.info("Event bus PUB on port %d", EVENT_PUB_PORT)

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _call_table(self, event_type: str, **data) -> dict:
        """Synchronous internal call to table-service (gRPC-like REQ/REP)."""
        msg = make_envelope(event_type, data)
        self.req.send_string(json.dumps(msg))
        return json.loads(self.req.recv_string())

    def _publish(self, event_type: str, data: dict):
        """Publish domain event to async bus (RabbitMQ-like PUB)."""
        event = make_envelope(event_type, data)
        self.pub.send_string(json.dumps(event))
        log.info("Published → %s", event_type)

    # ── State machine transitions ─────────────────────────────────────────────

    def create_reservation(self, data: dict) -> dict:
        global _counter

        customer_id          = data.get("customer_id", "CUST-UNKNOWN")
        customer_name        = data.get("customer_name", "Unknown")
        customer_email       = data.get("customer_email", "")
        restaurant_location  = data.get("restaurant_location", "Main Branch")
        party_size           = data.get("party_size", 2)
        reservation_date     = data.get("reservation_date", "")
        reservation_time     = data.get("reservation_time", "")

        # --- State: PENDING (reservation created, awaiting table) ---
        _counter += 1
        reservation_id = f"RES-{_counter:05d}"
        log.info("Reservation %s PENDING for %s", reservation_id, customer_name)

        # Internal gRPC-like call: check availability
        avail = self._call_table("CheckAvailability", party_size=party_size)
        available_tables = avail.get("data", {}).get("available_tables", [])
        if not available_tables:
            return make_envelope("CreateReservationFailed", {
                "reservation_id": reservation_id,
                "reason": "No tables available for the requested party size",
            })

        table_number = available_tables[0]["table_number"]

        # Internal gRPC-like call: reserve the table
        reserve = self._call_table("ReserveTable", table_number=table_number)
        if reserve.get("event_type") != "TableReserved":
            return make_envelope("CreateReservationFailed", {
                "reservation_id": reservation_id,
                "reason": reserve.get("data", {}).get("reason", "Table reservation failed"),
            })

        # --- State: CONFIRMED ---
        reservation = {
            "reservation_id":      reservation_id,
            "customer_id":         customer_id,
            "customer_name":       customer_name,
            "customer_email":      customer_email,
            "restaurant_location": restaurant_location,
            "table_number":        table_number,
            "party_size":          party_size,
            "reservation_date":    reservation_date,
            "reservation_time":    reservation_time,
            "status":              "CONFIRMED",
            "created_at":          datetime.now(timezone.utc).isoformat(),
        }
        reservations[reservation_id] = reservation
        log.info("Reservation %s CONFIRMED (table %s)", reservation_id, table_number)

        # Publish domain event → Notification Service picks this up
        self._publish("ReservationCreated", reservation)

        return make_envelope("ReservationConfirmed", reservation)

    def cancel_reservation(self, data: dict) -> dict:
        reservation_id = data.get("reservation_id", "")

        if reservation_id not in reservations:
            return make_envelope("CancelReservationFailed",
                                 {"reason": f"Reservation {reservation_id} not found"})

        res = reservations[reservation_id]
        if res["status"] == "CANCELLED":
            return make_envelope("CancelReservationFailed",
                                 {"reason": "Reservation is already cancelled"})

        # Business rule: cancellation allowed up to 24 h before
        try:
            res_dt = datetime.fromisoformat(
                f"{res['reservation_date']}T{res['reservation_time']}"
            ).replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) > res_dt - timedelta(hours=24):
                return make_envelope("CancelReservationFailed", {
                    "reason": "Cancellation not allowed within 24 hours of reservation time",
                })
        except ValueError:
            pass  # skip date check if date format is missing/invalid

        # Internal gRPC-like call: release the table
        self._call_table("ReleaseTable", table_number=res["table_number"])

        reservations[reservation_id]["status"] = "CANCELLED"
        log.info("Reservation %s CANCELLED", reservation_id)

        self._publish("ReservationCancelled", {
            "reservation_id": reservation_id,
            "table_number":   res["table_number"],
        })

        return make_envelope("ReservationCancelled", {"reservation_id": reservation_id})

    def update_status(self, data: dict) -> dict:
        """Staff-driven transitions: CONFIRMED→ARRIVED→SEATED→COMPLETED."""
        reservation_id = data.get("reservation_id", "")
        new_status     = data.get("status", "").upper()

        if reservation_id not in reservations:
            return make_envelope("UpdateStatusFailed",
                                 {"reason": f"Reservation {reservation_id} not found"})

        current = reservations[reservation_id]["status"]
        if STAFF_TRANSITIONS.get(current) != new_status:
            return make_envelope("UpdateStatusFailed", {
                "reason": f"Invalid transition: {current} → {new_status}",
            })

        reservations[reservation_id]["status"] = new_status
        log.info("Reservation %s: %s → %s", reservation_id, current, new_status)

        self._publish(f"Reservation{new_status.capitalize()}", {
            "reservation_id": reservation_id,
            "status":         new_status,
        })
        return make_envelope("StatusUpdated", {
            "reservation_id": reservation_id,
            "status":         new_status,
        })

    def list_reservations(self, data: dict) -> dict:
        customer_id = data.get("customer_id")
        result = list(reservations.values())
        if customer_id:
            result = [r for r in result if r["customer_id"] == customer_id]
        return make_envelope("ReservationList", {"reservations": result})

    # ── Request dispatcher ────────────────────────────────────────────────────

    def handle(self, msg: dict) -> dict:
        event_type = msg.get("event_type", "")
        data       = msg.get("data", {})
        handlers = {
            "CreateReservation":      self.create_reservation,
            "CancelReservation":      self.cancel_reservation,
            "UpdateReservationStatus": self.update_status,
            "ListReservations":       self.list_reservations,
        }
        handler = handlers.get(event_type)
        if handler:
            return handler(data)
        return make_envelope("Error", {"reason": f"Unknown event_type: {event_type}"})

    def run(self):
        while True:
            raw = self.rep.recv_string()
            log.info("Request  → %s", raw)
            try:
                response = self.handle(json.loads(raw))
            except Exception as exc:
                response = make_envelope("Error", {"reason": str(exc)})
            out = json.dumps(response)
            self.rep.send_string(out)
            log.info("Response ← %s", out)


if __name__ == "__main__":
    time.sleep(2)  # wait for table-service
    ReservationService().run()
