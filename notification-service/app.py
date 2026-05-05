"""
Notification Service — async event consumer.

Subscribes to the reservation-service event bus (ZeroMQ PUB/SUB,
simulating RabbitMQ choreography from the Part 3 design).

Handles: ReservationCreated, ReservationCancelled,
         ReservationArrived, ReservationSeated, ReservationCompleted
"""

import zmq
import json
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [notification-service] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

RESERVATION_PUB_URL = os.getenv("RESERVATION_PUB_URL", "tcp://reservation-service:5557")

HANDLED_EVENTS = {
    "ReservationCreated",
    "ReservationCancelled",
    "ReservationArrived",
    "ReservationSeated",
    "ReservationCompleted",
}


def notify(event_type: str, data: dict):
    """Simulate outbound notifications (email / SMS / push)."""
    res_id = data.get("reservation_id", "?")
    name   = data.get("customer_name", "Customer")
    email  = data.get("customer_email", "N/A")

    if event_type == "ReservationCreated":
        log.info(
            "[EMAIL → %s] Booking confirmed — %s | %s %s | Table %s | Party of %d",
            email, res_id,
            data.get("reservation_date", ""), data.get("reservation_time", ""),
            data.get("table_number", "?"), data.get("party_size", 0),
        )
    elif event_type == "ReservationCancelled":
        log.info("[EMAIL → %s] Booking %s has been cancelled.", email, res_id)
    elif event_type == "ReservationArrived":
        log.info("[STAFF DISPLAY] Reservation %s — guests have arrived.", res_id)
    elif event_type == "ReservationSeated":
        log.info("[STAFF DISPLAY] Reservation %s — guests are seated.", res_id)
    elif event_type == "ReservationCompleted":
        log.info("[LOYALTY TRIGGER] Reservation %s completed — award loyalty points.", res_id)


def main():
    context = zmq.Context()
    socket  = context.socket(zmq.SUB)
    socket.connect(RESERVATION_PUB_URL)
    socket.setsockopt_string(zmq.SUBSCRIBE, "")  # subscribe to all topics

    log.info("Notification Service subscribed to event bus at %s", RESERVATION_PUB_URL)

    while True:
        raw = socket.recv_string()
        log.info("Event received → %s", raw)
        try:
            msg = json.loads(raw)
            event_type = msg.get("event_type", "")
            if event_type in HANDLED_EVENTS:
                notify(event_type, msg.get("data", {}))
            else:
                log.debug("Ignoring event: %s", event_type)
        except json.JSONDecodeError:
            log.warning("Received invalid JSON")


if __name__ == "__main__":
    main()
