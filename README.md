# Restaurant Booking Management — Microservices Prototype

A containerised microservices prototype for a restaurant chain booking system, built as part of a Microservice Architecture course.

## Architecture

```
Client (host)
    │ ZeroMQ REQ  tcp://localhost:5556
    ▼
┌────────────────────────── restaurant-net (isolated bridge) ────────────────────────────┐
│                                                                                        │
│  ┌──────────────────────────┐   ZeroMQ REQ/REP    ┌──────────────────────────────┐   │
│  │   reservation-service    │ ──────────────────▶  │       table-service          │   │
│  │   REP :5556  (clients)   │ ◀──────────────────  │   REP :5555  (internal)      │   │
│  │   PUB :5557  (events)    │                      │   not exposed to host        │   │
│  └────────────┬─────────────┘                      └──────────────────────────────┘   │
│               │ ZeroMQ PUB                                                             │
│               ▼                                                                        │
│  ┌──────────────────────────┐                                                         │
│  │   notification-service   │  subscribes to domain events, simulates email/SMS       │
│  │   SUB :5557              │                                                         │
│  └──────────────────────────┘                                                         │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### Communication patterns

| Path | Pattern | Protocol | Maps to |
|------|---------|----------|---------|
| Client → reservation-service | Request-Reply | ZeroMQ REQ/REP | REST in full design |
| reservation-service → table-service | Request-Reply | ZeroMQ REQ/REP | gRPC internal call |
| reservation-service → notification-service | Publish-Subscribe | ZeroMQ PUB/SUB | RabbitMQ choreography |

### Message format

All messages use a standard event envelope:

```json
{
  "event_type": "ReservationConfirmed",
  "event_id":   "2c652781-f614-4973-91f3-80df91d069e0",
  "timestamp":  "2026-05-05T11:59:31.008596+00:00",
  "version":    "1.0",
  "data":       { ... }
}
```

### Reservation state machine

```
PENDING ──▶ CONFIRMED ──▶ ARRIVED ──▶ SEATED ──▶ COMPLETED
                │
                └──▶ CANCELLED  (allowed up to 24 h before reservation time)
```

## Services

### reservation-service
Core domain service (Reservation Aggregate).

| Event type (in) | Description |
|----------------|-------------|
| `CreateReservation` | Check table availability, reserve table, transition PENDING → CONFIRMED |
| `CancelReservation` | Release table, apply 24h cancellation policy, transition → CANCELLED |
| `UpdateReservationStatus` | Staff-driven transitions: CONFIRMED → ARRIVED → SEATED → COMPLETED |
| `ListReservations` | List all reservations, optionally filtered by `customer_id` |

### table-service
Manages physical table availability. Called only by reservation-service (not exposed to clients).

| Event type (in) | Description |
|----------------|-------------|
| `CheckAvailability` | Return tables with `seats >= party_size` |
| `ReserveTable` | Mark a table as unavailable |
| `ReleaseTable` | Mark a table as available again |
| `ListTables` | Return full table inventory |

### notification-service
Async event consumer. Subscribes to all events published by reservation-service and simulates outbound notifications.

| Event type (in) | Action |
|----------------|--------|
| `ReservationCreated` | Simulate confirmation email to customer |
| `ReservationCancelled` | Simulate cancellation email |
| `ReservationArrived` | Simulate staff display update |
| `ReservationSeated` | Simulate staff display update |
| `ReservationCompleted` | Trigger loyalty points award |

## Running the prototype

**Prerequisites:** Docker and Docker Compose installed.

```bash
# Start all services
docker compose up -d

# View logs (all services)
docker compose logs -f

# Run the test client
python3 client/test_client.py

# Stop and remove containers
docker compose down
```

The test client requires `pyzmq`:

```bash
pip install pyzmq
```

## Project structure

```
.
├── docker-compose.yml          # service definitions + isolated network
├── reservation-service/
│   ├── app.py                  # state machine, orchestrates table-service calls
│   ├── Dockerfile
│   └── requirements.txt
├── table-service/
│   ├── app.py                  # table inventory management
│   ├── Dockerfile
│   └── requirements.txt
├── notification-service/
│   ├── app.py                  # event subscriber, simulates notifications
│   ├── Dockerfile
│   └── requirements.txt
└── client/
    └── test_client.py          # manual / integration test client
```
