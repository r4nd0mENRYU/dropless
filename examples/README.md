# Examples

A 60-second, end-to-end walkthrough of Dropless on your machine.

## 1. Start a receiver that verifies signatures

```sh
python3 examples/receiver.py 9000
# listens on http://localhost:9000 and verifies the Svix-compatible signature
```

## 2. Start Dropless

```sh
docker compose up --build      # Postgres + Dropless on http://localhost:8080
```

## 3. Run the quickstart

```sh
bash examples/quickstart.sh
```

It creates a tenant API key and an endpoint pointing at the receiver, publishes
an event, and shows the delivery — which the receiver verifies and prints.

You can also open the dashboard at **http://localhost:8080**, paste the key the
script printed, and replay the delivery from the UI.
