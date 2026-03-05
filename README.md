# Delphix DCT Seminar Demos

Two small demos that use the Delphix DCT SDKs to list VDBs and optionally refresh a VDB to the latest timestamp.

## Go demo

Lists VDB name, size, and storage size. If you pass a VDB name or ID as an argument, it refreshes that VDB and waits for completion (name also works as ID).

Run:

```bash
export DCT_ENGINE_URL="https://your-engine.example.com"
export DCT_API_KEY="Bearer YOUR_API_KEY"

cd delphix/seminar/go
go run .
go run . "prod01-copy01"
```

## Python demo (uv)

Same behavior as the Go demo. SSL verification is disabled by default; set `DCT_INSECURE=0` to enable it or `DCT_CA_CERT` for a custom CA bundle.

Run:

```bash
export DCT_ENGINE_URL="https://your-engine.example.com"
export DCT_API_KEY="apk YOUR_API_KEY"
# Optional:
# export DCT_INSECURE=0
# export DCT_CA_CERT="/path/to/ca-bundle.pem"

cd delphix/seminar/python
uv run python main.py
uv run python main.py "prod01-copy01"
```
