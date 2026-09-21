# Baseline DLNA Fixtures

This fixture provides a small, secret-free catalog for manual checks on real DLNA
clients. It uses the production HTTP, SOAP, DIDL, media-proxy, eventing, and SSDP
implementations with a finite loopback fake upstream.

Generated media is intentionally not committed.

## Generate Media

From the repository root:

```sh
nix develop -c bash tests/fixtures/generate.sh /tmp/immich-dlna-fixtures
```

The output directory must not already exist. The script creates:

| File | Purpose |
| --- | --- |
| `original.jpg` | Original JPEG resource |
| `original-preview.jpg` | Preview for the original JPEG |
| `generated-display.jpg` | Generated full-size JPEG |
| `generated-preview.jpg` | Generated preview JPEG |
| `video-original.mp4` | Original H.264/AAC video |
| `video-playback.mp4` | Playback H.264/AAC alternative |

The images contain labeled color quadrants. The two 30-second videos have distinct
labels, dimensions, and tones so resource selection is observable.

Inspect the files before using them:

```sh
for file in /tmp/immich-dlna-fixtures/*; do
  nix develop -c ffprobe -v error -show_entries \
    stream=codec_name,profile,width,height,pix_fmt,sample_rate,channels:format=duration,size \
    -of json "$file"
  nix develop -c ffmpeg -v error -i "$file" -f null -
done
```

## Run

```sh
nix develop --command cargo run --example fixture-server -- \
  --listen 192.168.1.10:8200 \
  --fixtures /tmp/immich-dlna-fixtures
```

Replace the address with a concrete IPv4 address on an up, multicast-capable LAN
interface. Allow the selected TCP port and UDP 1900 only on the trusted interface.
Use an unused port if the production service is running.

The device appears as **DLNA Fixture Baseline**, containing one album named
**Mixed JPEG and H264 AAC**. The catalog has:

- An original JPEG with original and preview resources.
- A generated JPEG with display and preview resources.
- A video with original and playback resources.
- A playback-only appearance that makes the alternative selectable by clients
  which always choose the first resource.

The fixed device UUID is `30000000-0000-4000-8000-000000000001`. The fake upstream
is loopback-only and accepts only the fixture's synthetic API key.

## Manual Check

Verify:

1. The client discovers the device and opens the album.
2. Both image paths display with the expected labels and orientation.
3. The original video plays with the 440 Hz tone.
4. The playback-only video plays with the 880 Hz tone.
5. Pause/resume and forward/backward seeking work for both videos.
6. Debug logs show the expected resource URL and valid byte-range responses.

Client resource selection and automatic fallback are not guaranteed. Record the
actual requested URL rather than inferring selection from the visible item.

For direct transport checks, substitute any advertised media URL:

```sh
url=http://192.168.1.10:8200/media/assets/20000000-0000-4000-8000-000000000003/playback
curl --fail --head "$url"
curl --fail --dump-header - --range 0-1023 "$url" --output /dev/null
curl --fail "$url" | sha256sum
```

## Automated Smoke Tests

```sh
nix develop -c cargo test --test fixture_server
```

The two tests verify description and Browse wiring, then GET/HEAD/range delivery
for all six representations. Detailed protocol, validation, timeout, and failure
matrices remain in the production module tests rather than being duplicated here.
