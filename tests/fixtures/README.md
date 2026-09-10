# Generated Baseline Fixtures

These synthetic fixtures define the blocking baseline for the
LG C4/desktop gate. They require no
Immich account, production server, media library, or downloaded sample media.
Only the recipe, manifest, and test catalog belong in version control. Do not
commit generated JPEG/MP4 files.

## Generation

Run from the repository root with its locked Nix development environment:

```sh
ls /tmp
nix develop -c bash tests/fixtures/generate.sh /tmp/immich-dlna-fixtures
```

The single required argument is a **new directory** beneath an existing parent.
Existing directories, files, and symlinks are rejected, even an empty directory.
Every ffmpeg invocation also uses `-n`. Failed generation leaves its partial
directory for inspection; retry with a new path, not an overwrite switch.
The script writes only into the requested directory; Nix may populate its store.
No production configuration or service is used.

The development shell supplies `ffmpeg` and `ffprobe`. The script obtains DejaVu
Sans Mono from `dejavu_fonts` in the repository's locked nixpkgs input, not host
font discovery.

There is no random source or wall-clock metadata. Encoders and filters use one
thread and explicit settings. Record `flake.lock`, platform, `ffmpeg -version`,
and `sha256sum /tmp/immich-dlna-fixtures/*` with each evidence bundle. The recipe
is reproducible with the same toolchain/platform; byte identity across different
CPU architectures or encoder revisions is not an acceptance assumption.

## Manifest

The album UUID is `10000000-0000-4000-8000-000000000001`.
Its DLNA ID (`ALBUM_ID`) is `album:10000000-0000-4000-8000-000000000001`.
Root ID/parent are `0`/`-1`; album parent is `0`; each item ID is
`<ALBUM_ID>:asset:<asset UUID>` with parent `<ALBUM_ID>`.

| Asset | Fixed UUID | Advertised resources, in order |
| --- | --- | --- |
| Unedited original JPEG | `20000000-0000-4000-8000-000000000001` | original, preview |
| Generated JPEG image | `20000000-0000-4000-8000-000000000002` | display, preview; no original |
| H.264/AAC video | `20000000-0000-4000-8000-000000000003` | original, playback |

The extra **Playback Only - H264 AAC.mp4** item uses appearance UUID
`20000000-0000-4000-8000-000000000008` and one resource: the existing video's
`/media/assets/20000000-0000-4000-8000-000000000003/playback` URL. This test-only
appearance adds no upstream asset or file; the original-first item is unchanged.

All six files below are blocking baseline representations. Endpoint suffixes are
relative to the fake upstream's `/api/assets/<asset UUID>/`.

| Filename | Asset | MIME | Dimensions | Upstream endpoint and query | Visible label / expected output |
| --- | --- | --- | --- | --- | --- |
| `original.jpg` | Original JPEG | `image/jpeg` | 960x640 | `original` | `ORIGINAL JPEG`, upright color grid |
| `original-preview.jpg` | Original JPEG | `image/jpeg` | 480x320 | `thumbnail?size=preview&edited=true` | `ORIGINAL PREVIEW`, smaller upright grid |
| `generated-display.jpg` | Generated JPEG | `image/jpeg` | 960x640 | `thumbnail?size=fullsize&edited=true` | `GENERATED DISPLAY`, upright grid |
| `generated-preview.jpg` | Generated JPEG | `image/jpeg` | 480x320 | `thumbnail?size=preview&edited=true` | `GENERATED PREVIEW`, smaller upright grid |
| `video-original.mp4` | Video | `video/mp4` | 640x360 | `original` | `ORIGINAL 640x360`, moving test pattern and frame counter |
| `video-playback.mp4` | Video | `video/mp4` | 480x270 | `video/playback` | `PLAYBACK 480x270`, moving test pattern and frame counter |

JPEGs have red top-left, green top-right, blue bottom-left, and yellow bottom-right
quadrants, each with an explicit position/color label. They have no EXIF rotation;
this baseline does not test orientation-tag handling. Generated renditions are
synthetic stand-ins, not proof of Immich edit generation.

Both MP4s are 30 seconds, 30 fps (900 frames), square pixels, 16:9, H.264 Constrained
Baseline level 3.0, 8-bit `yuv420p`, AAC-LC stereo at 48 kHz/128 kbit/s, with
one-second keyframe intervals and `moov` before `mdat` (faststart). Original audio
is a continuous 440 Hz sine in both channels; playback audio is 880 Hz. Start at
low listening volume. Both should play smoothly with audible tone through C4
built-in speakers and the eARC soundbar. Seek forward to about 20 seconds
(frame 600), then backward to about 5 seconds (frame 150); movement and audio
must resume. Image audio/seek checks are not applicable.

## Catalog Contract

`tests/fixture_catalog.rs` is an integration-test module, also independently
testable with `nix develop -c cargo test --test fixture_catalog`. Include it in
the test-only fixture harness using `mod fixture_catalog;` from a sibling test
file. It imports `immich_dlna_proxy::protocol::{Object, Resource}`.

```rust
pub fn objects(http_address: std::net::SocketAddrV4) -> Vec<Object>;
pub const ALBUM_ID: &str;
pub const ORIGINAL_JPEG_ID: &str;
pub const GENERATED_JPEG_ID: &str;
pub const VIDEO_ID: &str;
pub fn media_file(
    asset: &str,
    endpoint: &str,
    query: Option<&str>,
) -> Option<(&'static str, &'static str)>;
```

`objects` returns root, one `object.container.album`, two
`object.item.imageItem.photo` items, then two `object.item.videoItem` appearances.
It uses absolute `http://<http_address>/media/assets/<UUID>/<representation>` URLs and
fixed dates; both video appearances share a date and sort by ID on ties. Only the
original video resource advertises duration `0:00:30.000`; resource
sizes, resolutions, and codec/DLNA profile claims are absent. The root has
`childCount=1`; the album omits it. Album artwork uses the original JPEG preview;
image artwork uses each image's preview. This small catalog omits video artwork.
There is no filesystem scan, upstream catalog fetch, production mode, or server
implementation here.

All video resources advertise `video/mp4` with DIDL `DLNA.ORG_OP=01` for byte
seeking. The playback-only resource has no duration, size, resolution, or profile.

`media_file` takes the canonical asset UUID, endpoint suffix such as
`video/playback` (no leading slash), and raw query without `?`. It returns a
relative filename and MIME, not file bytes or a response. The two thumbnail query
parameters may appear in either order; missing, duplicate, changed, or additional
parameters are rejected. Original/playback accept no query or an empty query.
Unknown assets/renditions return `None`; this is a deliberately finite fake
upstream, not a production authorization rule. The harness owns GET/HEAD/range
serving, request evidence, Browse, discovery, and events.

## Verification

Inspect each file locally before the device gate:

```sh
for file in /tmp/immich-dlna-fixtures/*; do
  nix develop -c ffprobe -v error -show_entries \
    stream=codec_name,profile,level,width,height,pix_fmt,r_frame_rate,sample_aspect_ratio,sample_rate,channels,nb_frames:format=duration,size \
    -of json "$file"
  nix develop -c ffmpeg -v error -i "$file" -f null -
done
```

Open all JPEGs and both videos in the selected desktop client, checking the
labels, quadrants, tone, and frame counter. Record the exact client/version, not
just the operating system. The hardware gate also covers discovery, root/album
navigation, and visibility of all four mixed-media items with `Filter=*`.

**Verify every alternative independently with direct requests and byte ranges.**
Do not infer successful preview/playback delivery from one successful original.
For each of the six local resource URLs from the manifest, run the following,
substituting the harness's actual LAN address and the chosen resource:

```sh
url=http://192.0.2.10:8200/media/assets/20000000-0000-4000-8000-000000000003/playback
curl --fail --head "$url"
curl --fail --dump-header - --range 0-1023 "$url" --output /dev/null
curl --fail --dump-header - --range 1024- "$url" --output /dev/null
curl --fail --dump-header - --range -1024 "$url" --output /dev/null
curl --fail "$url" | sha256sum
```

Expect HEAD 200, the manifest MIME and actual full length; range GETs 206 with
correct `Content-Range`/`Content-Length` and bytes matching that file's slice.
The full GET hash must equal the corresponding generated file hash. Also request
an offset beyond the file length and expect 416 with `Content-Range: bytes */N`.
Capture the same checks against the fake upstream to distinguish fixture-server
failures from proxy failures. Directly open both original and playback URLs in a
desktop player and repeat forward/backward seeking; ranges alone do not prove
audio playback or a client's seek behavior.

For normal DLNA browsing, record the **actual requested resource URL** from HTTP
logs alongside the visible label. Artwork GETs are not proof that a preview was
selected for full-screen display. Clients may always select the first resource;
alternative selection or automatic fallback is **not guaranteed**. A second
resource never requested by the C4 remains unverified on that device, even if
direct HTTP and desktop checks pass.

For the independent playback check, select **DLNA Fixture Baseline** > **Mixed
JPEG and H264 AAC** > **Playback Only - H264 AAC.mp4**. Expect `PLAYBACK 480x270`
and the 880 Hz tone, and confirm the `/playback` request in the logs. Test built-in
speakers and eARC, pause/resume, and forward/backward seeking. This does not test
automatic alternative selection or fallback from the original-first item.

On the tested LG OLED77C45LA.DEUQLJP, webOS25/platform10.3.1/firmware33.31.68,
both original and playback-only videos played with sound, forward/backward seeking
and pause/resume. Requests for both representations produced validated byte-range
responses. This verifies independent playback, not automatic fallback.

Automated fixture regressions (including counts, sorting, playback-only metadata,
GET/HEAD/ranges, and the seek/JPEG experiments):

```sh
nix develop -c cargo test --test fixture_catalog --test fixture_server
nix develop -c cargo fmt --all -- --check
```

## Gate Evidence

Leave results blank until observed. Use one row per representation, client, and
audio path; duplicate the blank row as needed. Retain request/range logs and
screenshots or observations with the generated-file hashes.

| Operator | Client / exact regional model | Version / firmware | Fixture filename / hash | Audio path | Actual selected resource URL | Display / audio result | Forward / backward seek result | HTTP / range evidence | Pass / fail / unverified |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | | | | | | |

| Gate decision | Approver | Blocking failures / unavailable devices | Evidence reference |
| --- | --- | --- | --- |
| | | | |

Baseline failures block the gate pending diagnosis or an explicit project-owner
acceptance change. Investigate the same sample bytes and transport evidence
before blaming a client; do not silently reclassify a failed baseline. Unavailable
C4/eARC access is an unverified gate requiring an explicit owner decision.

EXIF rotation, real edited/HEIC derivatives, panoramas, unusual video/audio
codecs, HDR, portrait video, and large files are **exploratory**, outside this
generated manifest. Add such cases only with an agreed file identifier, expected
outcome, and classification. Passing this small baseline does not certify those
formats or guarantee useful multi-resource selection on every client.

## Run The Server

```sh
nix develop --command cargo run --example fixture-server -- \
  --listen 192.168.1.10:8200 --fixtures /tmp/immich-dlna-fixtures
```

Replace the address with the server's concrete LAN IPv4 address. The selected
interface must be up and multicast-capable. Allow TCP 8200 and UDP 1900 only on
that trusted interface; the executable does not modify the firewall. The TV must
be on a network that permits SSDP multicast discovery of the server.
Use an unused TCP port if the production service is running; leave that service
and its configuration unchanged. No experiment flag is needed for playback-only.

The device is named **DLNA Fixture Baseline**, containing **Mixed JPEG and H264
AAC**, with fixed device UUID `30000000-0000-4000-8000-000000000001`.
A loopback-only fake Immich serves the six files through the production
proxy transport. No live API key or Immich connection is used. Fixed revisions
are valid only for this immutable test catalog. The example is not installed as
the production service.

Keep the terminal output to identify requested resources and ranges. Use Ctrl-C
to withdraw discovery advertisements and stop; admitted requests get at most ten
seconds to finish. Restart with the same fixed fixture device identity. Since the
catalog uses fixed revisions, re-enter or refresh the fixture listing if a client
has cached the earlier three-item album.

The example enables debug diagnostics. Media logs distinguish incoming Range
headers from forwarded ranges and report whether DLNA capability or time-seek
headers were present, without dumping their arbitrary values. Browse logs report
whether the requested Filter selects resources, artwork, and duration. These observations
do not add seek capabilities or change the response metadata.

## Album-Art Signaling Comparison

```sh
nix develop --command cargo run --example fixture-server -- \
  --listen 192.168.1.10:8400 --fixtures /tmp/immich-dlna-fixtures --album-art-test
```

This mode is mutually exclusive with the other experiments. It reuses the six
baseline files and advertises **DLNA Album Art A-B**, device UUID ending in `0006`.
Its two generic mixed-media albums contain the same four baseline items:

| Album | Artwork Signaling |
| --- | --- |
| A - Art URI Only | Plain `upnp:albumArtURI` |
| B - Art URI Plus Resource | The same property plus a cover `<res protocolInfo="http-get:*:image/jpeg:*">` |

Album UUIDs end in `0201`/`0202`. Cover asset aliases also end in `0201`/`0202`;
both serve the identical `generated-preview.jpg`. Within each album its cover
URL equals the second image's preview; between albums the URLs differ to avoid
cross-populating their cover caches. The expected cover reads **GENERATED PREVIEW**.
Only B adds the album resource. Neither cover claims a DLNA profile, dimensions,
duration, or size. This is an interoperability experiment, not production policy.

On first opening the device, wait on the album list before entering either album.
Record whether each cover appears and whether either cover URL is requested.
Then enter A, return, and record both tiles; repeat for B. Confirm both albums
still open normally, contain four items, and allow image viewing/video playback.
Distinguish early cover fetching from displaying an already-cached image. Repeated
runs with these fixed IDs are warm-cache observations, not fresh comparisons.

On LG OLED77C45LA.DEUQLJP, webOS25/platform10.3.1/firmware33.31.68, neither
album displayed a cover before entry. Each cover was requested only after entering
its album and appeared on return. Both remained folders with four items, and
sampled photo/video playback worked. The added album resource did not improve
initial cover loading in this comparison; it is not a production workaround.

## Byte-Seek Signaling Matrix

The fixture-only `--seek-test` experiment separates HTTP and DIDL byte-seek signals:

```sh
nix develop --command cargo run --example fixture-server -- \
  --listen 192.168.1.10:8200 --fixtures /tmp/immich-dlna-fixtures --seek-test
```

It advertises **DLNA Seek Matrix**, using a distinct device UUID ending in `0003`
to avoid reusing cached earlier listings. Open **Seek Signaling A-D**. The usual
baseline items remain, with four additional videos:

| Video | Media Path | DIDL Fourth Protocol Field | HTTP Content-Features |
| --- | --- | --- | --- |
| A - Control.mp4 | `http://192.168.1.10:8201/control` | `*` | Absent |
| B - HTTP byte seek.mp4 | `http://192.168.1.10:8201/byte-seek` | `*` | `DLNA.ORG_OP=01` |
| C - DIDL and HTTP byte seek.mp4 | `http://192.168.1.10:8201/both` | `DLNA.ORG_OP=01` | `DLNA.ORG_OP=01` |
| D - DIDL byte seek.mp4 | `http://192.168.1.10:8201/didl-only` | `DLNA.ORG_OP=01` | Absent |

All use the **identical original MP4**, one resource, the same duration and MIME.
A/B retain generic DIDL; C/D explicitly advertise byte seeking in the fourth
`protocolInfo` field. Their media responses use the same bounded
listener and real proxy. The extra listener uses the configured LAN address and
the next TCP port, so that port must also be reachable. Both listeners bind before
discovery starts and retain the upstream until admitted streams drain.

Test C, D and A: play and click the timeline near 20 seconds and then 5 seconds.
Record warnings, actual position changes, audio, and request evidence. The visible
video label and tone are intentionally identical. B/C add the HTTP header only on
200/206 responses. No case claims time seeking, a codec profile, conversion state,
or a broader capability mask. Normal baseline video resources advertise DIDL
OP=01; within the four comparison items, only C/D set the byte-seek field.

A is the no-signal control; B isolates HTTP-only, D isolates DIDL-only, and C tests
their combination. Compare observed results rather than assuming a flag is
sufficient. This is a diagnostic fixture, not a production advertising decision
or a promise that arbitrary upstream resources support seeking.

## JPEG Label Comparison

This separate fixture experiment tests two known JPEG payloads mislabeled as PNG,
without changing their bytes or the live server. The input directory contains
`image-1.jpg` and `image-2.jpg`, with hashes matching the inspected originals. These
are private test copies, not committed assets. No encoding or conversion is performed.

```sh
nix develop --command cargo run --example fixture-server -- \
  --listen 192.168.1.10:8400 --fixtures /tmp/jpeg-label-fixtures --jpeg-label-test
```

This mode is mutually exclusive with `--seek-test` and needs only those two files.
It advertises **DLNA JPEG Label Test**, UUID ending in `0005`, containing
**Same Bytes - PNG vs JPEG**. The normal proxy can remain running on another port.

| Entry | Input Bytes | DIDL MIME and HTTP Content-Type |
| --- | --- | --- |
| 1A - PNG label | image-1.jpg | image/png |
| 1B - JPEG label | image-1.jpg | image/jpeg |
| 2A - PNG label | image-2.jpg | image/png |
| 2B - JPEG label | image-2.jpg | image/jpeg |

Each entry has one original resource and no artwork or preview alternative. Titles
and resource URLs contain no filename extensions. Both MIME
labels change together; this test does not isolate DIDL from HTTP interpretation.
Verify all four served hashes and the expected labels before the TV test. Open
both B entries and the A controls, recording full-screen display or failure.

If B works while the corresponding A fails, the unchanged payload is displayable
when labeled correctly. If A also works or B also fails, report that result rather
than assuming the label is the sole cause. This is neither a genuine PNG test nor
a production MIME override, and it does not modify any upstream asset or setting.

On the tested LG OLED77C45LA.DEUQLJP, webOS25/platform10.3.1/firmware33.31.68,
both JPEG-labeled original payloads displayed and both PNG-labeled controls failed.
The same bytes therefore need correct labels, not conversion, for this comparison.
This result does not establish genuine PNG compatibility or identify which label
location the client uses; DIDL and HTTP were changed together.
