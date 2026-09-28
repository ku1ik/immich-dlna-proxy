# Immich DLNA Proxy

A read-only UPnP AV MediaServer facade for Immich, targeting modern televisions.
**Production integration is under verification**, not release-ready or fully hardware-tested.
One unprivileged Linux process exposes readable/shared albums, mixed photos/videos,
artwork, and streaming. Immich owns media/generation; access is HTTP API only.

## Access Boundary

Use **Immich >= 3.1.0** with one API key granting these four scopes:

| Scope | Use |
| --- | --- |
| `album.read` | Read albums and album-scoped searches |
| `asset.read` | Search members and existing encoded-video records |
| `asset.view` | Preview/full-size images and video playback |
| `asset.download` | Original media |

No administrator, system-settings, or write scopes are needed. Browsing uses version,
albums, and paginated metadata-search APIs without per-item detail/HEAD/media-body
preflights. Missing critical fields fail refreshes; newer versions are not certified.

**DLNA is unauthenticated: trusted LAN/media VLAN only, never the Internet.**
Album membership, visibility, and edit selection govern catalog advertising,
not byte authorization. Any reachable client knowing an asset UUID can request
`/media/assets/<UUID>/{original,display,preview,playback}` without first browsing.
Known URLs can still work after album removal, hiding, or editing, including an
unedited original omitted from the listing, if Immich permits the key to read it.
UUIDs are not authentication. The API key stays server-side.

Credentials stay within the configured origin/API prefix; ambient HTTP proxies are ignored.
Catalog/version redirects fail. Media redirects allow at most three hops within that
origin/prefix to the same asset's allowed endpoints, preserving edits/request semantics.
Cross-origin redirects, HTTPS downgrades, and loops fail; HTTPS validates certificates.

## Configuration

Required invocation: `immich-dlna-proxy --config /etc/immich-dlna-proxy.toml`.
No environment overrides or reload: restart after config/credential changes.
SIGTERM/SIGINT send best-effort SSDP departure announcements and exit successfully,
without draining active requests or playback.
Unknown TOML fields are rejected. Example for the standalone systemd unit below:

```toml
immich_url = "https://immich.example.com"
immich_api_key_file = "/run/credentials/immich-dlna-proxy.service/immich-api-key"
listen_address = "192.168.1.10:8200"
friendly_name = "Immich"
sort_locale = "pl"
server_uuid = "7b37df49-b75d-4bcb-89a6-0c917a934643"
log_level = "info"
```

Generate your own non-nil UUID **once** with `uuidgen` or `cat /proc/sys/kernel/random/uuid`;
retain it across restarts, not the example UUID. `immich_url` accepts an HTTP(S) origin
or reverse-proxy prefix, optionally ending in `/api`; trailing slashes are normalized
and `/api/` appended when absent. Userinfo, queries, and fragments are forbidden.

`listen_address` requires concrete local non-loopback unicast IPv4, port 1024-65535,
on exactly one up, multicast-capable interface. It determines listening, SSDP, and
advertised URLs, never HTTP `Host`; no wildcard, IPv6 discovery, or interface fallback.

Albums default to latest asset date descending, using Immich's album `endDate`
(photos and videos), with missing/invalid dates last. Ties use case-insensitive
album-title order, then UUID. `sort_locale` is required; `pl` selects Polish ICU
collation without installed OS locales. Members remain in capture-instant ascending
order. Explicit `+dc:date` / `-dc:date` sorting uses advertised dates; album dates
remain creation dates, independently of the default album ordering.
Photo dates use Immich's local capture date, without TV/server timezone conversion.
Defaults: `friendly_name = "Immich"` (1-128 XML-safe UTF-8 bytes), `log_level = "info"`
(allowed: `error`, `warn`, `info`, `debug`, `trace`).

Provision the key externally as a protected regular file, **<= 8 KiB**. Only trailing
CR/LF are removed, not spaces; empty keys/remaining controls are rejected. Never put
the key in TOML, command arguments, or the Nix store. For direct execution, use its
absolute runtime path. Systemd provisions credentials through `LoadCredential`.
Catalog revisions are kept in memory; no writable state directory is needed.

## Standalone Systemd

Install at `/usr/local/bin/immich-dlna-proxy` and use the TOML above. Provision
`/run/secrets/immich-dlna-api-key` before startup, readable by systemd, not other users.
Unit name: `immich-dlna-proxy.service`.

```ini
[Unit]
Description=Immich DLNA proxy
Wants=network-online.target
After=network-online.target

[Service]
ExecStart=/usr/local/bin/immich-dlna-proxy --config /etc/immich-dlna-proxy.toml
LoadCredential=immich-api-key:/run/secrets/immich-dlna-api-key
DynamicUser=yes
UMask=0077
Restart=on-failure
RestartSec=3
NoNewPrivileges=yes
CapabilityBoundingSet=
AmbientCapabilities=
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
PrivateDevices=yes
RestrictSUIDSGID=yes
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX AF_NETLINK

[Install]
WantedBy=multi-user.target
```

Keep host networking; do not enable `PrivateNetwork`. Allow the TCP service
port and SSDP UDP 1900 only on the trusted interface. Clients need the advertised address
and multicast `239.255.255.250:1900`; binding does not replace a firewall. No multicast routing.
The TOML credential path is fixed for this unit name, not shell-variable expansion.
IPv6 sockets are allowed for upstream HTTP/DNS resolution, not DLNA discovery.

## NixOS

Add the flake input and import its module in your existing NixOS configuration:

```nix
{
  inputs.immich-dlna-proxy.url = "github:ku1ik/immich-dlna-proxy";
  inputs.immich-dlna-proxy.inputs.nixpkgs.follows = "nixpkgs";

  outputs = { nixpkgs, immich-dlna-proxy, ... }: {
    nixosConfigurations.media = nixpkgs.lib.nixosSystem {
      modules = [
        ./configuration.nix
        immich-dlna-proxy.nixosModules.default
        {
          services.immich-dlna-proxy = {
            enable = true;
            immichUrl = "https://immich.example.com";
            immichApiKeyFile = "/run/secrets/immich-dlna-api-key";
            listenAddress = "192.168.1.10:8200";
            sortLocale = "pl";
            serverUuid = "7b37df49-b75d-4bcb-89a6-0c917a934643";
            openFirewall = true;
          };
        }
      ];
    };
  };
}
```

Replace the UUID/address. Keep `immichApiKeyFile` a **quoted runtime path string**,
never a Nix path literal or `builtins.readFile` of the key: secrets must not enter the store.
Provision/rotate externally, then restart. The module generates non-secret TOML and uses
`LoadCredential` with the hardening above.
Optional `package`, `friendlyName` (default `Immich`), and `logLevel` (default `info`)
are overridable. `openFirewall` defaults to **false**; enabling it opens the TCP service
port and UDP 1900 on all interfaces. Use it only when every interface is trusted; otherwise,
leave it disabled and configure interface-scoped firewall rules separately.

## Media Preparation

Keep **default JPEG previews and JPEG full-size output**. Artwork uses previews, not WebP
thumbnails. Incompatible derivatives may need regeneration; non-JPEG display/preview responses
fail. The proxy never changes settings, starts jobs, rewrites, converts, or transcodes.

Full-size generation is an **optional quality recommendation**, not a startup requirement:
it improves HEIC/other eligible images without increasing ordinary preview resolution.
Backfill with **Generate Thumbnails -> Missing**; use **All** only for intentional
replacement, then wait for jobs. With full-size disabled, Immich can fall back to
previews; edited images normally have a separate full-size generation path.

Unedited JPEG/PNG/GIF advertise original first, then JPEG preview (a still for GIF).
Edited/other images advertise edited-aware full-size JPEG then JPEG preview, not the
unedited original. Async generation means `edited=true` can return older/unedited
derivatives while pending, not a guarantee of the latest edit or client fallback.

Videos advertise **original first**, plus existing encoded MP4 playback when observed
in the album-scoped search. Retain default **H.264/AAC targets** and **required** policy
as compatibility guidance, not forced reencoding. Accepted streams may be copied;
old outputs can differ and MP4 does not prove H.264/AAC. Optional **Video Conversion
-> All** reevaluates under the selected policy; it is **not** transcode policy **all**.

Video DIDL resources advertise `DLNA.ORG_OP=01` for HTTP byte seeking only; images
remain generic. No HTTP `contentFeatures.dlna.org` header, time seeking, codec/profile
claims, HLS, or renderer probing. GET supports one closed, open-ended, or suffix
byte range. Unsupported, malformed, or multiple ranges are ignored together with
`If-Range`, giving an ordinary full/conditional request, not multipart delivery.
HEAD ignores Range/If-Range. Upstream media reads have a 60-second no-progress
timeout, not a total duration limit; failures after headers terminate the stream
without retry.

Client resource selection/fallback, legacy containers/codecs, HDR/audio/frame rates,
EXIF/portrait rotation, and large images/panoramas need sample-specific testing.
No universal playback guarantee, UPnP Search/write actions, or media cache.

## Freshness And State

Eligible album members are images/videos with `timeline` or `archive` visibility,
not trashed. Hidden/locked/unknown visibility is excluded; empty albums remain.
Offline or missing-generated-file assets are not silently removed from listings.
**Browse** refreshes needed data after a **60-second TTL** from successful publication.
While clients are active, background passes check the root and resident album contents:
the first pass starts after **two minutes**, and subsequent passes two minutes after
completion. Recently refreshed scopes are skipped. Foreground requests take priority
between scopes; load and traversal time can delay discovery.

Admitted Browse/media requests and successful ContentDirectory subscriptions **and
renewals** count as activity. Polling stops after **ten idle minutes**; an open media
GET keeps it active, with ten minutes of grace after playback ends. Update-ID polling,
discovery, and outgoing notifications do not extend activity. A valid subscription
alone is insufficient if its renewals are more than ten minutes apart.

Root refresh discovers album-list/metadata changes. Background contents discovery is
limited to resident albums; evicted or never-browsed contents need a Browse. Changed
fingerprints advance revisions and trigger notifications. Failures preserve published
state and retry on a later pass; Browse needing stale data returns an error. Background
access does not promote albums in the user-access cache order.

Fixed limits: 4,096 current albums; 20,000 eligible items/16 MiB per album; 50 pages/50,000
examined records per traversal; 32 resident albums/64 MiB projected cache; 16 media operations.
Over-limit refreshes fail, never truncate. Revision records survive snapshot eviction
but are discarded when a successful root refresh removes an album. Reappearing albums
are new observations; revision memory is bounded by the current album limit.

Global and per-album update IDs are wrapping 32-bit counters. One random seed initializes
the global counter at startup. Newly observed albums start at the global revision of
the publication introducing them. Full catalog fingerprints detect changes;
snapshots, counters, and pending notifications are published together in memory. Each
refresh has one 25-second budget; a timeout fails the refresh without terminating the service.
One catalog task serves local reads while refreshing one scope at a time. Up to eight
Browse operations share active work or wait within their original 25-second response
budget. A caller disconnecting or timing out does not cancel an active refresh.
Immich outages do not block startup: version checking occurs on first catalog access.

Restarting forgets counters and fingerprints but retains the configured device UUID.
**Restart cache invalidation is probabilistic**: a new revision can match one cached by a
client. First observations may report changes even when Immich is unchanged. Client restart
behavior with memory-only revisions remains to be checked on the LG C4 and desktop viewers.

### Upgrading from Persistent Revisions

Stop the old service, remove `state_directory` from standalone TOML and `StateDirectory` /
`StateDirectoryMode` from standalone units, then upgrade and restart with the same UUID.
The NixOS module generates the updated configuration. Unknown TOML fields are rejected.

Old revision files are unused and never automatically deleted. They may be archived or
removed explicitly; with DynamicUser, account for the symlink into `/var/lib/private`.
Restoring an archived ledger when rolling back does not preserve continuity with revisions
issued by the memory-only version.

## Diagnostics

- Discovery: check `systemctl status immich-dlna-proxy` and read `http://192.168.1.10:8200/device.xml` with `curl --fail`. This checks local HTTP, not Immich readiness. Confirm the configured address/interface, trusted firewall rules, multicast, and Wi-Fi client isolation.
- Desktop discovery: if direct media URLs work but discovery fails, check the client firewall too. SSDP replies come from the server's UDP 1900 to the client's search socket, which can use an ephemeral port. Opening only client UDP 1900 may be insufficient. Capture searches/replies on both hosts and scope any exception to the trusted interface/server; do not leave the firewall disabled.
- Missing albums/items: check Immich version and `album.read`/`asset.read`, visibility, and refresh/traversal limits. Browse again after TTL expiry; counter polling alone cannot refresh. Failed refreshes leave published counters unchanged; reseeding and first observations after restart are not proof of media changes.
- Missing key/media: inspect credential provisioning and restart after rotation. Media permission failures return 502; check `asset.view` and `asset.download`. A readable catalog does not prove byte access. Missing files normally return 404; inspect Immich generation jobs and failures.
- MIME/generation: display/preview must actually return JPEG and playback MP4. Stale encoded records or incompatible old derivatives can fail; check settings/jobs and intentionally regenerate as needed, not by granting admin scopes to the proxy.
- Mislabeled originals: Immich derives catalog MIME from the original filename and download MIME from the storage-path extension. A file named `.png` can contain JPEG data, leaving both labels wrong while its JPEG preview works. Inspect the actual format before blaming PNG support; metadata or thumbnail regeneration does not repair original extensions. The proxy does not probe or rewrite originals to correct this upstream inconsistency.
- Failed seek/playback: set TOML `log_level = "debug"`, restart, then use `journalctl -u immich-dlna-proxy`. Compare selected representation, incoming/forwarded range diagnostics, upstream status and 206 evidence using the same clip; test alternatives separately. 504 indicates an upstream deadline; 503 can indicate admission overload.

Logging uses TOML, **not `RUST_LOG`**. Diagnostics include IDs/status/resource kind, not keys,
upstream bodies, EXIF/GPS, storage paths, callback query secrets, or media. Keep reports sanitized.
For stale listings, debug logs show incoming Browse scope/pagination, response counts
and revisions, update-ID reads, subscription leases/outcomes and event delivery.
Refresh logs distinguish foreground and background work. On the tested LG C4, reopening
Media Player could reuse its cached listing while continuing subscription renewals.

## Verification

```sh
nix develop --command just check
nix eval --json .#checks.x86_64-linux.module-eval.tests
nix eval --json .#checks.aarch64-linux.module-eval.tests
nix build --no-link
```

`just check` runs format validation, tests, Clippy across all targets/features with warnings
denied, then a release build. Run Rust commands inside `nix develop`. Module evaluation
does not build the package; `nix build` verifies packaging separately. Automated tests
use local fixtures, not production keys or live changes.
The serial catalog loop and activity-gated polling have automated coverage, including
notification-driven revision discovery over HTTP. C4/desktop background-refresh and
memory-only restart behavior still need live-client verification.

The [fixture guide](tests/fixtures/README.md) has synthetic media, a test-only server,
and HTTP/TV/desktop checklists. Tested: **LG OLED77C45LA.DEUQLJP, webOS 25, platform 10.3.1,
firmware 33.31.68**. Fixture checks cover discovery/navigation, original/generated JPEG
display, and both original and independently selected playback H.264/AAC fixtures
with audio, forward/backward seeking and pause/resume.
DIDL OP=01 enabled real Range/206 seeks; HTTP-only signaling did not. Integrated checks
also cover a 1,297-item album, sampled JPEG/PNG originals, an Immich-rotated image,
and sampled original videos with seeking, pause/resume, orientation and both audio outputs.

**VLC 3.0.23 on NixOS 26.05** passed album discovery/browsing, the edited JPEG, and direct
original/playback-alternative video tests. Discovery required disabling a blocking client
firewall for diagnosis; a scoped exception with the firewall enabled remains unverified.
**VLC 3.0.23 on macOS 15.7.7** passed discovery/browsing and sampled photo/video display;
both desktop clients passed audio, forward/backward seeking and pause/resume on the
sampled original and playback-alternative URLs. Actual module-managed NixOS runtime
verification is deferred to deployment; module evaluation alone does not verify it.
Polish album ordering was observed on the TV; source-date projection and all three
date orderings matched Immich across 1,693 items, including local/UTC day differences.
These sample results do not guarantee automatic resource fallback or universal codec
support, and do not substitute for deployment-time runtime verification.
