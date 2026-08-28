# scicom-ooh

Out-of-home display system: a Raspberry Pi drives one or more screens through a
rotating playlist of live dashboards, photos and video, controlled entirely over
MQTT from a static web page.

## Architecture

```
   browser (static page, any host)
            │  MQTT over WebSocket
            ▼
        mosquitto  ──────────────┐   (broker; LAN + tailnet)
            ▲                    │
            │ outbound only      │
   ┌────────┴─────────┐          │
   │  wallboard       │          │
   │  daemon (Pi)     │          │
   │   ├── CDP ───────┼──► Chromium --kiosk   (one per display)
   │   ├── layer-shell┼──► boot shield + ticker bar
   │   └── cec-ctl ───┼──► TV power
   └──────────────────┘
```

The device never listens for control traffic. It connects *out* to the broker
and publishes retained state; the page is a thin client with no server of its own.

**DEVICE → DISPLAYS → PLAYLIST.** A device owns one display per HDMI output.
Playlists are a device-wide library; each display picks one, independently, or two
displays share a `mirror_group` and switch in step.

## Layout

| Path | What it is |
|---|---|
| `control-page/` | The whole UI. Static files — open locally or host anywhere (deployed to Cloudflare Pages). |
| `device/server.py` | The daemon: MQTT, playlist rotation, media, CEC, ticker, settings. |
| `device/templates/` | Pages Chromium loads locally: the idle screen and the media viewer. |
| `device/bin/wallboard-kiosk.sh` | Chromium supervisor — one instance per connected output. |
| `device/bin/wallboard-overlay.py` | `wlr-layer-shell` overlays: boot shield and news ticker. |
| `device/bin/wallboard-settings-apply` | Root helper for reboot-required settings. |
| `device/systemd/`, `device/mosquitto/`, `device/labwc/` | Unit, broker listeners, session autostart. |

## Notable design decisions

- **Real browser tabs, not iframes.** Grafana and Uptime Kuma refuse to be framed.
  Tabs also stay preloaded, so switching is instant.
- **Keep-live shim.** Grafana pauses its query timers when its tab is hidden and
  refetches on becoming visible, which looks like every page reloading on rotation.
  A small injected shim keeps `visibilityState` reporting `visible`, so hidden tabs
  keep polling and are current when shown.
- **Priming pass.** On start, every tab is visited briefly to trigger its load
  while a full-screen shield covers the screen. Waiting for full readiness took
  70–120s; triggering and letting keep-live finish the job takes ~27s.
- **Scroll/zoom locks.** Dashboards reflow repeatedly while loading, so a scroll
  position must be re-asserted, not set once. The scroll container is found by
  trying candidates and keeping whichever actually moves.
- **Overlays via the compositor.** The shield and ticker are layer-shell surfaces
  on the OVERLAY layer, so they sit above a fullscreen kiosk without any
  cooperation from the page underneath (no CSP problems, nothing to inject).
- **Videos play to completion.** The rotator watches the video element and advances
  on `ended`; photos and dashboards use the playlist dwell.
- **Stable device id.** The MQTT topic key is a persisted id derived from
  `machine-id`, never the hostname — renaming the host cannot orphan retained
  messages and leave a ghost device on the broker.

## Install (device)

```bash
sudo apt install -y chromium python3-flask python3-requests python3-paho-mqtt \
    python3-websocket python3-pil python3-gi python3-gi-cairo \
    gir1.2-gtk-3.0 gir1.2-gtklayershell-0.1 \
    mosquitto mosquitto-clients grim wlr-randr kanshi v4l-utils ffmpeg

sudo install -d -o "$USER" -g "$USER" /opt/wallboard /var/lib/wallboard
sudo install -o "$USER" -g "$USER" -m 0644 device/server.py /opt/wallboard/
sudo install -o "$USER" -g "$USER" -m 0644 device/templates/*.html /opt/wallboard/templates/
sudo install -m 0755 device/bin/* /usr/local/bin/
sudo install -m 0644 device/systemd/wallboard.service /etc/systemd/system/
sudo install -m 0644 device/mosquitto/wallboard.conf /etc/mosquitto/conf.d/
install -m 0644 device/labwc/autostart ~/.config/labwc/autostart

# the daemon calls exactly one privileged helper
echo "$USER ALL=(root) NOPASSWD: /usr/local/bin/wallboard-settings-apply" \
  | sudo tee /etc/sudoers.d/wallboard
sudo systemctl daemon-reload && sudo systemctl enable --now mosquitto wallboard
```

Then log in to the desktop session (the autostart launches Chromium and the
overlays) and point `control-page/index.html` at `ws://<host>:9001/mqtt`.

## Portability

Pure Python, so architecture is irrelevant (arm64 and x86-64 both fine). What it
actually needs:

| Requirement | Why | Portable? |
|---|---|---|
| Linux + Chromium | the display surface, driven over CDP | anywhere |
| **wlroots Wayland compositor** | `wlr-layer-shell` (shield, ticker), `grim` (preview), `wlr-randr` (outputs), `kanshi` (modes) | sway, labwc, river, hyprland, wayfire — **not** X11, GNOME or KDE |
| `/dev/cec0` + `cec-ctl` | TV power | Pi and some SBCs only; feature degrades |
| `raspi-config`, `/boot/firmware/config.txt` | reboot-required settings | Raspberry Pi only |

Everything else — playlists, media, ticker, scheduling, preview, interactive
input — is compositor-generic. On a non-wlroots desktop the overlays and preview
are what break first.
