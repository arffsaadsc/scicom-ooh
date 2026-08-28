#!/bin/bash
# Chromium kiosk supervisor.
#
# One Chromium instance per connected output, each with its own profile and CDP
# port (HDMI-A-1 -> 9222, HDMI-A-2 -> 9223), so displays rotate independently.
# Runs inside the labwc session, because only here is the Wayland env correct.
set -u

PORT="${WALLBOARD_PORT:-8080}"
BASE="${WALLBOARD_PROFILE_BASE:-$HOME/.local/share/wallboard}"

: "${XDG_RUNTIME_DIR:=/run/user/$(id -u)}"
export XDG_RUNTIME_DIR
if [ -z "${WAYLAND_DISPLAY:-}" ]; then
  for _s in "$XDG_RUNTIME_DIR"/wayland-*; do
    case "$_s" in *.lock) continue ;; esac
    [ -S "$_s" ] && { WAYLAND_DISPLAY="$(basename "$_s")"; break; }
  done
fi
export WAYLAND_DISPLAY="${WAYLAND_DISPLAY:-wayland-0}"
export XDG_SESSION_TYPE=wayland XDG_CURRENT_DESKTOP=labwc

cdp_port() {                     # HDMI-A-1 -> 9222
  local n="${1##*-}"
  [[ "$n" =~ ^[0-9]+$ ]] || n=1
  echo $((9222 + n - 1))
}

outputs() {                      # connected, non-headless outputs
  wlr-randr 2>/dev/null | awk '/^[^ ]/{name=$1} /Enabled: yes/{print name}' \
    | grep -v '^NOOP' | sort -u
}

launch() {
  local out="$1" prof="$BASE/profile-$1" port
  port=$(cdp_port "$out")
  mkdir -p "$prof"

  # Chromium stores the hostname in its singleton lock; after a rename it treats
  # the profile as owned by "another computer" and refuses to start.
  local lock="$prof/SingletonLock"
  if [ -L "$lock" ] && ! readlink "$lock" | grep -q "^$(hostname)-"; then
    echo "$(date +%T) [$out] clearing stale singleton lock ($(readlink "$lock"))"
    rm -f "$lock" "$prof/SingletonCookie" "$prof/SingletonSocket"
  fi
  local prefs="$prof/Default/Preferences"
  [ -f "$prefs" ] && sed -i 's/"exit_type":"[^"]*"/"exit_type":"Normal"/g;s/"exited_cleanly":false/"exited_cleanly":true/g' "$prefs" 2>/dev/null

  echo "$(date +%T) [$out] starting chromium on cdp $port"
  chromium \
    --ozone-platform=wayland \
    --kiosk \
    --user-data-dir="$prof" \
    --remote-debugging-port="$port" \
    --remote-allow-origins="http://127.0.0.1:$port" \
    --noerrdialogs --disable-infobars --no-first-run --no-default-browser-check \
    --disable-session-crashed-bubble --hide-crash-restore-bubble \
    --disable-notifications --password-store=basic \
    --disable-features=Translate,TranslateUI,MediaRouter,GlobalMediaControls \
    --autoplay-policy=no-user-gesture-required \
    --check-for-update-interval=31536000 --start-fullscreen \
    --disable-background-timer-throttling \
    --disable-backgrounding-occluded-windows \
    --disable-renderer-backgrounding \
    "http://127.0.0.1:${PORT}/idle?display=$out" >>"/tmp/wallboard-kiosk-$out.log" 2>&1 &
  echo $! > "/tmp/wallboard-kiosk-$out.pid"
}

# wait for the local page server so the first frame is the idle screen
for _ in $(seq 1 60); do
  curl -sf -o /dev/null "http://127.0.0.1:${PORT}/idle" && break
  sleep 1
done

declare -A RUNNING=()
while true; do
  mapfile -t outs < <(outputs)
  for out in "${outs[@]:-}"; do
    [ -z "$out" ] && continue
    pid="${RUNNING[$out]:-}"
    if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
      launch "$out"
      RUNNING[$out]=$(cat "/tmp/wallboard-kiosk-$out.pid")
    fi
  done
  # stop instances whose output went away
  for out in "${!RUNNING[@]}"; do
    if ! printf '%s\n' "${outs[@]:-}" | grep -qx "$out"; then
      echo "$(date +%T) [$out] output gone; stopping chromium ${RUNNING[$out]}"
      kill "${RUNNING[$out]}" 2>/dev/null
      unset "RUNNING[$out]"
    fi
  done
  sleep 5
done
