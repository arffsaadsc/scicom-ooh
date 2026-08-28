#!/usr/bin/env python3
"""
wallboard boot overlay.

A fullscreen wlr-layer-shell surface on the OVERLAY layer, which labwc draws
above fullscreen windows - so it hides the kiosk while the rotator cycles
through tabs warming them up. Deliberately a separate process from Chromium:
injecting an overlay into each page means fighting every site's DOM and CSP
(Grafana's style-src blocks injected stylesheets), and it cannot cover the gap
between tab switches.

It polls the control service and shows itself whenever warm-up is in progress.
"""
import gi

gi.require_version("Gtk", "3.0")
gi.require_version("Gdk", "3.0")
gi.require_version("GtkLayerShell", "0.1")
gi.require_version("Pango", "1.0")
gi.require_version("PangoCairo", "1.0")
from gi.repository import (Gdk, GLib, Gtk, GtkLayerShell,  # noqa: E402
                          Pango, PangoCairo)

import cairo  # noqa: E402
import json  # noqa: E402
import math  # noqa: E402
import time  # noqa: E402
import os  # noqa: E402

# Read the daemon's state file rather than an HTTP endpoint: control moved to
# MQTT and the old /api/state route no longer exists.
STATE_FILE = os.environ.get("WALLBOARD_STATE_FILE", "/run/wallboard/state.json")
STALE_AFTER = 12.0
# The overlay is the only thing that knows when a ticker run has actually
# finished scrolling. It drops the run id here so the daemon can clear the run
# immediately instead of waiting out a generous time estimate.
TICKER_DONE_FILE = os.environ.get("WALLBOARD_TICKER_DONE",
                                 "/run/wallboard/ticker-done.json")
POLL_MS = 300

def build_css(scale):
    """GTK3 node names are progressbar > trough > progress - the GTK4-style
    'progress trough' selector silently matches nothing. Sizes scale off the
    panel width so this is legible on a 4K TV as well as a 1080p monitor."""
    px = lambda n: int(round(n * scale))
    return ("""
    window { background: #05070a; }
    #title { color: #e6edf3; font-size: %dpx; font-weight: 600; }
    #sub   { color: #8b949e; font-size: %dpx; font-family: monospace; }
    #hint  { color: #4a525c; font-size: %dpx; font-family: monospace; }
    progressbar trough {
      min-height: %dpx; border-radius: 99px;
      background: rgba(230,237,243,.13); border: none;
    }
    progressbar progress {
      min-height: %dpx; border-radius: 99px;
      background: #f778ba; border: none;
    }
    """ % (px(34), px(17), px(13), px(7), px(7))).encode()


def parse_rgb(v, fallback=(1, 1, 1)):
    try:
        v = str(v).lstrip("#")
        if len(v) == 3:
            v = "".join(c * 2 for c in v)
        return tuple(int(v[i:i + 2], 16) / 255.0 for i in (0, 2, 4))
    except Exception:
        return fallback


class Ticker:
    """A news-ticker bar on the wlr-layer-shell OVERLAY layer.

    Drawn by the compositor above the fullscreen kiosk, so it needs no
    cooperation from the pages underneath - no CSP problems, nothing to inject,
    and it survives tab switches. It owns the animation and counts passes; the
    daemon only says "here is a run: this text, this many passes".
    """

    FPS_MS = 33

    def __init__(self):
        self.win = None
        self.area = None
        self.cfg = None
        self.run_id = None
        self.text = ""
        self.repeats = 2
        self.passes = 0
        self.x = None
        self.text_w = 0
        self.last = None
        self.alpha = 0.0
        self.phase = "idle"      # idle | in | run | out
        GLib.timeout_add(self.FPS_MS, self.tick)

    def fade_seconds(self):
        return max(0.0, float((self.cfg or {}).get("fade", 400)) / 1000.0)

    # ---- window lifecycle -------------------------------------------------
    def build(self, cfg):
        self.destroy()
        w = Gtk.Window()
        # An RGBA visual plus app_paintable is what makes alpha possible on this
        # surface, so the bar can fade over the page instead of snapping in.
        w.set_app_paintable(True)
        vis = w.get_screen().get_rgba_visual()
        if vis:
            w.set_visual(vis)
        GtkLayerShell.init_for_window(w)
        GtkLayerShell.set_layer(w, GtkLayerShell.Layer.OVERLAY)
        edge = (GtkLayerShell.Edge.TOP if cfg.get("position") == "top"
                else GtkLayerShell.Edge.BOTTOM)
        GtkLayerShell.set_anchor(w, GtkLayerShell.Edge.LEFT, True)
        GtkLayerShell.set_anchor(w, GtkLayerShell.Edge.RIGHT, True)
        GtkLayerShell.set_anchor(w, edge, True)
        GtkLayerShell.set_margin(w, edge, int(cfg.get("offset") or 0))
        GtkLayerShell.set_exclusive_zone(w, -1)          # never reserve space
        GtkLayerShell.set_keyboard_mode(w, GtkLayerShell.KeyboardMode.NONE)
        area = Gtk.DrawingArea()
        area.set_size_request(-1, int(cfg.get("height") or 64))
        area.connect("draw", self.draw)
        w.add(area)
        w.connect("realize", self._on_realize)
        self.win, self.area, self.cfg = w, area, dict(cfg)
        w.show_all()

    def _on_realize(self, _w):
        # empty input region: the bar must never swallow a click meant for the
        # page underneath (interactive mode uses those)
        try:
            import cairo as _c
            self.win.get_window().input_shape_combine_region(_c.Region(), 0, 0)
        except Exception:
            pass

    def destroy(self):
        if self.win:
            try:
                self.win.destroy()
            except Exception:
                pass
        self.win = self.area = self.cfg = None

    # ---- drawing ----------------------------------------------------------
    def draw(self, widget, cr):
        cfg = self.cfg or {}
        w = widget.get_allocated_width()
        h = widget.get_allocated_height()
        a = max(0.0, min(1.0, self.alpha))
        # start from fully transparent: without this the surface keeps whatever
        # was last painted and the fade has nothing to reveal underneath
        cr.set_operator(cairo.OPERATOR_SOURCE)
        cr.set_source_rgba(0, 0, 0, 0)
        cr.paint()
        cr.set_operator(cairo.OPERATOR_OVER)
        br, bg, bb = parse_rgb(cfg.get("bar_color"), (0.06, 0.46, 0.43))
        cr.set_source_rgba(br, bg, bb, a)
        cr.rectangle(0, 0, w, h)
        cr.fill()
        if not self.text:
            return False
        layout = PangoCairo.create_layout(cr)
        fd = Pango.FontDescription()
        fd.set_family("DejaVu Sans")
        fd.set_absolute_size(int(cfg.get("font_size") or 30) * Pango.SCALE)
        if int(cfg.get("bold") or 0):
            fd.set_weight(Pango.Weight.BOLD)
        layout.set_font_description(fd)
        layout.set_text(self.text, -1)
        tw, th = layout.get_pixel_size()
        self.text_w = tw
        if self.x is None:
            self.x = float(w)
        tr, tg, tb = parse_rgb(cfg.get("text_color"), (1, 1, 1))
        if a > 0.01:                       # measured above; only skip the paint
            cr.set_source_rgba(tr, tg, tb, a)
            cr.move_to(self.x, (h - th) / 2.0)
            PangoCairo.show_layout(cr, layout)
        return False

    # ---- animation --------------------------------------------------------
    def tick(self):
        # Wrapped: an exception escaping a GLib timeout removes the source, which
        # silently stops the ticker forever. Learned the hard way.
        try:
            self._tick()
        except Exception as e:
            Overlay._log("ticker tick error: %s" % e)
        return True

    def _tick(self):
        now = time.time()
        dt = 0.0 if self.last is None else min(now - self.last, 0.2)
        self.last = now
        if not self.win or not self.text or not self.area:
            return
        if self.x is None:                 # first frame: start off the right edge
            self.x = float(self.area.get_allocated_width() or 1920)
        cfg = self.cfg or {}
        f = self.fade_seconds()

        if self.phase == "in":
            self.alpha = 1.0 if f == 0 else min(1.0, self.alpha + dt / f)
            if self.alpha >= 1.0:
                self.phase = "run"
        elif self.phase == "out":
            self.alpha = 0.0 if f == 0 else max(0.0, self.alpha - dt / f)
            if self.alpha <= 0.0:
                self.finish()
                return

        # keep scrolling through the fades; only stop counting passes once the
        # bar is on its way out
        self.x -= float(cfg.get("speed") or 140) * dt
        if self.text_w and self.x < -self.text_w:
            if self.phase != "out":
                self.passes += 1
                if self.passes >= self.repeats:
                    self.phase = "out"          # fade away instead of vanishing
            self.x = float(self.area.get_allocated_width() or 1920)
        self.area.queue_draw()

    def finish(self):
        done = self.run_id
        self.destroy()
        self.text = ""
        self.x = None
        self.passes = 0
        self.alpha = 0.0
        self.phase = "idle"
        if done:
            try:
                tmp = TICKER_DONE_FILE + ".tmp"
                with open(tmp, "w") as fh:
                    json.dump({"run_id": done, "at": time.time()}, fh)
                os.replace(tmp, TICKER_DONE_FILE)
                Overlay._log("TICKER done run=%s" % done)
            except Exception as e:
                Overlay._log("ticker done-file failed: %s" % e)

    # ---- driven by the state file ----------------------------------------
    def update(self, t):
        if not t:
            # a manual stop should fade out too, not blink off
            if self.win and self.phase not in ("out", "idle"):
                self.phase = "out"
            self.run_id = None
            return
        if t.get("run_id") != self.run_id:
            self.run_id = t["run_id"]
            self.text = t.get("text") or ""
            self.repeats = max(1, int(t.get("repeats") or 2))
            self.passes = 0
            self.x = None
            self.alpha = 0.0
            self.phase = "in"
            self.build(t.get("cfg") or {})
            Overlay._log("TICKER start run=%s passes=%d chars=%d"
                         % (self.run_id, self.repeats, len(self.text)))
        elif self.win and (t.get("cfg") or {}) != self.cfg:
            keep_x = self.x
            self.build(t["cfg"])                # restyle mid-run
            self.x = keep_x


class Overlay:
    def __init__(self):
        self.win = Gtk.Window()
        self.win.set_name("root")
        # NOT app_paintable: that suppresses GTK's own background rendering and
        # leaves the surface transparent, showing the dashboard through it.

        mon = None
        disp = Gdk.Display.get_default()
        if disp:
            mon = disp.get_primary_monitor() or (
                disp.get_monitor(0) if disp.get_n_monitors() else None)
        width = mon.get_geometry().width if mon else 1920
        self.scale = max(1.0, width / 1920.0)

        GtkLayerShell.init_for_window(self.win)
        # OVERLAY sits above fullscreen windows; TOP does not reliably.
        GtkLayerShell.set_layer(self.win, GtkLayerShell.Layer.OVERLAY)
        for edge in (GtkLayerShell.Edge.TOP, GtkLayerShell.Edge.BOTTOM,
                     GtkLayerShell.Edge.LEFT, GtkLayerShell.Edge.RIGHT):
            GtkLayerShell.set_anchor(self.win, edge, True)
        # do not reserve screen space, and never take the keyboard
        GtkLayerShell.set_exclusive_zone(self.win, -1)
        GtkLayerShell.set_keyboard_mode(self.win, GtkLayerShell.KeyboardMode.NONE)

        box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL,
                      spacing=int(18 * self.scale))
        box.set_halign(Gtk.Align.CENTER)
        box.set_valign(Gtk.Align.CENTER)

        # hand-drawn so its size tracks the panel; GtkSpinner would not
        self.spin_angle = 0.0
        sz = int(66 * self.scale)
        self.spinner = Gtk.DrawingArea()
        self.spinner.set_size_request(sz, sz)
        self.spinner.connect("draw", self.draw_spinner)
        box.pack_start(self.spinner, False, False, 0)

        self.title = Gtk.Label(label="Preparing wallboard")
        self.title.set_name("title")
        box.pack_start(self.title, False, False, 0)

        self.sub = Gtk.Label(label="")
        self.sub.set_name("sub")
        self.sub.set_max_width_chars(60)
        self.sub.set_ellipsize(3)  # PANGO_ELLIPSIZE_END
        box.pack_start(self.sub, False, False, 0)

        self.bar = Gtk.ProgressBar()
        self.bar.set_size_request(int(460 * self.scale), -1)
        box.pack_start(self.bar, False, False, int(6 * self.scale))

        self.hint = Gtk.Label(label="")
        self.hint.set_name("hint")
        box.pack_start(self.hint, False, False, 0)

        self.win.add(box)

        prov = Gtk.CssProvider()
        prov.load_from_data(build_css(self.scale))
        Gtk.StyleContext.add_provider_for_screen(
            self.win.get_screen(), prov, Gtk.STYLE_PROVIDER_PRIORITY_APPLICATION)

        self.win.connect("realize", self._on_realize)
        GLib.timeout_add(50, self.spin_tick)
        self.ticker = Ticker()
        self.shown = False
        # visible at startup: the desktop is bare until Chromium paints
        self.show()
        GLib.timeout_add(POLL_MS, self.poll)

    def _on_realize(self, _w):
        # empty input region: clicks and touches pass through to whatever is
        # underneath, so the overlay can never trap a VNC user
        try:
            self.win.get_window().input_shape_combine_region(cairo.Region(), 0, 0)
        except Exception:
            pass

    def draw_spinner(self, widget, cr):
        w = widget.get_allocated_width()
        h = widget.get_allocated_height()
        lw = max(3.0, 5.0 * self.scale)
        r = min(w, h) / 2.0 - lw
        if r <= 0:
            return False
        cr.translate(w / 2.0, h / 2.0)
        cr.set_line_width(lw)
        cr.set_line_cap(cairo.LINE_CAP_ROUND)
        cr.set_source_rgba(0.90, 0.93, 0.95, 0.14)
        cr.arc(0, 0, r, 0, 2 * math.pi)
        cr.stroke()
        cr.set_source_rgb(0.968, 0.470, 0.729)          # #f778ba
        cr.arc(0, 0, r, self.spin_angle, self.spin_angle + math.pi * 0.62)
        cr.stroke()
        return False

    def spin_tick(self):
        if self.shown:                                  # no repaints when hidden
            self.spin_angle = (self.spin_angle + 0.20) % (2 * math.pi)
            self.spinner.queue_draw()
        return True

    @staticmethod
    def _log(msg):
        print("%s %s" % (time.strftime("%H:%M:%S"), msg), flush=True)

    def show(self):
        if not self.shown:
            self.win.show_all()
            self.shown = True
            self._log("SHIELD ON  (covering the screen)")

    def hide(self):
        if self.shown:
            self.win.hide()
            self.shown = False
            self._log("SHIELD OFF (screen handed to the kiosk)")

    def poll(self):
        try:
            age = time.time() - os.path.getmtime(STATE_FILE)
            if age > STALE_AFTER:
                raise OSError("state file is %.0fs stale" % age)
            with open(STATE_FILE) as fh:
                st = json.load(fh)
        except Exception:
            # service not up yet (or restarting) - keep the curtain closed
            self.title.set_text("Starting wallboard")
            self.sub.set_text("waiting for the wallboard service")
            self.hint.set_text("")
            self.bar.set_fraction(0)
            self.show()
            return True

        try:
            self.ticker.update(st.get("ticker"))
        except Exception as e:
            self._log("ticker error: %s" % e)

        if st.get("warming"):
            i = st.get("warm_index") or 0
            n = st.get("warm_total") or 0
            self.title.set_text("Preparing wallboard")
            self.sub.set_text("%d of %d   %s" % (i, n, st.get("warm_label") or ""))
            self.bar.set_fraction((float(i - 1) / n) if n else 0.0)
            self.hint.set_text("loading dashboards and restoring scroll positions")
            self.show()
        elif not st.get("chrome_up"):
            self.title.set_text("Starting wallboard")
            self.sub.set_text("waiting for the browser")
            self.bar.set_fraction(0)
            self.hint.set_text("")
            self.show()
        else:
            self.hide()
        return True


if __name__ == "__main__":
    print("%s overlay starting (scale detection follows)" % time.strftime("%H:%M:%S"), flush=True)
    Overlay()
    Gtk.main()
