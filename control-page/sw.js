// Wallboard control - offline shell only.
// There is nothing to cache from the device: every byte of live state arrives
// over MQTT at runtime. This exists so the app opens instantly and still opens
// with no network, showing the connection screen.
const CACHE = 'wallboard-shell-v1';
const SHELL = ['./', './index.html', './manifest.webmanifest',
               './icon-192.png', './icon-512.png'];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(SHELL)).then(() => self.skipWaiting()));
});
self.addEventListener('activate', e => {
  e.waitUntil(caches.keys()
    .then(ks => Promise.all(ks.filter(k => k !== CACHE).map(k => caches.delete(k))))
    .then(() => self.clients.claim()));
});
self.addEventListener('fetch', e => {
  const r = e.request;
  if (r.method !== 'GET') return;
  const u = new URL(r.url);
  if (u.origin !== location.origin) return;      // never touch broker traffic
  // Network first so an updated page always wins, cache only as a fallback.
  e.respondWith(
    fetch(r).then(resp => {
      const copy = resp.clone();
      caches.open(CACHE).then(c => c.put(r, copy));
      return resp;
    }).catch(() => caches.match(r).then(m => m || caches.match('./index.html')))
  );
});
