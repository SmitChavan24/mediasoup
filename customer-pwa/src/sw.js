import { precacheAndRoute } from 'workbox-precaching';

// Precaches all Vite-generated assets
precacheAndRoute(self.__WB_MANIFEST || []);

self.addEventListener('push', function (event) {
    console.log('[SW] Push Received.', event.data ? event.data.text() : 'No data');
    if (event.data) {
        const data = event.data.json();

        if (data.type === 'cancelCall') {
            event.waitUntil(
                self.registration.getNotifications({ tag: 'incoming-call' }).then(function (notifications) {
                    notifications.forEach(n => n.close());
                })
            );
            return;
        }

        const options = {
            body: data.message || 'Connecting to customer support',
            icon: '/pwa-192x192.svg',
            badge: '/pwa-192x192.svg',
            vibrate: [200, 100, 200, 100, 200, 100, 200],
            tag: 'incoming-call',
            renotify: true,
            requireInteraction: true,
            data: { callId: data.callId }
        };
        event.waitUntil(
            self.registration.showNotification(data.title || 'Support Incoming Call', options)
        );
    }
});

self.addEventListener('notificationclick', function (event) {
    console.log('[SW] Notification click Received.');
    event.notification.close();

    event.waitUntil(
        clients.matchAll({ type: 'window', includeUncontrolled: true }).then(function (clientList) {
            if (clientList.length > 0) {
                let client = clientList[0];
                for (let i = 0; i < clientList.length; i++) {
                    if (clientList[i].focused) { client = clientList[i]; }
                }
                return client.focus();
            }
            return clients.openWindow('/');
        })
    );
});
