const mediasoup = require('mediasoup');

(async () => {
    const worker = await mediasoup.createWorker();
    const router = await worker.createRouter({
        mediaCodecs: [
            {
                kind: 'audio',
                mimeType: 'audio/opus',
                clockRate: 48000,
                channels: 2,
            }
        ]
    });

    try {
        const canConsume = router.canConsume({
            producerId: 'invalid-id-does-not-exist',
            rtpCapabilities: {
                codecs: [
                    {
                        kind: 'audio',
                        mimeType: 'audio/opus',
                        preferredPayloadType: 100,
                        clockRate: 48000,
                        channels: 2,
                        rtcpFeedback: [],
                    }
                ],
                headerExtensions: [],
            }
        });
        console.log('Returned:', canConsume);
    } catch (err) {
        console.error('Thrown error:', err.message);
    }
    worker.close();
})();
