class RingtoneManager {
    constructor() {
        this.audioCtx = null;
        this.interval = null;
        this.isPlaying = false;
    }

    init() {
        if (!this.audioCtx) {
            const AudioContext = window.AudioContext || window.webkitAudioContext;
            if (AudioContext) {
                this.audioCtx = new AudioContext();
            }
        }
    }

    playRing() {
        if (!this.audioCtx) return;

        const osc1 = this.audioCtx.createOscillator();
        const osc2 = this.audioCtx.createOscillator();
        const gainNode = this.audioCtx.createGain();

        // US Style phone ring frequencies: 440 Hz and 480 Hz
        osc1.type = 'sine';
        osc2.type = 'sine';
        osc1.frequency.setValueAtTime(440, this.audioCtx.currentTime);
        osc2.frequency.setValueAtTime(480, this.audioCtx.currentTime);

        gainNode.gain.setValueAtTime(0, this.audioCtx.currentTime);
        gainNode.gain.linearRampToValueAtTime(0.3, this.audioCtx.currentTime + 0.1);
        gainNode.gain.setValueAtTime(0.3, this.audioCtx.currentTime + 1.8);
        gainNode.gain.linearRampToValueAtTime(0, this.audioCtx.currentTime + 2.0);

        osc1.connect(gainNode);
        osc2.connect(gainNode);
        gainNode.connect(this.audioCtx.destination);

        osc1.start();
        osc2.start();
        osc1.stop(this.audioCtx.currentTime + 2.0);
        osc2.stop(this.audioCtx.currentTime + 2.0);
    }

    start() {
        if (this.isPlaying) return;
        this.init();
        if (!this.audioCtx) return;

        if (this.audioCtx.state === 'suspended') {
            this.audioCtx.resume();
        }

        this.isPlaying = true;
        this.playRing(); // Initial ring
        this.interval = setInterval(() => {
            if (this.isPlaying) this.playRing();
        }, 1000); // Re-ring every 1 second
    }

    stop() {
        this.isPlaying = false;
        if (this.interval) {
            clearInterval(this.interval);
            this.interval = null;
        }
    }
}

export const ringtone = new RingtoneManager();
