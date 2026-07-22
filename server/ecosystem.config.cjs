module.exports = {
  apps: [
    {
      name: "voip-server",
      script: "index.js",
      // Adjust to wherever the mediasoup repo lives on the instance:
      cwd: "/home/ubuntu/mediasoup/server",
      env: {
        NODE_ENV: "production",
        // 3005 is taken by tglevel_dashboard on the prod instance; mediasoup gets its own port.
        PORT: 3006,
      },
      instances: 1,           // mediasoup keeps C++ workers in-process; do NOT cluster this
      // `instances` alone makes PM2 pick cluster mode, which contradicts the
      // line above and would fork a second copy the moment anyone raises the
      // count. Stating fork explicitly keeps the intent enforced.
      exec_mode: 'fork',
      autorestart: true,
      watch: false,
      max_memory_restart: "1G",
      log_date_format: "YYYY-MM-DD HH:mm:ss Z",
      error_file: "/home/ubuntu/.pm2/logs/voip-server-error.log",
      out_file: "/home/ubuntu/.pm2/logs/voip-server-out.log",
      merge_logs: true,
    },
  ],
};
