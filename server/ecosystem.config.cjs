module.exports = {
  apps: [
    {
      name: "voip-server",
      script: "index.js",
      // Adjust to wherever the mediasoup repo lives on the instance:
      cwd: "/home/ubuntu/mediasoup/server",
      env: {
        NODE_ENV: "production",
        // index.js currently calls server.listen(3005)
      },
      instances: 1,           // mediasoup keeps C++ workers in-process; do NOT cluster this
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
