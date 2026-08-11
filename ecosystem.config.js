/**
 * PM2 Ecosystem Config — WebSocket Gateway.
 *
 * Single-instance (fork mode) on purpose: Socket.io holds long-lived stateful
 * connections. Running it in cluster mode would require the @socket.io/redis-adapter
 * plus sticky sessions at the load balancer, otherwise clients bounce between
 * workers and drop. All other config (REDIS_URL, CHAT_SERVICE_URL, INTERNAL_SECRET,
 * SESSION_SECRET, ALLOWED_ORIGINS) is loaded from .env by server.js via dotenv.
 */
module.exports = {
    apps: [
        {
            name:        'ws-gateway',
            script:      'server.js',
            instances:   1,
            exec_mode:   'fork',
            watch:       false,
            max_memory_restart: '400M',
            error_file:  './logs/err.log',
            out_file:    './logs/out.log',
            log_date_format: 'YYYY-MM-DD HH:mm:ss',
            env: {
                NODE_ENV: 'production',
                PORT: 3020,   // mirrors PORT in .env
            },
        },
    ],
};
