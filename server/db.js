/**
 * MySQL Database Connection Pool
 *
 * Provides a connection pool for the mediasoup application.
 * Uses mysql2/promise for async/await support.
 */

const mysql = require('mysql2/promise');

let pool;

function createPool() {
  pool = mysql.createPool({
    host: process.env.DB_HOST || 'localhost',
    user: process.env.DB_USER || 'mediasoup_app',
    password: process.env.DB_PASSWORD || '',
    database: process.env.DB_NAME || 'mediasoup',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
  });

  console.log('[db] ✅ MySQL connection pool created');
  return pool;
}

function getPool() {
  return pool;
}

/**
 * Initialize the database — create tables if they don't exist.
 * Also handles migration from email → phone if the old schema exists.
 * Safe to call on every server start.
 */
async function initDatabase() {
  const conn = await pool.getConnection();
  try {
    // Check if users table exists and has the old 'email' column
    const [columns] = await conn.execute(
      `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'users'`,
      [process.env.DB_NAME || 'mediasoup']
    );

    const columnNames = columns.map(c => c.COLUMN_NAME);

    if (columnNames.includes('email') && !columnNames.includes('phone')) {
      // Migrate: rename email → phone
      console.log('[db] 🔄 Migrating users table: email → phone');
      await conn.execute(`ALTER TABLE users DROP INDEX email`);
      await conn.execute(`ALTER TABLE users CHANGE email phone VARCHAR(20) NOT NULL`);
      await conn.execute(`ALTER TABLE users ADD UNIQUE INDEX phone (phone)`);
      console.log('[db] ✅ Migration complete');
    } else if (columnNames.length === 0) {
      // Table doesn't exist — create fresh
      await conn.execute(`
        CREATE TABLE IF NOT EXISTS users (
          id INT AUTO_INCREMENT PRIMARY KEY,
          username VARCHAR(50) UNIQUE NOT NULL,
          phone VARCHAR(20) UNIQUE NOT NULL,
          password_hash VARCHAR(255) NOT NULL,
          role ENUM('agent', 'customer', 'admin') NOT NULL,
          push_subscription JSON NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          last_login TIMESTAMP NULL
        )
      `);
    }

    console.log('[db] ✅ users table ready');

    // ── call_history table ───────────────────────────────────────────────
    await conn.execute(`
      CREATE TABLE IF NOT EXISTS call_history (
        id           INT AUTO_INCREMENT PRIMARY KEY,
        call_id      VARCHAR(64) NOT NULL,
        caller_id    INT NULL,
        caller_name  VARCHAR(50),
        caller_role  ENUM('agent','customer'),
        callee_id    INT NULL,
        callee_name  VARCHAR(50),
        callee_role  ENUM('agent','customer'),
        status       ENUM('completed','missed','rejected') DEFAULT 'missed',
        started_at   DATETIME NOT NULL,
        answered_at  DATETIME NULL,
        ended_at     DATETIME NULL,
        duration_sec INT DEFAULT 0,
        call_date    DATE GENERATED ALWAYS AS (DATE(started_at)) STORED,
        INDEX idx_call_id (call_id),
        INDEX idx_caller (caller_id),
        INDEX idx_callee (callee_id),
        INDEX idx_date (call_date),
        INDEX idx_status (status)
      )
    `);
    console.log('[db] ✅ call_history table ready');
  } finally {
    conn.release();
  }
}

module.exports = { createPool, getPool, initDatabase };
