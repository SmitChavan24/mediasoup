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
 * Safe to call on every server start.
 */
async function initDatabase() {
  const conn = await pool.getConnection();
  try {
    await conn.execute(`
      CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(50) UNIQUE NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        password_hash VARCHAR(255) NOT NULL,
        role ENUM('agent', 'customer', 'admin') NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_login TIMESTAMP NULL
      )
    `);
    console.log('[db] ✅ users table ready');
  } finally {
    conn.release();
  }
}

module.exports = { createPool, getPool, initDatabase };
