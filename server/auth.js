/**
 * Authentication Module
 *
 * Handles user registration, login, and JWT token management.
 * Uses phone number as the primary login identifier.
 */

const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { getPool } = require('./db');

const JWT_SECRET = process.env.JWT_SECRET || 'fallback-dev-secret-change-me';
const JWT_EXPIRES_IN = '7d'; // 7 days

/**
 * Validate a phone number — digits only (with optional leading +), minimum 10 digits.
 */
function validatePhone(phone) {
  const cleaned = phone.replace(/[\s\-()]/g, '');
  if (!/^\+?\d{10,15}$/.test(cleaned)) {
    throw new Error('Invalid phone number. Must be 10–15 digits (optional + prefix).');
  }
  return cleaned;
}

/**
 * Register a new user.
 * @returns {{ id, username, phone, role, token }}
 */
async function registerUser(username, phone, password, role) {
  const pool = getPool();

  // Validate role
  const validRoles = ['agent', 'customer', 'admin'];
  if (!validRoles.includes(role)) {
    throw new Error('Invalid role. Must be agent, customer, or admin.');
  }

  // Validate password length
  if (!password || password.length < 6) {
    throw new Error('Password must be at least 6 characters.');
  }

  // Validate & clean phone
  const cleanPhone = validatePhone(phone);

  // Hash password
  const salt = await bcrypt.genSalt(10);
  const passwordHash = await bcrypt.hash(password, salt);

  try {
    const [result] = await pool.execute(
      'INSERT INTO users (username, phone, password_hash, role) VALUES (?, ?, ?, ?)',
      [username.trim(), cleanPhone, passwordHash, role]
    );

    const userId = result.insertId;
    const token = generateToken({ userId, username: username.trim(), role });

    return {
      id: userId,
      username: username.trim(),
      phone: cleanPhone,
      role,
      token,
    };
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') {
      if (err.message.includes('username')) {
        throw new Error('Username already taken.');
      }
      if (err.message.includes('phone')) {
        throw new Error('Phone number already registered.');
      }
      throw new Error('Username or phone number already exists.');
    }
    throw err;
  }
}

/**
 * Login with phone + password.
 * @returns {{ id, username, phone, role, token }}
 */
async function loginUser(phone, password) {
  const pool = getPool();

  const cleanPhone = validatePhone(phone);

  const [rows] = await pool.execute(
    'SELECT id, username, phone, password_hash, role FROM users WHERE phone = ?',
    [cleanPhone]
  );

  if (rows.length === 0) {
    throw new Error('Invalid phone number or password.');
  }

  const user = rows[0];
  const isMatch = await bcrypt.compare(password, user.password_hash);

  if (!isMatch) {
    throw new Error('Invalid phone number or password.');
  }

  // Update last_login
  await pool.execute('UPDATE users SET last_login = NOW() WHERE id = ?', [user.id]);

  const token = generateToken({ userId: user.id, username: user.username, role: user.role });

  return {
    id: user.id,
    username: user.username,
    phone: user.phone,
    role: user.role,
    token,
  };
}

/**
 * Verify a JWT token and return the decoded payload.
 * @returns {{ userId, username, role }}
 */
function verifyToken(token) {
  try {
    return jwt.verify(token, JWT_SECRET);
  } catch (err) {
    return null;
  }
}

/**
 * Generate a JWT token.
 */
function generateToken(payload) {
  return jwt.sign(payload, JWT_SECRET, { expiresIn: JWT_EXPIRES_IN });
}

/**
 * Guest Login for PHP Widget Integration.
 * Always uses a synthetic phone derived from phpUserId as the primary key.
 * This completely isolates widget customer accounts from agent/admin accounts.
 */
async function guestLogin(username, phone, phpUserId) {
  const pool = getPool();

  // Always use phpUserId-derived key when available — this prevents any clash
  // with existing agent/admin accounts that may share the same real phone.
  let lookupPhone;
  if (phpUserId) {
    // Stable, unique identifier: "php" + zero-padded userId, 12 chars total
    lookupPhone = `php${String(phpUserId).replace(/\D/g, '').padStart(9, '0')}`.slice(0, 12);
  } else if (phone && phone.trim().length >= 10) {
    try { lookupPhone = validatePhone(phone); } catch (e) { /* fall through */ }
  }

  if (!lookupPhone) {
    throw new Error('A valid phone number or user ID is required.');
  }

  const [rows] = await pool.execute(
    'SELECT id, username, phone, role FROM users WHERE phone = ?',
    [lookupPhone]
  );

  let user;
  if (rows.length === 0) {
    // Create a new customer account
    const salt = await bcrypt.genSalt(10);
    const randomPassword = require('crypto').randomBytes(16).toString('hex');
    const passwordHash = await bcrypt.hash(randomPassword, salt);

    // Attempt insert; if username is taken, append phpUserId suffix
    let insertName = username.trim() || 'Customer';
    try {
      const [result] = await pool.execute(
        'INSERT INTO users (username, phone, password_hash, role) VALUES (?, ?, ?, ?)',
        [insertName, lookupPhone, passwordHash, 'customer']
      );
      user = { id: result.insertId, username: insertName, phone: lookupPhone, role: 'customer' };
    } catch (err) {
      if (err.code === 'ER_DUP_ENTRY') {
        insertName = `${insertName}_${String(phpUserId || Date.now()).slice(-4)}`;
        const [result] = await pool.execute(
          'INSERT INTO users (username, phone, password_hash, role) VALUES (?, ?, ?, ?)',
          [insertName, lookupPhone, passwordHash, 'customer']
        );
        user = { id: result.insertId, username: insertName, phone: lookupPhone, role: 'customer' };
      } else {
        throw err;
      }
    }
  } else {
    user = rows[0];
    // Update username if it has changed in the PHP app
    if (username.trim() && user.username !== username.trim()) {
      await pool.execute('UPDATE users SET username = ?, last_login = NOW() WHERE id = ?', [username.trim(), user.id]);
      user.username = username.trim();
    } else {
      await pool.execute('UPDATE users SET last_login = NOW() WHERE id = ?', [user.id]);
    }
  }

  const token = generateToken({ userId: user.id, username: user.username, role: user.role });

  return {
    id: user.id,
    username: user.username,
    phone: user.phone,
    role: user.role,
    token,
  };
}

module.exports = { registerUser, loginUser, verifyToken, guestLogin };
