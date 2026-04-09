/**
 * Call History Module
 *
 * Provides functions to insert, update, and query call history records
 * stored in the MySQL `call_history` table.
 */

const { getPool } = require('./db');

/**
 * Look up a user's DB id by their username.
 * Returns null when the user is not found.
 */
async function getUserIdByUsername(username) {
    if (!username) return null;
    const pool = getPool();
    const [rows] = await pool.execute(
        'SELECT id FROM users WHERE username = ? LIMIT 1',
        [username]
    );
    return rows.length > 0 ? rows[0].id : null;
}

/**
 * Insert a new call record when a call is initiated (dialOut / callIn).
 *
 * @param {object} data
 * @param {string} data.callId        – e.g. "call_1712696400000"
 * @param {string} data.callerName    – username of the person who initiated
 * @param {string} data.callerRole    – 'agent' | 'customer'
 * @param {string} [data.calleeName]  – username of the target (may be null for callIn)
 * @param {string} [data.calleeRole]  – 'agent' | 'customer'
 */
async function insertCallRecord(data) {
    const pool = getPool();
    const callerId = await getUserIdByUsername(data.callerName);
    const calleeId = await getUserIdByUsername(data.calleeName);

    await pool.execute(
        `INSERT INTO call_history
       (call_id, caller_id, caller_name, caller_role, callee_id, callee_name, callee_role, status, started_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, 'missed', NOW())`,
        [
            data.callId,
            callerId,
            data.callerName || null,
            data.callerRole || null,
            calleeId,
            data.calleeName || null,
            data.calleeRole || null,
        ]
    );

    console.log(`[callHistory] 📝 Inserted record for callId=${data.callId}`);
}

/**
 * Update an existing call record.
 *
 * @param {string} callId
 * @param {object} fields – any subset of { status, answered_at, ended_at, duration_sec, callee_name, callee_role, callee_id }
 */
async function updateCallRecord(callId, fields) {
    const pool = getPool();

    const sets = [];
    const values = [];

    if (fields.status) {
        sets.push('status = ?');
        values.push(fields.status);
    }
    if (fields.answered_at) {
        sets.push('answered_at = NOW()');
    }
    if (fields.ended_at) {
        sets.push('ended_at = NOW()');
    }
    if (typeof fields.duration_sec === 'number') {
        sets.push('duration_sec = ?');
        values.push(Math.round(fields.duration_sec));
    }
    if (fields.callee_name) {
        sets.push('callee_name = ?');
        values.push(fields.callee_name);
    }
    if (fields.callee_role) {
        sets.push('callee_role = ?');
        values.push(fields.callee_role);
    }
    if (fields.callee_id !== undefined) {
        sets.push('callee_id = ?');
        values.push(fields.callee_id);
    }

    if (sets.length === 0) return;

    values.push(callId);

    await pool.execute(
        `UPDATE call_history SET ${sets.join(', ')} WHERE call_id = ?`,
        values
    );

    console.log(`[callHistory] 📝 Updated record for callId=${callId}: ${sets.join(', ')}`);
}

/**
 * Query call history with optional filters and pagination.
 *
 * @param {object} [filters]
 * @param {number} [filters.userId]    – filter by caller_id OR callee_id
 * @param {string} [filters.status]    – 'completed' | 'missed' | 'rejected'
 * @param {string} [filters.from]      – start date YYYY-MM-DD
 * @param {string} [filters.to]        – end date YYYY-MM-DD
 * @param {string} [filters.search]    – search by caller_name or callee_name
 * @param {number} [filters.page=1]
 * @param {number} [filters.limit=20]
 * @returns {{ rows: Array, total: number, page: number, totalPages: number }}
 */
async function getCallHistory(filters = {}) {
    const pool = getPool();
    const page = Math.max(1, parseInt(filters.page, 10) || 1);
    const limit = Math.min(100, Math.max(1, parseInt(filters.limit, 10) || 20));
    const offset = (page - 1) * limit;

    const wheres = [];
    const values = [];

    if (filters.userId) {
        wheres.push('(caller_id = ? OR callee_id = ?)');
        values.push(filters.userId, filters.userId);
    }
    if (filters.status) {
        wheres.push('status = ?');
        values.push(filters.status);
    }
    if (filters.from) {
        wheres.push('call_date >= ?');
        values.push(filters.from);
    }
    if (filters.to) {
        wheres.push('call_date <= ?');
        values.push(filters.to);
    }
    if (filters.search) {
        wheres.push('(caller_name LIKE ? OR callee_name LIKE ?)');
        const like = `%${filters.search}%`;
        values.push(like, like);
    }

    const whereClause = wheres.length > 0 ? `WHERE ${wheres.join(' AND ')}` : '';

    // Count total
    const [countRows] = await pool.execute(
        `SELECT COUNT(*) AS total FROM call_history ${whereClause}`,
        values
    );
    const total = countRows[0].total;

    // Fetch page
    const [rows] = await pool.execute(
        `SELECT id, call_id, caller_name, caller_role, callee_name, callee_role,
            status, started_at, answered_at, ended_at, duration_sec, call_date
     FROM call_history ${whereClause}
     ORDER BY started_at DESC
     LIMIT ? OFFSET ?`,
        [...values, String(limit), String(offset)]
    );

    return {
        rows,
        total,
        page,
        totalPages: Math.ceil(total / limit),
    };
}

module.exports = { insertCallRecord, updateCallRecord, getCallHistory, getUserIdByUsername };
