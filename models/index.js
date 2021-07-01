const { getConnection } = require('../config/dbconnection');

module.exports = {
    fetch: async (db_table, fields = '*', criteria, params = [], options) => {
        const db = await getConnection();
        const colums = Array.isArray(fields) ? fields.join(', ') : fields;
        const query = `SELECT ${colums} FROM ${db_table}`;
        if (criteria) query += ` WHERE ${criteria},`
        const result = await db.execute(query, params, options);
        return result.rows;
    }
}