const { getConnection } = require('../config/dbconnection');
const { ErrorHandler } = require('../helpers/errorHandler');
const axios = require('axios');


const queryTest = async () => {
    const sql = `select person_id, full_name, payroll_name 
        from per_all_people_f p
        join per_all_assignments_f a using(person_id)
        join pay_all_payrolls_f p ON p.payroll_id = a.payroll_id
        where payroll_name = 'Civil Pensions Payroll'
    `;
    const db = await getConnection();
    const result = await db.execute(sql);
    console.log(result.rows)
};

const fetchPersonData = async (personId) => {
    try {
        const res = await axios({
            method: 'get',
            url: '/service',
            baseURL: 'http://10.80.0.10:70/api/',
            params: {
                id: personId,
                key: 'AAD075138F'
            }
        });

        console.log(res)
        return res.resposne.data;
    } catch (err) {
        console.log(err);
        const code = err.response.status || 400;
        const message = err.response.statusText || 'Unable to fetch data';
        throw new ErrorHandler(code, message)
    }
}


module.exports = {
    fetchPersonData,
}
