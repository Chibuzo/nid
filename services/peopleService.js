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

const fetchAndUpdatePersonData = async personId => {
    const personData = await fetchPersonData(personId);
    const result = await savePersonData(personData);
    if (!result.rows) {
        throw new ErrorHandler(400, 'Couldn\'t save retrieved data');
    }
    //return updatePersonRecord(personData);
}

const fetchPersonData = async (personId) => {
    try {
        const res = await axios({
            method: 'get',
            url: '/apiService.svc/GetPersonByID',
            baseURL: 'http://10.80.0.10:8088/',
            params: {
                idNumber: personId,
                key: 'AAD075138F'
            }
        });

        return JSON.parse(res.data);
    } catch (err) {
        console.log(err);
        const code = err.response && err.response.status || 400;
        const message = err.response && err.response.statusText || 'Unable to fetch data';
        throw new ErrorHandler(code, message)
    }
}

const savePersonData = async ({ IDNumber, IdCollected, Status, Surname, FirstName, MiddleName, SexCode, BirthDate, DeathDate, NationalityCode, Nationality, Sex }) => {
    const sql = `INSERT INTO NID_TEMP 
                    ('IDNUMBER', 'IDCOLLECTED', 'STATUS', 'SURNAME', 'FIRSTNAME', 'MIDDLENAME', 'SEXCODE', 'BIRTHDATE', 'DEATHDATE', 'NATIONALITYCODE', 'NATIONALITY', 'SEX'
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`;

    const params = [IDNumber, IdCollected, Status, Surname, FirstName, MiddleName, SexCode, BirthDate, DeathDate, NationalityCode, Nationality, Sex];
    const db = await getConnection();
    const result = await db.execute(sql, params);
    console.log({ result })
    return result;
}

const updatePersonRecord = async data => {
    const sql = `UPDATE PERSONS SET
    
                WHERE national_identity = :nid`;

    const db = await getConnection();
    const result = await db.execute(sql, [data.IDNumber]);
}


module.exports = {
    fetchAndUpdatePersonData,
}
