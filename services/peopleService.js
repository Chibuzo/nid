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
    const db = await getConnection();
    const result = await db.execute("SELECT COUNT(*) num FROM HR.PER_ALL_PEOPLE_F");

    let fetchedData = [];
    for (let i = 0; i < result.rows[0].NUM + 100; i += 100) {
        const records = await fetchPeoplesRecord(db, i);

        fetchedData = await Promise.all(records.map(record => fetchPersonData(record.NID)));
        console.log({ fetchedData })
        //saveFetchedData(fetchedData);
    }

    // const person = await findPerson(personId);
    // if (person) {
    //     return fetchUpdatedRecord(personId);
    // }
    // const personData = await fetchPersonData(personId);
    // const result = await savePersonData(personData);
    // if (!result.lastRowid) {
    //     throw new ErrorHandler(400, 'Couldn\'t save retrieved data');
    // }
    // return updatePersonRecord(personData);
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

const savePersonData = async (db, { IDNumber, IdCollected, Status, Surname, FirstName, MiddleName, SexCode, BirthDate, DeathDate, NationalityCode, Nationality, Sex }) => {
    const sql = `INSERT INTO HR.NID_TEMP
                VALUES(:idnumber, :idcollection, :status, :surname, :firstname, :middlename, :sexcode, TO_DATE(:birthdate, 'DD/MM/YYYY'), TO_DATE(:deathdate, 'DD/MM/YYYY'), :nationalitycode, :nationality, :sex)`;

    const params = [IDNumber, IdCollected, Status, Surname, FirstName, MiddleName, SexCode, BirthDate, DeathDate, NationalityCode, Nationality, Sex];
    return db.execute(sql, params, { autoCommit: true });
}

const saveFetchedData = async (db, data) => {
    return Promise.all(data.map(d => {
        updatePersonRecord(db, d.IDNumber, d.DeathDate);
        savePersonData(db, d);
    }));
}

const findPerson = async personID => {
    const db = await getConnection();
    const result = await db.execute('SELECT IDNUMBER FROM HR.NID_TEMP WHERE IDNUMBER = :idnumber', [personID]);
    return result.rows;
}

const fetchPeoplesRecord = async (db, offset) => {
    const result = await db.execute("SELECT NATIONAL_IDENTIFIER nid FROM HR.PER_ALL_PEOPLE_F OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY", [offset, 200]);
    return result.rows;
}

const fetchUpdatedRecord = async personId => {
    const db = await getConnection();
    const result = await db.execute('SELECT * FROM HR.PER_ALL_PEOPLE_F WHERE NATIONAL_IDENTIFIER = :idnumber', [personID]);
    return result.rows[0];
}

const updatePersonRecord = async (db, nid, death_date = null) => {
    const sql = `UPDATE HR.PER_ALL_PEOPLE_F SET
                    ATTRIBUTE10 = 'verified',
                    DATE_OF_DEATH = :date_of_death
                WHERE NATIONAL_IDENTIFIER = :nid`;

    const result = await db.execute(sql, [death_date, nid], { autoCommit: true });
}


module.exports = {
    fetchAndUpdatePersonData,
}
