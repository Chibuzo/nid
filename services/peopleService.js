const { getConnection } = require('../config/dbconnection');
const { ErrorHandler } = require('../helpers/errorHandler');
const axios = require('axios');

const fields = ['FIRST_NAME', 'LAST_NAME', 'MIDDLE_NAMES', 'NATIONAL_IDENTIFIER NID', 'DATE_OF_BIRTH'];
const MONTH = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];

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

const fetchAndUpdatePeopleData = async () => {
    const db = await getConnection();
    const result = await db.execute("SELECT COUNT(*) NUM FROM HR.PER_ALL_PEOPLE_F");

    let fetchedData = [];
    for (let i = 0; i < result.rows[0].NUM; i += 200) {
        const records = await fetchPeoplesRecord(db, i);

        fetchedData = await Promise.all(records.map(record => fetchPersonData(record.NID)));
        try {
            saveFetchedData(db, fetchedData);
        } catch (err) {
            continue; // add log for this
        }
        console.log('Batch:' + i);
    }
    const sql = `UPDATE HR.PER_ALL_PEOPLE_F SET
                    ATTRIBUTE10 = 'unverified',
                WHERE ATTRIBUTE10 <> 'verified'`;

    await db.execute(sql, { autoCommit: true });
}

const fetchAndUpdatePersonData = async personId => {
    const verifiedPerson = await findVerifiedPerson(personId);
    if (verifiedPerson) {
        return fetchUpdatedRecord(personId);
    }

    // look for person data on NID database
    const personData = await fetchPersonData(personId);
    if (!personData) {
        throw new ErrorHandler(404, 'Employee record not found on NID database');
    }

    const db = await getConnection();
    const result = await savePersonData(db, personData);
    if (!result.lastRowid) {
        throw new ErrorHandler(400, 'Couldn\'t save retrieved data');
    }
    return updatePersonRecord(db, personData);
}

const fetchPersonData = async (personId) => {
    if (personId) {
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

            if (!res.data.Status) return JSON.parse(res.data);
        } catch (err) {
            console.log(err.response || 'error');
            const code = err.response && err.response.status || 400;
            const message = err.response && err.response.statusText || 'Unable to fetch data';
            // throw new ErrorHandler(code, message)
        }
    }
}

const savePersonData = async (db, { IDNumber, IdCollected, Status, Surname, FirstName, MiddleName, SexCode, BirthDate, DeathDate, NationalityCode, Nationality, Sex }) => {
    const sql = `INSERT INTO HR.NID_TEMP
                VALUES(:idnumber, :idcollection, :status, :surname, :firstname, :middlename, :sexcode, TO_DATE(:birthdate, 'DD-MON-YY'), TO_DATE(:deathdate, 'DD-MON-YY'), :nationalitycode, :nationality, :sex)`;

    const params = [IDNumber, IdCollected, Status, Surname, FirstName, MiddleName, SexCode, BirthDate, DeathDate, NationalityCode, Nationality, Sex];
    try {
        const result = await db.execute(sql, params, { autoCommit: true });
        return result;
    } catch (err) {

    }
    return null;
}

const saveFetchedData = async (db, data) => {
    return Promise.all(data.map(d => {
        if (d && d.IDNumber) {
            updatePersonRecord(db, d);
            savePersonData(db, d);
        }
    }));
}

const findVerifiedPerson = async personID => {
    const db = await getConnection();
    const result = await db.execute('SELECT IDNUMBER FROM HR.NID_TEMP WHERE IDNUMBER = :idnumber', [personID]);
    return result.rows;
}

const fetchPeoplesRecord = async (db, offset) => {
    const result = await db.execute(`SELECT ${fields} FROM HR.PER_ALL_PEOPLE_F OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`, [offset, 200]);
    return result.rows;
}

const fetchUpdatedRecord = async personId => {
    const db = await getConnection();
    const result = await db.execute('SELECT * FROM HR.PER_ALL_PEOPLE_F WHERE NATIONAL_IDENTIFIER = :idnumber', [personID]);
    return result.rows[0];
}

const updatePersonRecord = async (db, person) => {
    const { IDNumber, Surname = '', FirstName = '', MiddleName = '', BirthDate, DeathDate } = person;

    let birthdate = null;
    if (BirthDate) {
        const birth_date = BirthDate.split('/');
        birthdate = `${birth_date[0]}-${MONTH[birth_date[1] - 1]}-${birth_date[2]}`;
    }
    let deathdate = null;
    if (DeathDate) {
        const death_date = DeathDate.split('/');
        deathdate = `${death_date[0]}-${MONTH[death_date[1] - 1]}-${death_date[2]}`;
    }


    const sql = `UPDATE HR.PER_ALL_PEOPLE_F SET
                    LAST_NAME = :lastname,
                    FIRST_NAME = :firstname,
                    MIDDLE_NAMES = :middlename,
                    DATE_OF_BIRTH = TO_DATE(:birthdate, 'DD-MON-YY'),
                    ATTRIBUTE10 = 'verified',
                    DATE_OF_DEATH = TO_DATE(:date_of_death, 'DD-MON-YY')
                WHERE NATIONAL_IDENTIFIER = :nid`;

    const params = [Surname, FirstName, MiddleName, birthdate, deathdate, IDNumber];

    const result = await db.execute(sql, params, { autoCommit: true });
}

const findRecentlyAddedEmployees = async db => {
    const sql = `SELECT ${fields} FROM HR.PER_ALL_PEOPLE_F WHERE TO_CHAR(TO_DATE(sysdate - 1, 'DD-MON-YY')) = TO_CHAR(EFFECTIVE_START_DATE)`;
    // const sql = `SELECT ${fields} FROM HR.PER_ALL_PEOPLE_F FETCH NEXT 3 ROWS ONLY`;
    const result = await db.execute(sql);
    return result.rows;
}

const verifyNewRecords = async () => {
    const db = await getConnection();
    const records = await findRecentlyAddedEmployees(db);
    fetchedData = await Promise.all(records.map(record => fetchPersonData(record.NID)));

    fetchedData.forEach(data => {
        savePersonData(db, data);

        const { Surname, FirstName, MiddleName, BirthDate } = data;
        const { FIRST_NAME, LAST_NAME, MIDDLE_NAMES, DATE_OF_BIRTH } = records.find(record => record.NID == data.IDNumber);
        if (Surname != LAST_NAME || FirstName != FIRST_NAME || MiddleName != MIDDLE_NAMES || BirthDate != DATE_OF_BIRTH) {
            modifyRecord(db, data);
        }
    });
}

const modifyRecord = async (db, newRecord) => {
    const { IDNumber, Surname = '', FirstName = '', MiddleName = '', BirthDate, DeathDate } = newRecord;

    const old_record_criteria = "NATIONAL_IDENTIFIER = :nid AND TO_CHAR(EFFECTIVE_END_DATE) >= TO_CHAR(TO_DATE(sysdate, 'DD-MON-YY'))";
    const sql = `INSERT INTO HR.PER_ALL_PEOPLE_ERROR 
                    SELECT * FROM HR.PER_ALL_PEOPLE_F
                    WHERE ${old_record_criteria}`;

    const result = await db.execute(sql, [IDNumber], { autoCommit: true });

    // obsolete old record
    await db.execute(`UPDATE HR.PER_ALL_PEOPLE_ERROR SET 
                        EFFECTIVE_END_DATE = TO_DATE(sysdate, 'DD-MON-YY')
                    WHERE ${old_record_criteria}`, [IDNumber], { autoCommit: true });

    // update record
    let birthdate = null;
    if (BirthDate) {
        const birth_date = BirthDate.split('/');
        birthdate = `${birth_date[0]}-${MONTH[birth_date[1] - 1]}-${birth_date[2]}`;
    }
    let deathdate = null;
    if (DeathDate) {
        const death_date = DeathDate.split('/');
        deathdate = `${death_date[0]}-${MONTH[death_date[1] - 1]}-${death_date[2]}`;
    }

    const params = [Surname, FirstName, MiddleName, deathdate, birthdate, IDNumber];
    const query = `UPDATE HR.PER_ALL_PEOPLE_F SET
                    LAST_NAME = :lastname,
                    FIRST_NAME = :firstname,
                    MIDDLE_NAMES = :middlename,
                    DATE_OF_DEATH = TO_DATE(:deathdate, 'DD-MON-YY'),
                    DATE_OF_BIRTH = TO_DATE(:birthdate, 'DD-MON-YY'),
                    EFFECTIVE_START_DATE = TO_DATE((sysdate + 1), 'DD-MON-YY'),
                    EFFECTIVE_END_DATE = TO_DATE('31-DEC-12', 'DD-MON-YY'),
                    ATTRIBUTE10 = 'verified' 
                WHERE ${old_record_criteria}`;

    await db.execute(query, params, { autoCommit: true });
}


module.exports = {
    fetchAndUpdatePeopleData,
    fetchAndUpdatePersonData,
    verifyNewRecords
}
