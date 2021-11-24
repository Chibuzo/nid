const routes = require('express').Router();
const { fetchAndUpdatePersonData, savePersonData } = require('../services/peopleService');

routes.get('/:person_id', async (req, res) => {
    try {
        const person = await fetchAndUpdatePersonData(req.params.person_id);
        res.status(200).json({ status: true, data: { person } });
    } catch (err) {
        console.log(err)
        res.status(err.statusCode || 500).json({ status: false, message: err.message });
    }
});

routes.post('/save', async (req, res) => {
    try {
        const person = await savePersonData({
            "IDNumber": "060277102023",
            "IdCollected": "Y",
            "Status": "Active",
            "Surname": "MOKHATHI",
            "FirstName": "SETLABOCHA",
            "MiddleName": "JONAS",
            "SexCode": "M",
            "BirthDate": "27/07/1976",
            "DeathDate": "27/07/1976",
            "NationalityCode": "LSO",
            "Nationality": "Lesotho",
            "Sex": "Male"
        });
        res.status(200).json({ status: true, data: { person } });
    } catch (err) {
        console.log(err)
        res.status(err.statusCode || 500).json({ status: false, message: err.message });
    }
});

module.exports = routes;