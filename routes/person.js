const routes = require('express').Router();
const { fetchAndUpdatePersonData, fetchAndUpdatePeopleData, queryTest } = require('../services/peopleService');


routes.get('/test', async (req, res) => {
    try {
        const person = await queryTest();
        res.status(200).json({ status: true, data: { person } });
    } catch (err) {
        console.log(err)
        res.status(err.statusCode || 500).json({ status: false, message: err.message });
    }
});

routes.get('/:person_id', async (req, res) => {
    try {
        const person = await fetchAndUpdatePersonData(req.params.person_id);
        res.status(200).json({ status: true, data: { person } });
    } catch (err) {
        console.log(err)
        res.status(err.statusCode || 500).json({ status: false, message: err.message });
    }
});

routes.put('/verify-all', async (req, res) => {
    try {
        await fetchAndUpdatePeopleData();
        res.status(200).json({ status: true, message: 'Operation successful' });
    } catch (err) {
        console.log(err)
        res.status(err.statusCode || 500).json({ status: false, message: err.message });
    }
});


module.exports = routes;