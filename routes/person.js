const routes = require('express').Router();
const { fetchAndUpdatePersonData } = require('../services/peopleService');

routes.get('/:person_id', async (req, res) => {
    try {
        const person = await fetchAndUpdatePersonData(req.params.person_id);
        res.status(200).json({ status: true, data: { person } });
    } catch (err) {
        console.log(err)
        res.status(err.statusCode || 500).json({ status: false, message: err.message });
    }
});

module.exports = routes;