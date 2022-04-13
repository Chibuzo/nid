const routes = require('express').Router();
const personRoutes = require('./person');

routes.get('/', (req, res) => {
    res.status(200).json({ message: 'What are you looking for here??!!' });
});

routes.use('/persons', personRoutes);


module.exports = routes;