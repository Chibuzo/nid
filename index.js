const express = require('express');
const app = express();
require('dotenv').config();
const cron = require('node-cron');
const apiRoutes = require('./routes');
const { handleError, ErrorHandler } = require('./helpers/errorHandler');
const { verifyNewRecords, fetchPersonData } = require('./services/peopleService');

// initialize DB
require('./config/dbconnection');

app.use(express.json());

app.get('/', (req, res) => {
    res.status(200).json({ message: 'Welcome to NID api!' });
});

//app.use('/api', header_validation, apiRoutes);
app.use('/api', apiRoutes);

// catch 404 routes
app.use((req, res, next) => {
    throw new ErrorHandler(404, "Route not found!");
});


app.use((err, req, res, next) => {
    handleError(err, res);
});

app.set('port', process.env.PORT);

app.listen(app.get('port'), () => {
    console.log('App listening on port ' + process.env.PORT);

    // Cron job scheduled to run at 6am everyday
    cron.schedule('* * * * *', async function () {
        //cron.schedule('59 5 * * *', function () {
        try {
            //verifyNewRecords();
            const data = await fetchPersonData('041164235921');
            console.log(data);
            console.log('Records verified');
        } catch (err) {
            console.log(err)
        }
    });
});