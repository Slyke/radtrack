const express = require('express');
const multer = require('multer');
const csv = require('csv-parser');
const { Pool } = require('pg');
const fs = require('fs');
const app = express();
const port = 3000;

const pool = new Pool({
    user: 'youruser',
    host: 'localhost',
    database: 'yourdatabase',
    password: 'yourpassword',
    port: 5432,
});

const upload = multer({ dest: 'uploads/' });

app.post('/upload', upload.single('file'), (req, res) => {
    const fileRows = [];

    fs.createReadStream(req.file.path)
        .pipe(csv())
        .on('data', (row) => {
            fileRows.push(row);
        })
        .on('end', () => {
            fs.unlinkSync(req.file.path);
            fileRows.forEach(async (row) => {
                await pool.query(
                    'INSERT INTO data (timestamp, time, latitude, longitude, accuracy, doserate, countrate, comment) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)',
                    [row.Timestamp, row.Time, row.Latitude, row.Longitude, row.Accuracy, row.DoseRate, row.CountRate, row.Comment]
                );
            });
            res.send('File uploaded and data inserted.');
        });
});

app.get('/data', async (req, res) => {
    const { date, minIntensity, maxIntensity } = req.query;
    let query = 'SELECT * FROM data WHERE 1=1';
    const params = [];
    if (date) {
        query += ' AND time::date = $1';
        params.push(date);
    }
    if (minIntensity) {
        query += ' AND doserate >= $2';
        params.push(minIntensity);
    }
    if (maxIntensity) {
        query += ' AND doserate <= $3';
        params.push(maxIntensity);
    }
    const result = await pool.query(query, params);
    res.json(result.rows);
});

app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});
