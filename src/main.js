const mqtt = require('mqtt');
const express = require('express');
const mysql = require('mysql2/promise');

require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;
const MQTT_HOST = process.env.MQTT_HOST || '';
const MQTT_USERNAME = process.env.MQTT_USERNAME || '';
const MQTT_PASSWORD = process.env.MQTT_PASSWORD || '';
const MYSQL_HOST = process.env.MYSQL_HOST || '';
const MYSQL_USERNAME = process.env.MYSQL_USERNAME || '';
const MYSQL_PASSWORD = process.env.MYSQL_PASSWORD || '';


app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});

// Configure MQTT connection
const mqttClient = mqtt.connect(MQTT_HOST, {
  username: MQTT_USERNAME,
  password: MQTT_PASSWORD,
});

mqttClient.on('error', (err) => {
  console.error('Connection error:', err);
  mqttClient.end();
});

// Subscribe to MQTT topics
const topics = ['/temperature', '/humidity', '/light'];

mqttClient.on('connect', () => {
  console.log('Connected to MQTT broker');

  topics.forEach((topic) => {
    mqttClient.subscribe(topic, (err) => {
      if (err) {
        console.error('Error subscribing to topic', err);
        return;
      }
      console.log(`Subscribed to MQTT topic: ${topic}`);
    });
  });
});

// Configure MySQL connection
const pool = mysql.createPool({
  host: MYSQL_HOST,
  user: MYSQL_USERNAME,
  password: MYSQL_PASSWORD,
  database: 'iot',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

// Save MQTT data to MySQL
mqttClient.on('message', async (topic, message) => {
  const value = message.toString();
  console.log(`Received message on topic ${topic}: ${value}`);

  try {
    const timestamp = new Date().toISOString().slice(0, 19).replace('T', ' ');
    let query = '';

    if (topic === '/temperature') {
      query = 'INSERT INTO temperature (temperature, timestamp) VALUES (?, ?)';
    } else if (topic === '/humidity') {
      query = 'INSERT INTO humidity (humidity, timestamp) VALUES (?, ?)';
    } else if (topic === '/light') {
      query = 'INSERT INTO light (off, timestamp) VALUES (?, ?)';
    } else {
      console.error('Unknown topic:', topic);
      return;
    }

    const [result] = await pool.execute(query, [parseFloat(value), timestamp]);
    console.log('Inserted data into MySQL:', result.affectedRows > 0);
  } catch (e) {
    console.error('Error saving data to MySQL:', e);
  }
});

// Close MQTT connection on exit
process.on('SIGINT', () => {
  mqttClient.end();
  process.exit();
});
