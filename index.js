require('dotenv').config();
const fs = require('fs');
const { Client } = require('pg');
const axios = require('axios');

// Database configuration
const config = {
    connectionString: `postgres://candidate:${process.env.DB_PASSWORD}@rc1b-r21uoagjy1t7k77h.mdb.yandexcloud.net:6432/db1`,
    ssl: {
        rejectUnauthorized: true,
        ca: fs.readFileSync('/home/stariz/.postgresql/root.crt').toString(),
    },
};

const client = new Client(config);

// Function to fetch all characters from the Rick and Morty API
async function fetchAllCharacters() {
    let characters = [];
    let page = 1;
    let totalPages;

    do {
        const response = await axios.get(`https://rickandmortyapi.com/api/character/?page=${page}`);
        characters = characters.concat(response.data.results);
        totalPages = response.data.info.pages;
        page++;
    } while (page <= totalPages);

    return characters;
}

// Function to insert characters into the database
async function insertCharacters(characters) {
    const tableName = 'rick_and_morty_characters';
    await client.query(`DROP TABLE IF EXISTS ${tableName}`);
    await client.query(`
        CREATE TABLE ${tableName} (
            id SERIAL PRIMARY KEY,
            name TEXT,
            status TEXT,
            species TEXT,
            type TEXT,
            gender TEXT,
            origin TEXT,
            location TEXT,
            image TEXT,
            episode TEXT[],
            url TEXT,
            created TEXT
        )
    `);

    for (const character of characters) {
        const query = `
            INSERT INTO ${tableName} (name, status, species, type, gender, origin, location, image, episode, url, created)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `;
        const values = [
            character.name,
            character.status,
            character.species,
            character.type,
            character.gender,
            character.origin.name,
            character.location.name,
            character.image,
            character.episode,
            character.url,
            character.created,
        ];

        await client.query(query, values);
    }
}

// Main function to fetch and insert characters
async function main() {
    try {
        await client.connect();
        console.log('Connected to PostgreSQL');
        
        const characters = await fetchAllCharacters();
        await insertCharacters(characters);
        console.log('Characters inserted successfully.');
    } catch (error) {
        console.error('Error:', error);
    } finally {
        await client.end();
        console.log('Disconnected from PostgreSQL');
    }
}

main();