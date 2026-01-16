import express from 'express';

const app = express();
const port = process.env.PORT || 3000;
const apiBase = process.env.API_BASE || '';

app.get('/', (req, res) => {
  res.type('html').send(`
    <html>
      <head><title>ClimSystems AI Agent</title></head>
      <body style="font-family: Arial; padding: 24px;">
        <h1>ClimSystems AI Agent (Starter)</h1>
        <p>Web UI placeholder. API base: <code>${apiBase}</code></p>
        <p>Try API health: <a href="${apiBase}/health">${apiBase}/health</a></p>
      </body>
    </html>
  `);
});

app.listen(port, '0.0.0.0', () => {
  console.log(`Web listening on ${port}`);
});
