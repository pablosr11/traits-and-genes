import express from "express";

const app = express();
app.engine("html", require("ejs").renderFile);
const port = 8080; // default port to listen

import fs from "fs";

// define a route handler for the default home page
app.get("/", (req, res) => {
  res.render("/Users/pablo.sanderson/repos/myheritage/fuploader/index.html");
});

app.get("/result", (req, res) => {
  res.download("/Users/pablo.sanderson/repos/myheritage/fuploader/TODO");
});

app.post("/upload", (req, res) => {
  let name = req.query["i"];
  let filename = name + ".csv";

  req.on("data", (chunk) => {
    // check is the expected chunk, otherwise corrupted file
    fs.appendFileSync(filename, chunk);
    console.log(`received ${chunk.length}`);
  });

  console.log(`stored in ${name}`);

  // check if final header included, and filesize is equal to it
  if (req.query["q"] && +req.query["q"] === fs.statSync(filename).size) {
    //trigger whatever comes next
    console.log("COMMMMPLETEDDDDD");
    res.json({ link: `http://localhost:8080/result/?i=${name}` });
  }
  res.end();
});

// start the Express server
app.listen(port, () => {
  console.log(`server started at http://localhost:${port}`);
});
