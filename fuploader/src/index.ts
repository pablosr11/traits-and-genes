import express from "express";

const app = express();
app.engine("html", require("ejs").renderFile);
const port = 8080; // default port to listen

import fs from "fs";

// define a route handler for the default home page
app.get("/", (req, res) => {
  res.render("/Users/pablo.sanderson/repos/myheritage/fuploader/index.html");
});

app.get("/result/:jobId", (req, res) => {
  console.log(`Downloading ${req.params["jobId"]}`);
  res.download("/Users/pablo.sanderson/repos/myheritage/fuploader/TODO");
});

let isready = false;
app.get("/status/:jobId", (req, res) => {
  // check work is ready, if not update field
  if (isready) {
    res.json({ link: `http://localhost:8080/result/${req.params["jobId"]}` });
  } else {
    res.json({ msg: "not yet" });
    isready = true;
  }
});

app.post("/upload", (req, res) => {
  let name = req.query["i"];
  let filename = name + ".csv";

  req.on("data", (chunk) => {
    // check is the expected chunk, otherwise corrupted file
    fs.appendFileSync(filename, chunk);
    console.log(`received ${chunk.length}. Storing in ${filename}`);
  });

  // check if final header included, and filesize is equal to it
  if (req.query["q"] && +req.query["q"] === fs.statSync(filename).size) {
    //trigger whatever comes next
    console.log("COMMMMPLETEDDDDD");
    res.json({ link: `http://localhost:8080/status/${name}` });
  }
  res.end();
});

// start the Express server
app.listen(port, () => {
  console.log(`server started at http://localhost:${port}`);
});
