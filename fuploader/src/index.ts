import express from "express";
import fs from "fs";

const app = express();
const port = 8080; // default port to listen
app.engine("html", require("ejs").renderFile);

const HOST = `http://localhost:${port}`;

// start the Express server
app.listen(port, () => {
  console.log(`server started at ${HOST}`);
});

app.get("/", (req, res) => {
  res.render("/Users/pablo.sanderson/repos/myheritage/fuploader/index.html");
});

app.get("/result/:jobId", (req, res) => {
  res.download("/Users/pablo.sanderson/repos/myheritage/fuploader/TODO");
});

let isready = false;
app.get("/status/:jobId", (req, res) => {
  // check work is ready, if not update field
  if (isready) {
    res.json({ link: `${HOST}/result/${req.params["jobId"]}` });
  } else {
    res.json({ isready: 0 });
    isready = true;
  }
});

app.post("/upload", (req, res) => {
  let name = req.query["i"];
  let filename = name + ".csv";

  req.on("data", (chunk) => {
    // add ID to identify each chunk - to detect missing packets
    fs.appendFileSync(filename, chunk);
    console.log(`received ${chunk.length}. Storing in ${filename}`);
  });

  // check if final header included, and filesize is equal to it
  if (req.query["q"] && +req.query["q"] === fs.statSync(filename).size) {
    console.log(`Upload completed: ${filename}`);
    res.json({ link: `${HOST}/status/${name}` });
  }
  res.end();
});
