import express from "express";
import fs from "fs";
import cors from "cors";

const app = express();
const port = 8080; // default port to listen
const HOST = `http://localhost:${port}`;

// specify what origins can send request
// null works for local html file, change when proxy server etc are setup
const corsOptions = {
  origin: `null`,
};

app.use(cors(corsOptions));

// start the Express server
app.listen(port, () => {
  console.log(`server started at ${HOST}`);
});

app.get("/", (req, res) => {
  //load html file directly instead
  res.json({ info: "Wrong site" });
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
  res.send("OK");
});
